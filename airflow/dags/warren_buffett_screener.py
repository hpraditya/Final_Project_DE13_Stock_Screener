from __future__ import annotations

import json
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.sector_client import extract_domain

logger = logging.getLogger(__name__)

SECTOR_DOMAINS = [
    "company_profile",
    "financial_statements",
    "valuation_ratios",
    "daily_prices",
]

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

GE_SUITE_DIR = Path("/opt/airflow/quality/expectations")


def _s3_endpoint() -> str:
    return os.environ["SEAWEEDFS_ENDPOINT"]


def _pg_conn_str() -> str:
    return (
        f"postgresql://{os.environ['POSTGRES_USER']}"
        f":{os.environ['POSTGRES_PASSWORD']}"
        f"@{os.environ['POSTGRES_HOST']}"
        f"/{os.environ['POSTGRES_DB']}"
    )


# ---------------------------------------------------------------------------
# Data-quality helper — pattern from github.com/ismaildawoodjee/GreatEx
# Validates a DataFrame against a GE expectation suite JSON without needing
# datasource or checkpoint configuration (uses ge.from_pandas() in-memory).
# ---------------------------------------------------------------------------
def _validate_with_ge(df: pd.DataFrame, suite_name: str, label: str) -> None:
    """Validate *df* against the named expectation suite.

    Reads the suite from GE_SUITE_DIR/<suite_name>.json and calls each
    expectation method directly on a ge.from_pandas() PandasDataset.
    Raises ValueError listing every failed expectation if any fail.
    """
    import great_expectations as ge

    suite_path = GE_SUITE_DIR / f"{suite_name}.json"
    suite_dict = json.loads(suite_path.read_text())
    ge_df = ge.from_pandas(df)

    failures: list[str] = []
    for exp in suite_dict.get("expectations", []):
        method_name = exp["expectation_type"]
        kwargs = exp.get("kwargs", {})
        try:
            result = getattr(ge_df, method_name)(**kwargs)
            if not result.success:
                failures.append(
                    f"  ✗ {method_name}  kwargs={kwargs}  result={result.result}"
                )
        except AttributeError:
            failures.append(f"  ✗ {method_name} not found in GE version installed")
        except Exception as exc:
            failures.append(f"  ✗ {method_name} errored: {exc}")

    total = len(suite_dict.get("expectations", []))
    passed = total - len(failures)
    logger.info("[%s] %d/%d expectations passed.", label, passed, total)

    if failures:
        raise ValueError(
            f"[{label}] Data quality FAILED — {len(failures)} expectation(s):\n"
            + "\n".join(failures)
        )


# ---------------------------------------------------------------------------
# Gate: Bronze
# ---------------------------------------------------------------------------
def run_gate_bronze(**context) -> None:
    """Validate bronze financial_statements Parquet from SeaweedFS."""
    from utils.storage import read_parquet_from_bronze

    run_date: str = context["ds"]
    df = read_parquet_from_bronze("financial_statements", run_date)
    logger.info(
        "Bronze gate — financial_statements: %d rows, %d columns (run_date=%s)",
        len(df), len(df.columns), run_date,
    )
    _validate_with_ge(df, "bronze_raw_suite", f"bronze/financial_statements/{run_date}")


# ---------------------------------------------------------------------------
# Transform: Silver
# ---------------------------------------------------------------------------
def run_transform_silver(**context) -> None:
    """Run all silver-layer cleaning transforms for this run_date.

    Each transform reads from bronze S3, cleans the data, and writes the
    result back to the silver S3 path internally — return values are ignored.
    """
    run_date: str = context["ds"]
    endpoint = _s3_endpoint()

    from transform.silver.clean_financials import clean_financials
    from transform.silver.clean_profile import clean_profile
    from transform.silver.clean_valuation import clean_valuation

    logger.info("Silver transform: financial_statements [run_date=%s]", run_date)
    clean_financials(run_date, endpoint)

    logger.info("Silver transform: company_profile [run_date=%s]", run_date)
    clean_profile(run_date, endpoint)

    logger.info("Silver transform: valuation_ratios [run_date=%s]", run_date)
    clean_valuation(run_date, endpoint)

    logger.info("Silver transforms complete.")


# ---------------------------------------------------------------------------
# Transform: Gold
# ---------------------------------------------------------------------------
def run_transform_gold(**context) -> None:
    """Compute all Buffett metrics and write to PostgreSQL gold schema."""
    run_date: str = context["ds"]

    from transform.gold.metrics_buffett import run as run_metrics

    logger.info("Gold transform: computing Buffett metrics [run_date=%s]", run_date)
    run_metrics(run_date, _pg_conn_str(), _s3_endpoint())
    logger.info("Gold transforms complete.")


# ---------------------------------------------------------------------------
# Gate: Gold
# ---------------------------------------------------------------------------
def run_gate_gold(**context) -> None:
    """Validate gold fct_buffett_screening from PostgreSQL."""
    import psycopg2

    run_date: str = context["ds"]
    pg_host = os.environ["POSTGRES_HOST"]
    pg_db = os.environ["POSTGRES_DB"]
    pg_user = os.environ["POSTGRES_USER"]
    pg_password = os.environ["POSTGRES_PASSWORD"]

    conn = psycopg2.connect(
        host=pg_host, dbname=pg_db, user=pg_user, password=pg_password
    )
    try:
        df = pd.read_sql(
            "SELECT * FROM gold.fct_buffett_screening WHERE run_date = %s",
            conn,
            params=(run_date,),
        )
    finally:
        conn.close()

    lolos = (df["status"] == "LOLOS").sum() if len(df) > 0 else 0
    logger.info(
        "Gold gate — run_date=%s: total=%d LOLOS=%d TIDAK=%d",
        run_date, len(df), lolos, len(df) - lolos,
    )
    _validate_with_ge(df, "gold_metrics_suite", f"gold/fct_buffett_screening/{run_date}")


# ---------------------------------------------------------------------------
# Load: Warehouse
# ---------------------------------------------------------------------------
def run_load_warehouse(**context) -> None:
    """Verify gold load and log summary statistics."""
    import psycopg2

    run_date: str = context["ds"]
    pg_host = os.environ["POSTGRES_HOST"]
    pg_db = os.environ["POSTGRES_DB"]
    pg_user = os.environ["POSTGRES_USER"]
    pg_password = os.environ["POSTGRES_PASSWORD"]

    conn = psycopg2.connect(
        host=pg_host, dbname=pg_db, user=pg_user, password=pg_password
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), SUM(CASE WHEN status = 'LOLOS' THEN 1 ELSE 0 END) "
                "FROM gold.fct_buffett_screening WHERE run_date = %s",
                (run_date,),
            )
            row = cur.fetchone() or (0, 0)
            total, lolos = row[0] or 0, row[1] or 0
            logger.info(
                "Warehouse verified — run_date=%s: total=%d LOLOS=%d TIDAK=%d",
                run_date, total, lolos, total - lolos,
            )
    finally:
        conn.close()
    logger.info("Warehouse load complete.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="warren_buffett_screener",
    default_args=default_args,
    schedule="0 10 * * 5",   # Every Friday 17:00 WIB = 10:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["buffett", "idx", "screening"],
) as dag:

    extract_tasks = [
        PythonOperator(
            task_id=f"extract_{domain}",
            python_callable=extract_domain,
            op_kwargs={"domain": domain},
        )
        for domain in SECTOR_DOMAINS
    ]

    gate_bronze = PythonOperator(
        task_id="gate_bronze",
        python_callable=run_gate_bronze,
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_transform_silver,
    )

    transform_gold = PythonOperator(
        task_id="transform_gold",
        python_callable=run_transform_gold,
    )

    gate_gold = PythonOperator(
        task_id="gate_gold",
        python_callable=run_gate_gold,
    )

    load_warehouse = PythonOperator(
        task_id="load_warehouse",
        python_callable=run_load_warehouse,
    )

    extract_tasks >> gate_bronze >> transform_silver >> transform_gold >> gate_gold >> load_warehouse
