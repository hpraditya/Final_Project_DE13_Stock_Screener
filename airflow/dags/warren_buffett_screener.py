from __future__ import annotations

import json
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.sector_client import extract_fundamentals

logger = logging.getLogger(__name__)

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
# Extract: Daily Prices (smart backfill)
# ---------------------------------------------------------------------------
def run_extract_prices(**context) -> None:
    """
    Idempotent monthly closing price ingestion (last Friday of each month only).

    Schedule: runs every Friday (`0 10 * * 5`) but only processes data when
    run_date is the last Friday of its calendar month. Other Fridays are skipped.

    First run (no data under bronze/daily_prices/ in the lake):
        Backfill last-Friday-of-month closes for current year + 1 year back
        (~24 partitions). One Parquet partition per date.

    Subsequent runs (history already exists):
        Fetch prices for run_date only if it is the last Friday of the month;
        otherwise log and return early (no API calls, no write).

    If the Sectors API does not respond within 30 seconds, ingestion is stopped
    immediately (AirflowFailException — no retries), a log entry is written, and
    a Slack alert is sent via SLACK_WEBHOOK_URL.
    """
    try:
        from airflow.exceptions import AirflowFailException  # type: ignore[import]
    except ImportError:
        AirflowFailException = RuntimeError  # type: ignore[assignment,misc]
    from extraction.client import SectorClient, APITimeoutError
    from utils.storage import has_bronze_data, write_parquet_to_bronze
    from utils.notify import send_slack_alert

    run_date: str = context["ds"]

    try:
        client = SectorClient(api_key=os.environ["SECTOR_API_KEY"])

        if not has_bronze_data("daily_prices"):
            # First run — backfill last Friday of each month for current year + 1 year back
            logger.info(
                "No price history in data lake — running monthly last-Friday backfill [run_date=%s]",
                run_date,
            )
            tickers = client._fetch_ticker_list()
            if not tickers:
                raise RuntimeError("Ticker list empty — check SECTOR_API_KEY and /v2/companies/")
            friday_data = client._fetch_friday_prices_history(tickers, run_date, years_back=1)
            if not friday_data:
                logger.warning("Backfill returned no data; skipping write")
                return
            for friday_date, df in sorted(friday_data.items()):
                write_parquet_to_bronze(df, "daily_prices", friday_date)
                logger.info("Backfill: %s — %d rows written", friday_date, len(df))
            logger.info("Price backfill complete: %d monthly partitions", len(friday_data))

        else:
            # Incremental — only process on last Friday of the month
            if not client._is_last_friday_of_month(run_date):
                logger.info(
                    "Skipping extract_prices — %s is not the last Friday of its month", run_date
                )
                return

            logger.info(
                "Last Friday of month detected — incremental fetch [run_date=%s]", run_date
            )
            tickers = client._fetch_ticker_list()
            if not tickers:
                raise RuntimeError("Ticker list empty — check SECTOR_API_KEY and /v2/companies/")
            df = client._fetch_daily_prices_all(tickers, run_date)
            if df.empty:
                logger.warning(
                    "No price data returned for run_date=%s; skipping write", run_date
                )
                return
            write_parquet_to_bronze(df, "daily_prices", run_date)
            logger.info("Incremental prices: %d rows written for %s", len(df), run_date)

    except APITimeoutError as exc:
        msg = (
            f"Ingestion dihentikan — API timeout setelah 30 detik "
            f"[task=extract_prices, run_date={run_date}]: {exc}"
        )
        logger.error(msg)
        send_slack_alert(
            title="🚨 Price Ingestion Timeout — Pipeline Dihentikan",
            details={
                "DAG": "warren_buffett_screener",
                "Task": "extract_prices",
                "Run date": run_date,
                "Error": str(exc),
                "Action": "Ingestion dihentikan. Periksa status API Sectors.",
            },
        )
        raise AirflowFailException(msg) from exc


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
    schedule="0 10 * * 5",   # Every Friday 17:00 WIB = 10:00 UTC; price task self-skips on non-last-Fridays
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["buffett", "idx", "screening"],
) as dag:

    # Single task: fetches ticker list once, runs 3 domains sequentially.
    # Replaces 3 parallel tasks — cuts ticker-list calls from 3→1 and peak
    # request rate from ~480 req/min to ~120 req/min (avoids 429 bursts).
    extract_fundamentals_task = PythonOperator(
        task_id="extract_fundamentals",
        python_callable=extract_fundamentals,
    )

    # Price ingestion — independent of the fundamental screening pipeline.
    # On first run: 2-year Friday backfill. On subsequent runs: incremental.
    extract_prices = PythonOperator(
        task_id="extract_prices",
        python_callable=run_extract_prices,
    )

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

    # Screening pipeline: fundamentals → validate → transform → score → verify → load
    _ = extract_fundamentals_task >> gate_bronze >> transform_silver >> transform_gold >> gate_gold >> load_warehouse

    # extract_prices runs independently — auto-registered via DAG context manager.
