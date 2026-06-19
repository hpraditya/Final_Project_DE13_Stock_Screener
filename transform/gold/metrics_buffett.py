"""
Compute Buffett metrics from silver layer and load results into PostgreSQL.

Flow:
  1. Read silver Parquet files from SeaweedFS via boto3.
  2. Register DataFrames as DuckDB in-memory views.
  3. Run metric SQL queries to produce metrics_roe / metrics_der / etc. tables.
  4. Join all metrics into a single screening DataFrame.
  5. Apply scoring thresholds (scoring.py).
  6. Initialize PostgreSQL gold schema (DDL), upsert dim_company, insert fct_buffett_screening.
"""

import io
import os
import logging
from pathlib import Path

import boto3
import duckdb
import pandas as pd
import psycopg2
import psycopg2.extras

from transform.gold.scoring import score

logger = logging.getLogger(__name__)

QUERIES_DIR = Path(__file__).parent / "queries"
DDL_DIR   = Path(__file__).parent.parent.parent / "warehouse" / "ddl"
VIEWS_DIR = Path(__file__).parent.parent.parent / "warehouse" / "views"


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _s3_client(s3_endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=os.environ["SEAWEEDFS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["SEAWEEDFS_SECRET_KEY"],
        region_name="us-east-1",
    )


def _read_silver(s3_endpoint: str, domain: str, run_date: str) -> pd.DataFrame:
    bucket = os.environ["SEAWEEDFS_BUCKET"]
    key = f"silver/{domain}/run_date={run_date}/data.parquet"
    logger.info("Reading silver: %s", key)
    obj = _s3_client(s3_endpoint).get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def _ensure_schema(conn) -> None:
    """Run all DDL files then view definitions to initialize the gold schema."""
    with conn.cursor() as cur:
        if DDL_DIR.exists():
            for ddl_file in sorted(DDL_DIR.glob("*.sql")):
                logger.info("Applying DDL: %s", ddl_file.name)
                cur.execute(ddl_file.read_text())
        if VIEWS_DIR.exists():
            for view_file in sorted(VIEWS_DIR.glob("*.sql")):
                logger.info("Applying view: %s", view_file.name)
                cur.execute(view_file.read_text())
    conn.commit()


def _upsert_dim_company(conn, profile_df: pd.DataFrame) -> None:
    """Insert or update dim_company from silver profile data."""
    sql = """
        INSERT INTO gold.dim_company
            (ticker, company_name, sector, sub_sector, market_cap, listing_date)
        VALUES %s
        ON CONFLICT (ticker) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            sector       = EXCLUDED.sector,
            sub_sector   = EXCLUDED.sub_sector,
            market_cap   = EXCLUDED.market_cap,
            listing_date = EXCLUDED.listing_date,
            updated_at   = CURRENT_TIMESTAMP
    """
    rows = [
        (
            row["ticker"],
            row.get("company_name"),
            row.get("sector"),
            row.get("sub_sector"),
            row.get("market_cap"),
            row.get("listing_date"),
        )
        for _, row in profile_df.iterrows()
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows)
    conn.commit()
    logger.info("Upserted %d rows into gold.dim_company", len(rows))


def _insert_screening(conn, df: pd.DataFrame, run_date: str) -> None:
    """Delete existing run_date rows then bulk-insert screening results."""
    delete_sql = "DELETE FROM gold.fct_buffett_screening WHERE run_date = %s"
    insert_sql = """
        INSERT INTO gold.fct_buffett_screening
            (ticker, run_date, period, company_name, sector,
             roe, der, fcf, fcf_margin,
             eps_growth_yoy, eps_cagr_5y,
             per, pbv, graham_combined,
             criteria_passed, status)
        VALUES %s
        ON CONFLICT (ticker, run_date) DO UPDATE SET
            period          = EXCLUDED.period,
            company_name    = EXCLUDED.company_name,
            sector          = EXCLUDED.sector,
            roe             = EXCLUDED.roe,
            der             = EXCLUDED.der,
            fcf             = EXCLUDED.fcf,
            fcf_margin      = EXCLUDED.fcf_margin,
            eps_growth_yoy  = EXCLUDED.eps_growth_yoy,
            eps_cagr_5y     = EXCLUDED.eps_cagr_5y,
            per             = EXCLUDED.per,
            pbv             = EXCLUDED.pbv,
            graham_combined = EXCLUDED.graham_combined,
            criteria_passed = EXCLUDED.criteria_passed,
            status          = EXCLUDED.status,
            loaded_at       = CURRENT_TIMESTAMP
    """

    def _val(row, col):
        v = row.get(col)
        if pd.isna(v) if isinstance(v, float) else v is None:
            return None
        return v

    rows = [
        (
            row["ticker"], run_date, _val(row, "period"),
            _val(row, "company_name"), _val(row, "sector"),
            _val(row, "roe"), _val(row, "der"), _val(row, "fcf"), _val(row, "fcf_margin"),
            _val(row, "eps_growth_yoy"), _val(row, "eps_cagr_5y"),
            _val(row, "per"), _val(row, "pbv"), _val(row, "graham_combined"),
            int(row.get("criteria_passed", 0)), row.get("status", "TIDAK"),
        )
        for _, row in df.iterrows()
    ]
    with conn.cursor() as cur:
        cur.execute(delete_sql, (run_date,))
        psycopg2.extras.execute_values(cur, insert_sql, rows)
    conn.commit()
    logger.info("Inserted %d rows into gold.fct_buffett_screening (run_date=%s)", len(rows), run_date)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(run_date: str, pg_conn_str: str, s3_endpoint: str) -> None:
    """Compute all Buffett metrics and load into PostgreSQL gold schema."""

    # 1. Read silver data
    fin_df     = _read_silver(s3_endpoint, "financial_statements", run_date)
    profile_df = _read_silver(s3_endpoint, "company_profile",      run_date)
    val_df     = _read_silver(s3_endpoint, "valuation_ratios",     run_date)

    # Ensure run_date column is present (silver writes already add it)
    for df in (fin_df, profile_df, val_df):
        if "run_date" not in df.columns:
            df["run_date"] = run_date

    # 2. Register silver DataFrames as DuckDB in-memory views
    con = duckdb.connect()
    con.register("silver_financials", fin_df)
    con.register("silver_profile",    profile_df)
    con.register("silver_valuation",  val_df)

    # 3. Run metric SQL queries and store each result as a DuckDB table
    metric_names = ["roe", "der", "fcf", "growth", "valuation"]
    for name in metric_names:
        sql = (QUERIES_DIR / f"{name}.sql").read_text()
        # Replace named parameter with quoted literal
        sql = sql.replace(":run_date", f"'{run_date}'")
        # Strip trailing semicolon so it can be embedded in CREATE TABLE AS
        sql = sql.rstrip().rstrip(";")
        con.execute(f"CREATE OR REPLACE TABLE metrics_{name} AS {sql}")
        row_count = con.execute(f"SELECT COUNT(*) FROM metrics_{name}").fetchone()[0]
        logger.info("metrics_%s: %d rows", name, row_count)

    # 4. Join all metrics — use latest available period per ticker
    screening_df = con.execute(f"""
        WITH latest_period AS (
            SELECT ticker, MAX(period) AS period
            FROM silver_financials
            GROUP BY ticker
        )
        SELECT
            p.ticker,
            p.company_name,
            p.sector,
            lp.period,
            r.roe,
            d.der,
            fc.fcf,
            fc.fcf_margin,
            g.eps_growth_yoy,
            g.eps_cagr_5y,
            v.per,
            v.pbv,
            v.graham_combined
        FROM silver_profile p
        JOIN latest_period lp ON p.ticker = lp.ticker
        LEFT JOIN metrics_roe       r  ON p.ticker = r.ticker  AND lp.period = r.period
        LEFT JOIN metrics_der       d  ON p.ticker = d.ticker  AND lp.period = d.period
        LEFT JOIN metrics_fcf       fc ON p.ticker = fc.ticker AND lp.period = fc.period
        LEFT JOIN metrics_growth    g  ON p.ticker = g.ticker  AND lp.period = g.period
        LEFT JOIN metrics_valuation v  ON p.ticker = v.ticker  AND lp.period = v.period
    """).df()
    con.close()

    logger.info("Screening candidates before scoring: %d", len(screening_df))

    # 5. Apply Buffett scoring
    screening_df = score(screening_df)

    lolos = (screening_df["status"] == "LOLOS").sum()
    logger.info("Scoring done — LOLOS: %d / %d", lolos, len(screening_df))

    # 6. Write to PostgreSQL
    conn = psycopg2.connect(pg_conn_str)
    try:
        _ensure_schema(conn)
        _upsert_dim_company(conn, profile_df)
        _insert_screening(conn, screening_df, run_date)
    finally:
        conn.close()
