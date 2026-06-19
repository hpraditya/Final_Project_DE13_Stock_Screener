"""
Tarik historical_financials 2 tahun ke belakang untuk emiten oil/gas/coal di IDX.
Idempoten: data lama di bronze akan di-overwrite dengan run untuk run_date yang sama.

Usage (dari project root):
    python scripts/backfill_fundamentals.py
"""
import os
import sys
from datetime import date
from dotenv import load_dotenv

load_dotenv()

# Add project root and airflow/dags to sys.path.
# NOTE: Do NOT add just the project root and import "airflow.dags.utils.storage"
# because "airflow" is also the name of the installed Apache Airflow package —
# that would import the Airflow package instead of our local airflow/dags/utils/storage.py.
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "airflow", "dags"))

from extraction.client import SectorClient
from utils.storage import write_parquet_to_bronze


def get_run_dates(years: int = 2):
    """Return one run_date per year going back *years* from today."""
    today = date.today()
    return [
        today.replace(year=today.year - y).strftime("%Y-%m-%d")
        for y in range(years)
    ]


def main():
    client = SectorClient(api_key=os.environ["SECTOR_API_KEY"])

    for domain in ("company_profile", "financial_statements", "valuation_ratios"):
        for run_date in get_run_dates(years=2):
            print(f"Backfilling domain={domain} run_date={run_date} ...")
            try:
                df = client.fetch_domain(domain, run_date=run_date)
                write_parquet_to_bronze(df, domain=domain, run_date=run_date)
                print(f"  OK — {len(df)} rows written to bronze.")
            except Exception as exc:
                print(f"  ERROR: {exc}")


if __name__ == "__main__":
    main()
