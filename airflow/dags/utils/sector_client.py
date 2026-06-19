import os
import sys
import logging

sys.path.insert(0, "/opt/airflow")

from extraction.client import SectorClient

logger = logging.getLogger(__name__)
_client = None


def _get_client() -> SectorClient:
    global _client
    if _client is None:
        api_key = os.environ["SECTOR_API_KEY"]
        _client = SectorClient(api_key=api_key)
    return _client


def extract_domain(domain: str, **context) -> None:
    """Airflow shim: fetch one domain for oil/gas/coal companies and write to bronze."""
    run_date: str = context["ds"]  # YYYY-MM-DD provided by Airflow
    logger.info("Starting extraction: domain=%s  run_date=%s", domain, run_date)

    client = _get_client()
    data = client.fetch_domain(domain, run_date=run_date)

    if data.empty:
        logger.warning(
            "fetch_domain returned 0 rows for domain=%s run_date=%s. "
            "Check SECTOR_API_KEY and API rate limits.",
            domain, run_date,
        )
    else:
        logger.info("Fetched %d rows for domain=%s", len(data), domain)

    from utils.storage import write_parquet_to_bronze
    write_parquet_to_bronze(data, domain=domain, run_date=run_date)
    logger.info("Wrote domain=%s to bronze layer (run_date=%s, rows=%d)", domain, run_date, len(data))
