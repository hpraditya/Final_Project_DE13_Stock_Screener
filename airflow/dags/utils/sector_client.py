"""
Airflow shim untuk ekstraksi data dari Sectors API.

Fitur watchdog:
  Setiap domain extraction menjalankan background thread 30-detik timer.
  Data dari ticker pertama ditulis ke S3 segera setelah tersedia (incremental write).
  Jika 30 detik berlalu tanpa satu baris pun masuk ke data lake → DAG dihentikan,
  log ditulis, dan notifikasi Slack dikirim.

Efisiensi request:
  extract_fundamentals() memanggil _fetch_ticker_list() SEKALI untuk 3 domain,
  lalu menjalankan setiap domain secara sequential. Ini menekan peak request rate
  dari ~480 req/menit (3 task paralel) menjadi ~120 req/menit (1 task sequential).
"""
import os
import sys
import logging
import threading
import time

import pandas as pd

sys.path.insert(0, "/opt/airflow")

from extraction.client import SectorClient, APITimeoutError
from extraction.endpoints import DOMAIN_TO_SECTION
from utils.notify import send_slack_alert
from utils.storage import write_parquet_to_bronze

logger = logging.getLogger(__name__)

WATCHDOG_SECS = 30

_FUNDAMENTAL_DOMAINS = [
    "company_profile",
    "financial_statements",
    "valuation_ratios",
]

_client: SectorClient | None = None


def _get_client() -> SectorClient:
    global _client
    if _client is None:
        _client = SectorClient(api_key=os.environ["SECTOR_API_KEY"])
    return _client


def _get_fail_exc():
    """Return AirflowFailException in Airflow runtime, RuntimeError locally."""
    try:
        from airflow.exceptions import AirflowFailException  # type: ignore[import]
        return AirflowFailException
    except ImportError:
        return RuntimeError


class _Watchdog:
    """
    Background timer. Fires if .clear() is not called within timeout_secs.
    Check .fired() in the main thread between tickers to detect timeout.
    """

    def __init__(self, timeout_secs: int = WATCHDOG_SECS):
        self._cleared = threading.Event()
        self._fired = threading.Event()
        self._timeout = timeout_secs
        self._start = time.monotonic()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _run(self) -> None:
        if not self._cleared.wait(timeout=self._timeout):
            self._fired.set()

    def clear(self) -> None:
        """Signal that data has been written — watchdog will not fire."""
        self._cleared.set()

    def fired(self) -> bool:
        return self._fired.is_set()

    def elapsed(self) -> float:
        return time.monotonic() - self._start

    def stop(self) -> None:
        self._cleared.set()          # unblock background thread so it exits
        self._thread.join(timeout=2)


def _abort_on_watchdog(watchdog: _Watchdog, domain: str, run_date: str) -> None:
    """Raise and notify if the watchdog has fired."""
    if not watchdog.fired():
        return
    FailExc = _get_fail_exc()
    elapsed = watchdog.elapsed()
    msg = (
        f"Watchdog: tidak ada data masuk ke data lake dalam {elapsed:.0f}s "
        f"[domain={domain}, run_date={run_date}]. DAG dihentikan."
    )
    logger.error(msg)
    send_slack_alert(
        title="🚨 Ingestion Watchdog Timeout — DAG Dihentikan",
        details={
            "DAG":      "warren_buffett_screener",
            "Task":     "extract_fundamentals",
            "Run date": run_date,
            "Domain":   domain,
            "Elapsed":  f"{elapsed:.0f}s",
            "Error":    "Tidak ada data yang masuk ke data lake dalam 30 detik.",
        },
    )
    raise FailExc(msg)


def _extract_one_domain(
    domain: str,
    tickers: list[str],
    run_date: str,
    client: SectorClient,
) -> None:
    """
    Jalankan ingestion untuk satu domain dengan tickers yang sudah di-fetch.

    Watchdog (30 detik):
      - Timer dimulai saat domain ini mulai diproses.
      - Data ticker pertama langsung ditulis ke S3 → watchdog cleared.
      - Jika 30 detik berlalu tanpa write → raise AirflowFailException + Slack alert.
      - Setelah semua ticker selesai → tulis ulang data lengkap (overwrite partial write).
    """
    FailExc = _get_fail_exc()

    section = DOMAIN_TO_SECTION.get(domain)
    if section is None:
        raise ValueError(f"Unknown domain '{domain}'. Valid: {list(DOMAIN_TO_SECTION)}")

    watchdog = _Watchdog(timeout_secs=WATCHDOG_SECS)
    watchdog.start()
    logger.info(
        "Domain [%s] started — watchdog=%ds tickers=%d",
        domain, WATCHDOG_SECS, len(tickers),
    )

    try:
        cutoff = client._cutoff_year(run_date)
        rows: list[dict] = []
        first_write_done = False

        for ticker in tickers:
            _abort_on_watchdog(watchdog, domain, run_date)

            try:
                raw = client.fetch_company_report(ticker, section)
                new_rows = client._flatten_response(ticker, section, raw, cutoff)
                rows.extend(new_rows)
            except APITimeoutError:
                raise
            except Exception as exc:
                logger.warning("Skipping %s/%s: %s", domain, ticker, exc)
                continue

            # Tulis segera setelah ticker pertama berhasil → watchdog cleared
            if rows and not first_write_done:
                write_parquet_to_bronze(pd.DataFrame(rows), domain=domain, run_date=run_date)
                first_write_done = True
                watchdog.clear()
                logger.info(
                    "Watchdog cleared [%s]: first write at t=%.1fs rows=%d",
                    domain, watchdog.elapsed(), len(rows),
                )

        # Tulis ulang data lengkap (overwrite partial write dari ticker pertama)
        if rows:
            write_parquet_to_bronze(pd.DataFrame(rows), domain=domain, run_date=run_date)
            logger.info(
                "Domain [%s] selesai: run_date=%s total_rows=%d",
                domain, run_date, len(rows),
            )
        else:
            logger.warning("Tidak ada data: domain=%s run_date=%s", domain, run_date)

    except APITimeoutError as exc:
        msg = (
            f"API timeout [domain={domain}, run_date={run_date}]: {exc}"
        )
        logger.error(msg)
        send_slack_alert(
            title="🚨 Ingestion API Timeout — DAG Dihentikan",
            details={
                "DAG":      "warren_buffett_screener",
                "Task":     "extract_fundamentals",
                "Run date": run_date,
                "Domain":   domain,
                "Error":    str(exc),
            },
        )
        raise FailExc(msg) from exc

    finally:
        watchdog.stop()


def extract_fundamentals(**context) -> None:
    """
    Ambil semua fundamental data (3 domain) secara sequential dengan 1 ticker list call.

    Urutan: company_profile → financial_statements → valuation_ratios

    Efisiensi dibanding 3 task paralel:
      - Ticker list call: 1x (bukan 3x)
      - Peak request rate: ~120 req/menit (bukan ~480 req/menit)
      - Bebas dari 429 burst akibat parallel ticker list requests

    Watchdog aktif per-domain: jika 30 detik berlalu tanpa data masuk ke data lake
    untuk domain yang sedang berjalan → DAG dihentikan + Slack alert.
    """
    FailExc = _get_fail_exc()
    run_date: str = context["ds"]

    client = _get_client()

    # Fetch ticker list SEKALI untuk semua domain
    logger.info("Fetching ticker list (shared across all fundamental domains)...")
    try:
        tickers = client._fetch_ticker_list()
    except APITimeoutError as exc:
        msg = f"API timeout saat fetch ticker list [run_date={run_date}]: {exc}"
        logger.error(msg)
        send_slack_alert(
            title="🚨 Ticker List Timeout — DAG Dihentikan",
            details={
                "DAG":      "warren_buffett_screener",
                "Task":     "extract_fundamentals",
                "Run date": run_date,
                "Error":    str(exc),
            },
        )
        raise FailExc(msg) from exc

    if not tickers:
        raise FailExc(
            "Ticker list kosong — periksa SECTOR_API_KEY dan endpoint /v2/companies/"
        )

    logger.info(
        "Ticker list ready: %d tickers — menjalankan %d domain secara sequential",
        len(tickers), len(_FUNDAMENTAL_DOMAINS),
    )

    # Jalankan setiap domain secara sequential dengan tickers yang sama
    for domain in _FUNDAMENTAL_DOMAINS:
        logger.info("--- Mulai domain: %s ---", domain)
        _extract_one_domain(domain, tickers, run_date, client)
        logger.info("--- Selesai domain: %s ---", domain)

    logger.info(
        "extract_fundamentals complete: %d domains, %d tickers, run_date=%s",
        len(_FUNDAMENTAL_DOMAINS), len(tickers), run_date,
    )
