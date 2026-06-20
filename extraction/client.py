import time
import logging
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from extraction.endpoints import BASE_URL, SECTION_ENDPOINTS, DOMAIN_TO_SECTION

logger = logging.getLogger(__name__)

# Sectors API where-clause to select the Oil, Gas & Coal sub-sector on IDX.
# Uses double-quoted string literal as required by the /v2/companies/?where= syntax.
# NL query (q=) was replaced because the LLM translation generates invalid WHERE clauses.
_SUB_SECTOR_WHERE = 'sub_sector = "Oil, Gas & Coal"'

# Seconds to wait between individual API calls (reduces 429 rate-limit hits).
# At 0.5s/request the sequential fundamental loop runs at ~120 req/min —
# well within Sectors API rate limits for a 358-request weekly pipeline run.
_REQUEST_SLEEP = 0.5

# Default seconds to back off when a 429 is received without a Retry-After header
_RATE_LIMIT_BACKOFF = 10


class APITimeoutError(RuntimeError):
    """Raised when the Sectors API does not respond within the configured timeout."""


class SectorClient:
    def __init__(self, api_key: str, timeout: int = 30, max_retries: int = 5):
        self.api_key = api_key
        self.timeout = timeout
        self.session = self._build_session(max_retries)

    def _build_session(self, max_retries: int) -> requests.Session:
        session = requests.Session()
        # v2 uses the raw API key directly — no "Bearer" prefix
        session.headers.update({"Authorization": self.api_key})
        # Retry on server errors only — 429 is handled manually in _safe_get()
        # so that we can respect the Retry-After header before re-attempting.
        retry = Retry(
            total=max_retries,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        return session

    def _safe_get(self, url: str, params: dict | None = None) -> requests.Response:
        """
        Wrapper around session.get() that:
          - Raises APITimeoutError on Timeout
          - Handles 429 by reading Retry-After header and sleeping before retry
            (retries up to 3 times, then raises the 429 response error)
        """
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.exceptions.Timeout:
                raise APITimeoutError(
                    f"Sectors API tidak merespons dalam {self.timeout}s [url={url}]"
                )
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", _RATE_LIMIT_BACKOFF))
                logger.warning(
                    "Rate limited (429) on %s — sleeping %ds before retry %d/3",
                    url, retry_after, attempt + 1,
                )
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            return resp
        # Third attempt was also 429 — raise so the caller can handle
        resp.raise_for_status()  # will raise HTTPError with 429
        return resp  # unreachable but satisfies type checker

    def fetch_company_report(self, ticker: str, section: str) -> dict[str, Any]:
        if section not in SECTION_ENDPOINTS:
            raise KeyError(f"Unknown section '{section}'. Valid: {list(SECTION_ENDPOINTS)}")
        clean_ticker = ticker.upper().replace(".JK", "")
        url = f"{BASE_URL}/company/report/{clean_ticker}/"
        response = self._safe_get(url, params={"sections": section})
        time.sleep(_REQUEST_SLEEP)
        return response.json()

    def _fetch_ticker_list(self) -> list[str]:
        """
        Fetch all IDX Oil, Gas & Coal tickers using the where= filter parameter.

        Uses `where=sub_sector = "Oil, Gas & Coal"` (SQL-like syntax required by
        the Sectors API v2). The NL query parameter (q=) was abandoned because the
        server-side LLM translation generated invalid WHERE clauses (HTTP 400).

        As of June 2026 this sub-sector contains 89 companies on IDX. The default
        limit=100 covers them all in a single page — pagination is retained for
        future growth.
        """
        url = f"{BASE_URL}/companies/"
        tickers: list[str] = []
        offset = 0
        limit = 100

        while True:
            resp = self._safe_get(
                url,
                params={
                    "where": _SUB_SECTOR_WHERE,
                    "order_by": "-market_cap",
                    "limit": limit,
                    "offset": offset,
                },
            )
            data = resp.json()

            for item in data.get("results", []):
                symbol = item.get("symbol", "")
                clean = symbol.upper().replace(".JK", "").strip()
                if clean:
                    tickers.append(clean)

            pagination = data.get("pagination", {})
            if not pagination.get("has_next"):
                break
            offset = pagination.get("next_offset", offset + limit)
            time.sleep(_REQUEST_SLEEP)

        logger.info("Fetched %d Oil, Gas & Coal tickers", len(tickers))
        return tickers

    def _cutoff_year(self, run_date: str) -> int:
        """Return the minimum year to include: run_year - 1.

        Scope: tahun berjalan + 1 tahun ke belakang. Two consecutive years are
        sufficient for YoY EPS growth computation (current vs prior year).
        """
        return datetime.strptime(run_date, "%Y-%m-%d").year - 1

    @staticmethod
    def _is_last_friday_of_month(date_str: str) -> bool:
        """Return True if date_str (YYYY-MM-DD) is the last Friday of its calendar month."""
        from datetime import timedelta
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        if d.weekday() != 4:   # 4 = Friday
            return False
        return (d + timedelta(days=7)).month != d.month

    def _flatten_response(
        self, ticker: str, section: str, raw: dict, cutoff_year: int
    ) -> list[dict]:
        """Map a nested v2 company/report response to flat row dicts."""
        company_name = raw.get("company_name", "")

        if section == "overview":
            ov = raw.get("overview", {})
            return [{
                "ticker": ticker,
                "company_name": company_name,
                "sector": ov.get("sector"),
                "sub_sector": ov.get("sub_sector"),
                "market_cap": ov.get("market_cap"),
                "listing_date": ov.get("listing_date"),
            }]

        if section == "financials":
            fin = raw.get("financials", {})
            # historical_eps: {"2023": {"eps": 398.5, ...}, ...}
            hist_eps = {
                year_str: d.get("eps")
                for year_str, d in fin.get("historical_eps", {}).items()
            }
            rows = []
            for h in fin.get("historical_financials", []):
                year = h.get("year")
                if year is None or year < cutoff_year:
                    continue
                ocf = h.get("operating_cash_flow")
                fcf = h.get("free_cash_flow")
                capex = (ocf - fcf) if ocf is not None and fcf is not None else None
                rows.append({
                    "ticker": ticker,
                    "period": str(year),
                    "revenue": h.get("revenue"),
                    "gross_profit": h.get("gross_profit"),
                    "net_income": h.get("earnings"),   # v2 field name is "earnings"
                    "total_assets": h.get("total_assets"),
                    "total_equity": h.get("total_equity"),
                    "total_debt": h.get("total_debt"),
                    "operating_cash_flow": ocf,
                    "capex": capex,
                    "eps": hist_eps.get(str(year), h.get("eps")),
                })
            return rows

        if section == "valuation":
            val = raw.get("valuation", {})
            last_price = val.get("last_close_price")
            rows = []
            for h in val.get("historical_valuation", []):
                year = h.get("year")
                if year is None or year < cutoff_year:
                    continue
                rows.append({
                    "ticker": ticker,
                    "period": str(year),
                    "per": h.get("pe"),   # v2 uses "pe" not "per"
                    "pbv": h.get("pb"),   # v2 uses "pb" not "pbv"
                    "price": last_price,
                    "ev_ebitda": h.get("enterprise_to_ebitda"),
                })
            return rows

        # Generic fallback: return section dict as one flat row
        flat = {"ticker": ticker, "company_name": company_name}
        section_data = raw.get(section, {})
        if isinstance(section_data, dict):
            flat.update(section_data)
        return [flat]

    def _fetch_daily_prices_all(self, tickers: list[str], run_date: str) -> "pd.DataFrame":
        """Fetch latest daily price records for all tickers (no date filter).

        Raises APITimeoutError immediately if any ticker's request times out —
        the caller is responsible for logging and notification.
        """
        import pandas as pd
        records = []
        for ticker in tickers:
            try:
                url = f"{BASE_URL}/daily/{ticker}/"
                resp = self._safe_get(url)
                data = resp.json()
                time.sleep(_REQUEST_SLEEP)
                if isinstance(data, list):
                    for row in data:
                        row["ticker"] = ticker
                    records.extend(data)
                elif isinstance(data, dict):
                    data["ticker"] = ticker
                    records.append(data)
            except APITimeoutError:
                raise
            except Exception as exc:
                logger.warning("Skipping daily_prices/%s: %s", ticker, exc)
        return pd.DataFrame(records)

    def _fetch_friday_prices_history(
        self,
        tickers: list[str],
        run_date: str,
        years_back: int = 1,
    ) -> "dict[str, pd.DataFrame]":
        """
        Fetch full price history per ticker, then keep only the **last Friday of
        each calendar month** within *years_back* years of *run_date*.

        Scope: tahun berjalan + 1 tahun ke belakang → ~24 monthly data points.

        Returns a dict keyed by date string (YYYY-MM-DD) so each entry can be
        written as one bronze partition.

        Raises APITimeoutError immediately if any ticker's request times out.
        """
        import pandas as pd
        from datetime import date as date_type, timedelta

        run_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
        cutoff_dt = date_type(run_dt.year - years_back, 1, 1)

        all_records: list[dict] = []
        for ticker in tickers:
            try:
                url = f"{BASE_URL}/daily/{ticker}/"
                resp = self._safe_get(url)
                data = resp.json()
                time.sleep(_REQUEST_SLEEP)
                rows = data if isinstance(data, list) else [data]
                for row in rows:
                    row["ticker"] = ticker
                all_records.extend(rows)
            except APITimeoutError:
                raise
            except Exception as exc:
                logger.warning("Skipping history daily_prices/%s: %s", ticker, exc)

        if not all_records:
            logger.warning("No daily price records returned for any ticker")
            return {}

        df = pd.DataFrame(all_records)
        if "date" not in df.columns:
            logger.warning("API response has no 'date' column; cannot split by Friday")
            return {}

        df["_date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["_date"])

        # Keep only Fridays (dayofweek 4) within the window
        mask = (
            (df["_date"].dt.dayofweek == 4) &
            (df["_date"].dt.date >= cutoff_dt) &
            (df["_date"].dt.date <= run_dt)
        )
        df = df[mask].copy()

        if df.empty:
            logger.warning("No Friday records in [%s, %s]", cutoff_dt, run_dt)
            return {}

        # Keep only the last Friday of each calendar month
        df["_is_last_friday"] = df["_date"].apply(
            lambda d: (d + pd.Timedelta(days=7)).month != d.month
        )
        df = df[df["_is_last_friday"]].copy()

        if df.empty:
            logger.warning("No last-Friday-of-month records found in [%s, %s]", cutoff_dt, run_dt)
            return {}

        result: dict[str, "pd.DataFrame"] = {}
        for friday_str, group in df.groupby(df["_date"].dt.strftime("%Y-%m-%d")):
            result[friday_str] = group.drop(columns=["_date", "_is_last_friday"]).reset_index(drop=True)

        logger.info(
            "History: %d last-Friday-of-month partitions across %d tickers [%s → %s]",
            len(result), len(tickers), cutoff_dt, run_dt,
        )
        return result

    def fetch_domain(self, domain: str, run_date: str) -> "pd.DataFrame":
        """Fetch all oil/gas/coal IDX companies for a domain, limited to 1 year back.

        Raises APITimeoutError immediately if any API call times out.
        """
        import pandas as pd

        tickers = self._fetch_ticker_list()
        if not tickers:
            raise RuntimeError(
                "Ticker list is empty — check SECTOR_API_KEY and /v2/companies/ endpoint"
            )

        if domain == "daily_prices":
            return self._fetch_daily_prices_all(tickers, run_date)

        section = DOMAIN_TO_SECTION.get(domain)
        if section is None:
            raise ValueError(
                f"Unknown domain '{domain}'. Valid domains: {list(DOMAIN_TO_SECTION)}"
            )

        cutoff = self._cutoff_year(run_date)
        rows: list[dict] = []
        for ticker in tickers:
            try:
                raw = self.fetch_company_report(ticker, section)
                rows.extend(self._flatten_response(ticker, section, raw, cutoff))
            except APITimeoutError:
                raise  # propagate — stops all further ingestion immediately
            except Exception as exc:
                logger.warning("Skipping %s/%s: %s", domain, ticker, exc)

        return pd.DataFrame(rows)
