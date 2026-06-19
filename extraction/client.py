import time
import logging
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from extraction.endpoints import BASE_URL, SECTION_ENDPOINTS, DOMAIN_TO_SECTION

logger = logging.getLogger(__name__)

# Only fetch companies from the Oil, Gas & Coal sub-sector
_OIL_GAS_COAL_QUERY = "oil gas coal"


class SectorClient:
    def __init__(self, api_key: str, timeout: int = 30, max_retries: int = 3):
        self.api_key = api_key
        self.timeout = timeout
        self.session = self._build_session(max_retries)

    def _build_session(self, max_retries: int) -> requests.Session:
        session = requests.Session()
        # v2 uses the raw API key directly — no "Bearer" prefix
        session.headers.update({"Authorization": self.api_key})
        retry = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        return session

    def fetch_company_report(self, ticker: str, section: str) -> dict[str, Any]:
        if section not in SECTION_ENDPOINTS:
            raise KeyError(f"Unknown section '{section}'. Valid: {list(SECTION_ENDPOINTS)}")
        # v2 per-ticker endpoints use bare symbol — strip .JK suffix if present
        clean_ticker = ticker.upper().replace(".JK", "")
        url = f"{BASE_URL}/company/report/{clean_ticker}/"
        response = self.session.get(url, params={"sections": section}, timeout=self.timeout)
        response.raise_for_status()
        time.sleep(0.2)
        return response.json()

    def _fetch_ticker_list(self) -> list[str]:
        """Fetch all IDX oil/gas/coal tickers via NL query, handling pagination."""
        url = f"{BASE_URL}/companies/"
        tickers: list[str] = []
        offset = 0
        limit = 100

        while True:
            resp = self.session.get(
                url,
                params={"q": _OIL_GAS_COAL_QUERY, "limit": limit, "offset": offset},
                timeout=self.timeout,
            )
            resp.raise_for_status()
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
            time.sleep(0.1)

        logger.info("Fetched %d oil/gas/coal tickers", len(tickers))
        return tickers

    def _cutoff_year(self, run_date: str) -> int:
        """Return the minimum year to include: run_year - 2 (two years back for YoY growth)."""
        return datetime.strptime(run_date, "%Y-%m-%d").year - 2

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
        """Fetch latest daily price records for all tickers (no date filter)."""
        import pandas as pd
        records = []
        for ticker in tickers:
            try:
                url = f"{BASE_URL}/daily/{ticker}/"
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                data = resp.json()
                time.sleep(0.2)
                if isinstance(data, list):
                    for row in data:
                        row["ticker"] = ticker
                    records.extend(data)
                elif isinstance(data, dict):
                    data["ticker"] = ticker
                    records.append(data)
            except Exception as exc:
                logger.warning("Skipping daily_prices/%s: %s", ticker, exc)
        return pd.DataFrame(records)

    def fetch_domain(self, domain: str, run_date: str) -> "pd.DataFrame":
        """Fetch all oil/gas/coal IDX companies for a domain, limited to 1 year back."""
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
            except Exception as exc:
                logger.warning("Skipping %s/%s: %s", domain, ticker, exc)

        return pd.DataFrame(rows)
