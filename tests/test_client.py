"""
Tests for extraction/client.py

Covers:
  - _cutoff_year()          : pure date logic
  - _flatten_response()     : v2 API field mapping (overview, financials, valuation)
  - fetch_company_report()  : ticker .JK stripping + HTTP call
  - _fetch_ticker_list()    : pagination with mock requests
"""
import pytest
from unittest.mock import patch, MagicMock, call
from extraction.client import SectorClient


@pytest.fixture
def client():
    return SectorClient(api_key="test_key")


# ---------------------------------------------------------------------------
# _cutoff_year
# ---------------------------------------------------------------------------

def test_cutoff_year_is_run_year_minus_2(client):
    """Must return run_year - 2 to allow YoY EPS growth computation."""
    assert client._cutoff_year("2026-06-18") == 2024
    assert client._cutoff_year("2025-01-01") == 2023
    assert client._cutoff_year("2024-12-31") == 2022


# ---------------------------------------------------------------------------
# _flatten_response — overview
# ---------------------------------------------------------------------------

def test_flatten_overview_returns_one_row(client):
    raw = {
        "company_name": "Bayan Resources Tbk",
        "overview": {
            "sector": "Energy",
            "sub_sector": "Coal",
            "market_cap": 5_000_000_000_000,
            "listing_date": "2008-08-12",
        },
    }
    rows = client._flatten_response("BYAN", "overview", raw, cutoff_year=2024)
    assert len(rows) == 1
    r = rows[0]
    assert r["ticker"] == "BYAN"
    assert r["company_name"] == "Bayan Resources Tbk"
    assert r["sector"] == "Energy"
    assert r["market_cap"] == 5_000_000_000_000


# ---------------------------------------------------------------------------
# _flatten_response — financials
# ---------------------------------------------------------------------------

def test_flatten_financials_renames_earnings_to_net_income(client):
    """v2 API uses 'earnings'; pipeline expects 'net_income'."""
    raw = {
        "company_name": "BYAN",
        "financials": {
            "historical_eps": {"2024": {"eps": 500.0}},
            "historical_financials": [{
                "year": 2024, "revenue": 1e12, "gross_profit": 4e11,
                "earnings": 2e11,  # v2 field name
                "total_assets": 5e12, "total_equity": 2e12, "total_debt": 1e12,
                "operating_cash_flow": 3e11, "free_cash_flow": 2e11,
            }],
        },
    }
    rows = client._flatten_response("BYAN", "financials", raw, cutoff_year=2023)
    assert len(rows) == 1
    assert rows[0]["net_income"] == 2e11


def test_flatten_financials_computes_capex_as_ocf_minus_fcf(client):
    """capex = operating_cash_flow - free_cash_flow (v2 doesn't expose capex directly)."""
    raw = {
        "company_name": "BYAN",
        "financials": {
            "historical_eps": {},
            "historical_financials": [{
                "year": 2024, "revenue": 1e12, "gross_profit": 4e11, "earnings": 2e11,
                "total_assets": 5e12, "total_equity": 2e12, "total_debt": 1e12,
                "operating_cash_flow": 3e11, "free_cash_flow": 2e11,
            }],
        },
    }
    rows = client._flatten_response("BYAN", "financials", raw, cutoff_year=2023)
    assert rows[0]["capex"] == pytest.approx(1e11)  # 3e11 - 2e11


def test_flatten_financials_excludes_years_before_cutoff(client):
    """Years < cutoff_year must be filtered out."""
    raw = {
        "company_name": "BYAN",
        "financials": {
            "historical_eps": {},
            "historical_financials": [
                {"year": 2022, "revenue": 1e12, "gross_profit": 3e11, "earnings": 1e11,
                 "total_assets": 5e12, "total_equity": 2e12, "total_debt": 1e12,
                 "operating_cash_flow": 2e11, "free_cash_flow": 1e11},
                {"year": 2024, "revenue": 2e12, "gross_profit": 6e11, "earnings": 2e11,
                 "total_assets": 7e12, "total_equity": 3e12, "total_debt": 1.5e12,
                 "operating_cash_flow": 4e11, "free_cash_flow": 3e11},
            ],
        },
    }
    rows = client._flatten_response("BYAN", "financials", raw, cutoff_year=2023)
    assert len(rows) == 1
    assert rows[0]["period"] == "2024"


def test_flatten_financials_eps_from_historical_eps_dict(client):
    """historical_eps is a dict-of-dicts {year_str: {eps: value}}, not a list."""
    raw = {
        "company_name": "BYAN",
        "financials": {
            "historical_eps": {
                "2024": {"eps": 498.5},
                "2023": {"eps": 320.0},
            },
            "historical_financials": [
                {"year": 2024, "revenue": 1e12, "gross_profit": 4e11, "earnings": 2e11,
                 "total_assets": 5e12, "total_equity": 2e12, "total_debt": 1e12,
                 "operating_cash_flow": 3e11, "free_cash_flow": 2e11},
            ],
        },
    }
    rows = client._flatten_response("BYAN", "financials", raw, cutoff_year=2023)
    assert rows[0]["eps"] == pytest.approx(498.5)


# ---------------------------------------------------------------------------
# _flatten_response — valuation
# ---------------------------------------------------------------------------

def test_flatten_valuation_renames_pe_pb_to_per_pbv(client):
    """v2 API uses 'pe' and 'pb'; must be renamed to 'per' and 'pbv'."""
    raw = {
        "company_name": "BYAN",
        "valuation": {
            "last_close_price": 25_000,
            "historical_valuation": [
                {"year": 2024, "pe": 8.5, "pb": 1.2, "enterprise_to_ebitda": 4.0},
            ],
        },
    }
    rows = client._flatten_response("BYAN", "valuation", raw, cutoff_year=2023)
    assert len(rows) == 1
    assert rows[0]["per"] == 8.5
    assert rows[0]["pbv"] == 1.2
    assert rows[0]["price"] == 25_000
    assert rows[0]["ev_ebitda"] == 4.0


def test_flatten_valuation_excludes_years_before_cutoff(client):
    raw = {
        "company_name": "BYAN",
        "valuation": {
            "last_close_price": 25_000,
            "historical_valuation": [
                {"year": 2021, "pe": 5.0, "pb": 0.8, "enterprise_to_ebitda": 3.0},
                {"year": 2024, "pe": 8.5, "pb": 1.2, "enterprise_to_ebitda": 4.0},
            ],
        },
    }
    rows = client._flatten_response("BYAN", "valuation", raw, cutoff_year=2023)
    assert len(rows) == 1
    assert rows[0]["period"] == "2024"


# ---------------------------------------------------------------------------
# fetch_company_report — HTTP + ticker cleaning
# ---------------------------------------------------------------------------

def test_fetch_company_report_strips_jk_suffix(client):
    """Ticker with .JK suffix must be stripped before the URL is built."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = {}
    mock_resp.raise_for_status.return_value = None

    with patch.object(client.session, "get", return_value=mock_resp) as mock_get:
        client.fetch_company_report("BYAN.JK", "financials")

    called_url: str = mock_get.call_args[0][0]
    assert "BYAN.JK" not in called_url
    assert "/BYAN/" in called_url


def test_fetch_company_report_raises_on_http_error(client):
    """HTTP errors must propagate so Airflow retries the task."""
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = Exception("500 Server Error")

    with patch.object(client.session, "get", return_value=mock_resp):
        with pytest.raises(Exception, match="500 Server Error"):
            client.fetch_company_report("BYAN", "financials")


# ---------------------------------------------------------------------------
# _fetch_ticker_list — pagination
# ---------------------------------------------------------------------------

def test_fetch_ticker_list_stops_when_no_next_page(client):
    """Pagination must stop when has_next is False."""
    page1 = {
        "results": [{"symbol": "BYAN.JK"}, {"symbol": "PTBA.JK"}],
        "pagination": {"has_next": False, "total_count": 2, "showing": 2},
    }
    mock_resp = MagicMock()
    mock_resp.json.return_value = page1
    mock_resp.raise_for_status.return_value = None

    with patch.object(client.session, "get", return_value=mock_resp):
        tickers = client._fetch_ticker_list()

    assert tickers == ["BYAN", "PTBA"]


def test_fetch_ticker_list_paginates_through_all_pages(client):
    """Must follow has_next=True and accumulate results across pages."""
    page1 = {
        "results": [{"symbol": "BYAN.JK"}],
        "pagination": {"has_next": True, "next_offset": 1},
    }
    page2 = {
        "results": [{"symbol": "PTBA.JK"}, {"symbol": "ITMG.JK"}],
        "pagination": {"has_next": False},
    }
    mock_resp = MagicMock()
    mock_resp.json.side_effect = [page1, page2]
    mock_resp.raise_for_status.return_value = None

    with patch.object(client.session, "get", return_value=mock_resp):
        tickers = client._fetch_ticker_list()

    assert tickers == ["BYAN", "PTBA", "ITMG"]
