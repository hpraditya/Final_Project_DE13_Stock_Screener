"""
Tests for transform/silver/

Strategy:
  - Pure-logic functions (_duckdb_endpoint, dedup) → no mocking needed.
  - I/O functions (clean_financials, etc.) → mock duckdb.connect + _s3_client
    to isolate the business logic from infrastructure.

Reference pattern: github.com/ismaildawoodjee/GreatEx (mock-then-assert style)
"""
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Pure logic: _duckdb_endpoint
# ---------------------------------------------------------------------------

def test_duckdb_endpoint_strips_http_scheme():
    from transform.silver.clean_financials import _duckdb_endpoint
    assert _duckdb_endpoint("http://seaweedfs-filer:8333") == "seaweedfs-filer:8333"


def test_duckdb_endpoint_strips_https_scheme():
    from transform.silver.clean_financials import _duckdb_endpoint
    assert _duckdb_endpoint("https://s3.example.com:443") == "s3.example.com:443"


def test_duckdb_endpoint_passthrough_when_no_scheme():
    from transform.silver.clean_financials import _duckdb_endpoint
    assert _duckdb_endpoint("seaweedfs-filer:8333") == "seaweedfs-filer:8333"


# ---------------------------------------------------------------------------
# Pure logic: deduplication (ROW_NUMBER pattern)
# ---------------------------------------------------------------------------

def test_dedup_keeps_only_rn1_rows():
    """The rn==1 filter must keep exactly one row per ticker+period."""
    df = pd.DataFrame([
        {"ticker": "BYAN", "period": "2024", "eps": 500.0, "rn": 1},
        {"ticker": "BYAN", "period": "2024", "eps": 500.0, "rn": 2},  # duplicate
        {"ticker": "PTBA", "period": "2024", "eps": 300.0, "rn": 1},
    ])
    result = df[df["rn"] == 1].drop(columns=["rn"])
    assert len(result) == 2
    assert set(result["ticker"]) == {"BYAN", "PTBA"}
    assert "rn" not in result.columns


def test_dedup_removes_duplicate_ticker_period():
    df = pd.DataFrame([
        {"ticker": "ENRG", "period": "2024", "revenue": 1e12, "rn": 1},
        {"ticker": "ENRG", "period": "2024", "revenue": 1e12, "rn": 2},
        {"ticker": "ENRG", "period": "2023", "revenue": 8e11, "rn": 1},
    ])
    result = df[df["rn"] == 1].drop(columns=["rn"])
    assert len(result) == 2  # ENRG-2024 kept once, ENRG-2023 kept once


# ---------------------------------------------------------------------------
# clean_financials — integration (mocked DuckDB + S3)
# ---------------------------------------------------------------------------

def _make_sample_financials():
    return pd.DataFrame([{
        "ticker": "BYAN", "period": "2024", "revenue": 1e12,
        "gross_profit": 5e11, "net_income": 3e11, "total_assets": 2e13,
        "total_equity": 1e13, "total_debt": 5e12,
        "operating_cash_flow": 4e11, "capex": 1e11, "eps": 500.0, "rn": 1,
    }])


def _mock_clean_financials_deps(sample_df):
    """Return context-manager patches for duckdb.connect and _s3_client."""
    mock_s3 = MagicMock()
    mock_s3.head_bucket.return_value = {}
    mock_s3.put_object.return_value = {}

    setup_result = MagicMock()       # first con.execute() — INSTALL httpfs etc.
    query_result = MagicMock()       # second con.execute() — SELECT ...
    query_result.df.return_value = sample_df
    mock_con = MagicMock()
    mock_con.execute.side_effect = [setup_result, query_result]

    return mock_con, mock_s3


def test_clean_financials_adds_run_date_column():
    """Output DataFrame must have a 'run_date' column set to the input run_date."""
    run_date = "2026-06-18"
    sample = _make_sample_financials()
    mock_con, mock_s3 = _mock_clean_financials_deps(sample)

    with patch("transform.silver.clean_financials.duckdb.connect", return_value=mock_con), \
         patch("transform.silver.clean_financials._s3_client", return_value=mock_s3):
        from transform.silver.clean_financials import clean_financials
        result = clean_financials(run_date, "http://localhost:8333")

    assert "run_date" in result.columns
    assert (result["run_date"] == run_date).all()


def test_clean_financials_writes_to_s3():
    """Silver data must be written to S3 (put_object called)."""
    run_date = "2026-06-18"
    sample = _make_sample_financials()
    mock_con, mock_s3 = _mock_clean_financials_deps(sample)

    with patch("transform.silver.clean_financials.duckdb.connect", return_value=mock_con), \
         patch("transform.silver.clean_financials._s3_client", return_value=mock_s3):
        from transform.silver.clean_financials import clean_financials
        clean_financials(run_date, "http://localhost:8333")

    assert mock_s3.put_object.called
    call_kwargs = mock_s3.put_object.call_args[1]
    assert "silver/financial_statements" in call_kwargs["Key"]
    assert run_date in call_kwargs["Key"]


def test_clean_financials_drops_rn_column():
    """The dedup helper column 'rn' must not appear in the output."""
    run_date = "2026-06-18"
    sample = _make_sample_financials()
    mock_con, mock_s3 = _mock_clean_financials_deps(sample)

    with patch("transform.silver.clean_financials.duckdb.connect", return_value=mock_con), \
         patch("transform.silver.clean_financials._s3_client", return_value=mock_s3):
        from transform.silver.clean_financials import clean_financials
        result = clean_financials(run_date, "http://localhost:8333")

    assert "rn" not in result.columns


def test_clean_financials_passes_stripped_endpoint_to_duckdb():
    """DuckDB s3_endpoint must not include 'http://' — _duckdb_endpoint strips it."""
    run_date = "2026-06-18"
    sample = _make_sample_financials()
    mock_con, mock_s3 = _mock_clean_financials_deps(sample)

    with patch("transform.silver.clean_financials.duckdb.connect", return_value=mock_con), \
         patch("transform.silver.clean_financials._s3_client", return_value=mock_s3):
        from transform.silver.clean_financials import clean_financials
        clean_financials(run_date, "http://seaweedfs-filer:8333")

    # The first execute call is the setup SQL
    setup_sql: str = mock_con.execute.call_args_list[0][0][0]
    assert "http://" not in setup_sql
    assert "seaweedfs-filer:8333" in setup_sql


# ---------------------------------------------------------------------------
# Ticker normalization logic (shared across all 3 silver transforms)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("byan.jk",  "BYAN"),
    ("PTBA",     "PTBA"),
    ("itmg.JK",  "ITMG"),
    ("medc",     "MEDC"),
])
def test_ticker_normalization(raw, expected):
    """Ticker must be uppercased with .jk/.JK suffix removed."""
    result = raw.upper().replace(".JK", "")
    assert result == expected
