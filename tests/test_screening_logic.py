"""
Tests for transform/gold/scoring.py

Covers:
  - All 7 criteria pass → LOLOS
  - Each criterion failing individually → TIDAK
  - NaN values in metrics → treated as "not passed" (pandas NaN comparison returns False)
  - criteria_passed count accuracy
"""
import math
import pandas as pd
import pytest
from transform.gold.scoring import score, THRESHOLDS, TOTAL_CRITERIA


def _row(**overrides) -> pd.DataFrame:
    """Return a one-row DataFrame with all criteria at passing thresholds."""
    base = {
        "ticker": "TEST",
        "roe": THRESHOLDS["roe"] + 0.01,          # just above threshold
        "der": THRESHOLDS["der"] - 0.01,          # just below threshold
        "fcf": THRESHOLDS["fcf"] + 1,             # just above 0
        "eps_growth_yoy": THRESHOLDS["eps_growth_yoy"] + 0.01,
        "per": THRESHOLDS["per"] - 1,             # just below threshold
        "pbv": THRESHOLDS["pbv"] - 0.1,           # just below threshold
    }
    base.update(overrides)
    return pd.DataFrame([base])


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_lolos_when_all_7_criteria_met():
    result = score(_row())
    assert result.iloc[0]["status"] == "LOLOS"
    assert result.iloc[0]["criteria_passed"] == TOTAL_CRITERIA


def test_criteria_passed_is_integer():
    result = score(_row())
    assert isinstance(result.iloc[0]["criteria_passed"], (int, float))
    assert result.iloc[0]["criteria_passed"] == int(result.iloc[0]["criteria_passed"])


# ---------------------------------------------------------------------------
# Each criterion failing in isolation
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("failing_field,bad_value", [
    ("roe",            THRESHOLDS["roe"] - 0.01),   # below 15%
    ("der",            THRESHOLDS["der"] + 0.01),   # above 0.5
    ("fcf",            THRESHOLDS["fcf"] - 1),      # below 0
    ("eps_growth_yoy", THRESHOLDS["eps_growth_yoy"] - 0.01),
    ("per",            THRESHOLDS["per"] + 1),      # above 15
    ("pbv",            THRESHOLDS["pbv"] + 0.1),    # above 1.5
])
def test_tidak_when_one_criterion_fails(failing_field, bad_value):
    result = score(_row(**{failing_field: bad_value}))
    assert result.iloc[0]["status"] == "TIDAK"
    assert result.iloc[0]["criteria_passed"] == TOTAL_CRITERIA - 1


def test_tidak_when_graham_combined_exceeds_threshold():
    """per * pbv > 22.5 should fail even if per and pbv individually pass."""
    # per=13, pbv=1.8 → per * pbv = 23.4 > 22.5  (but per <= 15 and pbv <= 1.5 fail too
    #  here we specifically want graham to be the sole failing criterion)
    # per=10, pbv=2.3 → per * pbv = 23 > 22.5 ; but pbv > 1.5 also fails
    # To isolate graham: per=11, pbv=2.1 → both fail. Not easy to isolate.
    # Test that graham combined contributes to the count properly.
    df = pd.DataFrame([{
        "ticker": "GRAHAM",
        "roe": 0.20, "der": 0.3, "fcf": 100_000,
        "eps_growth_yoy": 0.15,
        "per": 14.0,   # passes per <= 15
        "pbv": 1.7,    # fails pbv <= 1.5  AND fails graham_combined
    }])
    result = score(df)
    # 2 criteria fail: pbv > 1.5 and per * pbv = 23.8 > 22.5
    assert result.iloc[0]["criteria_passed"] == TOTAL_CRITERIA - 2
    assert result.iloc[0]["status"] == "TIDAK"


# ---------------------------------------------------------------------------
# NaN edge cases
# ---------------------------------------------------------------------------

def test_nan_eps_growth_counts_as_not_passed():
    """
    Companies without two years of EPS data will have NaN eps_growth_yoy.
    pandas NaN comparison (NaN >= threshold) returns False → criterion not passed.
    """
    result = score(_row(eps_growth_yoy=float("nan")))
    assert result.iloc[0]["criteria_passed"] == TOTAL_CRITERIA - 1
    assert result.iloc[0]["status"] == "TIDAK"


def test_nan_roe_counts_as_not_passed():
    """NaN ROE (e.g., total_equity = 0) → roe criterion not passed."""
    result = score(_row(roe=float("nan")))
    assert result.iloc[0]["criteria_passed"] == TOTAL_CRITERIA - 1


def test_nan_per_and_pbv_count_as_not_passed():
    """If per and pbv are NaN (no valuation data), 3 criteria fail: per, pbv, graham."""
    result = score(_row(per=float("nan"), pbv=float("nan")))
    # per, pbv, and graham_combined (nan * nan = nan) all fail
    assert result.iloc[0]["criteria_passed"] == TOTAL_CRITERIA - 3


def test_all_nan_produces_zero_criteria():
    df = pd.DataFrame([{
        "ticker": "ALLNAN",
        "roe": float("nan"),
        "der": float("nan"),
        "fcf": float("nan"),
        "eps_growth_yoy": float("nan"),
        "per": float("nan"),
        "pbv": float("nan"),
    }])
    result = score(df)
    assert result.iloc[0]["criteria_passed"] == 0
    assert result.iloc[0]["status"] == "TIDAK"


# ---------------------------------------------------------------------------
# Multiple companies
# ---------------------------------------------------------------------------

def test_score_handles_multiple_rows():
    df = pd.DataFrame([
        {"ticker": "PASS", "roe": 0.20, "der": 0.3, "fcf": 500_000,
         "eps_growth_yoy": 0.12, "per": 12.0, "pbv": 1.2},
        {"ticker": "FAIL", "roe": 0.05, "der": 0.8, "fcf": -100_000,
         "eps_growth_yoy": -0.1, "per": 20.0, "pbv": 3.0},
    ])
    result = score(df)
    assert result.loc[result["ticker"] == "PASS", "status"].iloc[0] == "LOLOS"
    assert result.loc[result["ticker"] == "FAIL", "status"].iloc[0] == "TIDAK"


def test_score_does_not_mutate_input():
    df = _row()
    original_columns = set(df.columns)
    score(df)
    assert set(df.columns) == original_columns  # score() uses df.copy()
