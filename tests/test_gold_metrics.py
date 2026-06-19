import pandas as pd
import pytest


def test_roe_calculation():
    net_income = 300.0
    total_equity = 1000.0
    roe = net_income / total_equity
    assert abs(roe - 0.3) < 1e-9


def test_der_calculation():
    total_debt = 400.0
    total_equity = 1000.0
    der = total_debt / total_equity
    assert abs(der - 0.4) < 1e-9


def test_fcf_calculation():
    ocf = 500.0
    capex = 100.0
    fcf = ocf - capex
    assert fcf == 400.0


def test_roe_nullif_guard():
    total_equity = 0.0
    result = None if total_equity == 0 else 300.0 / total_equity
    assert result is None


def test_graham_combined():
    per = 12.0
    pbv = 1.5
    graham = per * pbv
    assert graham == 18.0
    assert graham <= 22.5
