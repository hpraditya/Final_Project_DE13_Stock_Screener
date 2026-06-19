from pydantic import BaseModel
from typing import Optional


class FinancialStatement(BaseModel):
    ticker: str
    period: str  # e.g. "2024" (integer year as string; v2 API returns annual data only)
    revenue: Optional[float] = None
    gross_profit: Optional[float] = None
    net_income: Optional[float] = None
    total_assets: Optional[float] = None
    total_equity: Optional[float] = None
    total_debt: Optional[float] = None
    operating_cash_flow: Optional[float] = None
    capex: Optional[float] = None
    eps: Optional[float] = None
