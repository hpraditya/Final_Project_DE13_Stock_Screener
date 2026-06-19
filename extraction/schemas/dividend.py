from pydantic import BaseModel
from typing import Optional


class DividendHistory(BaseModel):
    ticker: str
    period: str
    dividend_per_share: Optional[float] = None
    dividend_yield: Optional[float] = None
    payout_ratio: Optional[float] = None
