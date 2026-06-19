from pydantic import BaseModel
from typing import Optional


class ValuationRatios(BaseModel):
    ticker: str
    period: str
    per: Optional[float] = None
    pbv: Optional[float] = None
    price: Optional[float] = None
    ev_ebitda: Optional[float] = None
