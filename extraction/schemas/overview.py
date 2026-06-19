from pydantic import BaseModel, Field
from typing import Optional


class CompanyOverview(BaseModel):
    ticker: str
    company_name: str
    sector: Optional[str] = None
    sub_sector: Optional[str] = None
    market_cap: Optional[float] = None
    listing_date: Optional[str] = None
