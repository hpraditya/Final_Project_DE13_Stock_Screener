from pydantic import BaseModel
from typing import Optional


class ManagementInfo(BaseModel):
    ticker: str
    ceo_name: Optional[str] = None
    insider_ownership_pct: Optional[float] = None
    institutional_ownership_pct: Optional[float] = None
