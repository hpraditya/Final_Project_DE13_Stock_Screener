BASE_URL = "https://api.sectors.app/v2"

SECTION_ENDPOINTS = {
    "overview": "overview",
    "financials": "financials",
    "valuation": "valuation",
    "dividend": "dividend",
    "management": "management",
    "trading_info": "trading_info",
}

DOMAIN_TO_SECTION = {
    "company_profile": "overview",
    "financial_statements": "financials",
    "valuation_ratios": "valuation",
    "dividend_history": "dividend",
    "management_info": "management",
    # daily_prices uses a dedicated /daily/{ticker}/ endpoint handled in SectorClient
    "daily_prices": "trading_info",
}
