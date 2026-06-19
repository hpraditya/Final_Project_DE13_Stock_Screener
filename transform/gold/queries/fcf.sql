-- FCF = operating_cash_flow - capex
SELECT
    ticker,
    period,
    operating_cash_flow - capex AS fcf,
    (operating_cash_flow - capex) / NULLIF(revenue, 0) AS fcf_margin
FROM silver_financials
WHERE run_date = :run_date;
