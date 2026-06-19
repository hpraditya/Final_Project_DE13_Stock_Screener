-- ROE = net_income / total_equity
SELECT
    ticker,
    period,
    net_income / NULLIF(total_equity, 0) AS roe
FROM silver_financials
WHERE run_date = :run_date;
