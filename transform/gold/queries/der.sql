-- DER = total_debt / total_equity
SELECT
    ticker,
    period,
    total_debt / NULLIF(total_equity, 0) AS der
FROM silver_financials
WHERE run_date = :run_date;
