-- EPS growth YoY and 5-year CAGR
-- Note: eps_cagr_5y requires 6 periods of history; will be NULL with only 2 years of data.
WITH ranked AS (
    SELECT
        ticker,
        period,
        run_date,
        eps,
        LAG(eps, 1) OVER (PARTITION BY ticker ORDER BY period) AS eps_prev_1y,
        LAG(eps, 5) OVER (PARTITION BY ticker ORDER BY period) AS eps_prev_5y
    FROM silver_financials
)
SELECT
    ticker,
    period,
    (eps - eps_prev_1y) / NULLIF(ABS(eps_prev_1y), 0)        AS eps_growth_yoy,
    POWER(eps / NULLIF(ABS(eps_prev_5y), 0), 1.0/5) - 1      AS eps_cagr_5y
FROM ranked
WHERE run_date = :run_date;
