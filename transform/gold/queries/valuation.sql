-- Valuation ratios + Graham Combined
SELECT
    v.ticker,
    v.period,
    v.per,
    v.pbv,
    v.per * v.pbv AS graham_combined,
    v.price,
    v.ev_ebitda
FROM silver_valuation v
WHERE run_date = :run_date;
