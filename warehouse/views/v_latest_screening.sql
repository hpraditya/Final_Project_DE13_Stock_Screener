CREATE OR REPLACE VIEW gold.v_latest_screening AS
SELECT
    s.*,
    c.listing_date
FROM gold.fct_buffett_screening s
JOIN gold.dim_company c ON s.ticker = c.ticker
WHERE s.run_date = (SELECT MAX(run_date) FROM gold.fct_buffett_screening)
ORDER BY s.criteria_passed DESC, s.roe DESC;
