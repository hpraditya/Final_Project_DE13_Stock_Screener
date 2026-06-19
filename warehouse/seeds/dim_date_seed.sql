-- Seed dim_date from 2024-01-01 to 2030-12-31
INSERT INTO gold.dim_date (run_date, year, quarter, month, week_of_year, day_of_week, is_friday)
SELECT
    d::DATE AS run_date,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(QUARTER FROM d) AS quarter,
    EXTRACT(MONTH FROM d) AS month,
    EXTRACT(WEEK FROM d) AS week_of_year,
    EXTRACT(DOW FROM d) AS day_of_week,
    EXTRACT(DOW FROM d) = 5 AS is_friday
FROM generate_series('2024-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS t(d)
ON CONFLICT (run_date) DO NOTHING;
