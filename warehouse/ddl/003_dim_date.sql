CREATE TABLE IF NOT EXISTS gold.dim_date (
    run_date        DATE PRIMARY KEY,
    year            INT,
    quarter         INT,
    month           INT,
    week_of_year    INT,
    day_of_week     INT,
    is_friday       BOOLEAN
);
