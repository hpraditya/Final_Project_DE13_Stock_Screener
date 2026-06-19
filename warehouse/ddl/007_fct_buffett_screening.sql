CREATE TABLE IF NOT EXISTS gold.fct_buffett_screening (
    ticker              VARCHAR(10) NOT NULL,
    run_date            DATE NOT NULL,
    period              VARCHAR(10),
    company_name        TEXT,
    sector              TEXT,
    roe                 NUMERIC,
    der                 NUMERIC,
    fcf                 NUMERIC,
    fcf_margin          NUMERIC,
    eps_growth_yoy      NUMERIC,
    eps_cagr_5y         NUMERIC,
    per                 NUMERIC,
    pbv                 NUMERIC,
    graham_combined     NUMERIC,
    criteria_passed     INT,
    status              VARCHAR(10) CHECK (status IN ('LOLOS', 'TIDAK')),
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, run_date)
);

CREATE INDEX IF NOT EXISTS idx_buffett_screening_status
    ON gold.fct_buffett_screening (status, run_date);

CREATE INDEX IF NOT EXISTS idx_buffett_screening_sector
    ON gold.fct_buffett_screening (sector, run_date);
