CREATE TABLE IF NOT EXISTS gold.dim_company (
    ticker          VARCHAR(10) PRIMARY KEY,
    company_name    TEXT NOT NULL,
    sector          TEXT,
    sub_sector      TEXT,
    market_cap      NUMERIC,
    listing_date    DATE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
