-- dim_fiscal_period — lookup table for fiscal period metadata.
--
-- period_key format: "2024" (four-digit year string, annual only).
-- The pipeline fetches annual data from the Sectors API v2; quarterly
-- periods are not supported. The original "2023-12" format was used
-- in the deprecated v1 API and is no longer applicable.
CREATE TABLE IF NOT EXISTS gold.dim_fiscal_period (
    period_key      VARCHAR(10) PRIMARY KEY,  -- e.g. '2024' (annual fiscal year)
    fiscal_year     INT,
    fiscal_month    INT,                      -- always 12 for annual periods
    period_type     VARCHAR(10)               -- 'annual' (quarterly not yet supported)
);
