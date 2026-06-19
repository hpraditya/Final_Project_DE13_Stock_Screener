-- fct_fundamentals_annual — raw annual fundamentals per ticker/period.
--
-- STATUS: Schema created but NOT currently populated by the pipeline.
-- The pipeline writes to gold.fct_buffett_screening directly.
-- This table is reserved for a future "store all fundamentals" use case
-- where raw financials should be persisted independently of the screening run.
--
-- To populate: add a silver→gold step that writes clean_financials output here.
CREATE TABLE IF NOT EXISTS gold.fct_fundamentals_annual (
    ticker              VARCHAR(10) NOT NULL,
    period              VARCHAR(10) NOT NULL,  -- e.g. '2024'
    revenue             NUMERIC,
    gross_profit        NUMERIC,
    net_income          NUMERIC,
    total_assets        NUMERIC,
    total_equity        NUMERIC,
    total_debt          NUMERIC,
    operating_cash_flow NUMERIC,
    capex               NUMERIC,
    eps                 NUMERIC,
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, period)
);
