-- fct_valuation_snapshot — point-in-time valuation ratios per ticker.
--
-- STATUS: Schema created but NOT currently populated by the pipeline.
-- Valuation ratios are embedded inside gold.fct_buffett_screening.
-- This table is reserved for a future use case where daily/weekly
-- valuation snapshots should be persisted independently.
--
-- To populate: add a silver→gold step that writes clean_valuation output here.
CREATE TABLE IF NOT EXISTS gold.fct_valuation_snapshot (
    ticker          VARCHAR(10) NOT NULL,
    run_date        DATE NOT NULL,
    period          VARCHAR(10),              -- e.g. '2024'
    per             NUMERIC,
    pbv             NUMERIC,
    price           NUMERIC,
    ev_ebitda       NUMERIC,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, run_date)
);
