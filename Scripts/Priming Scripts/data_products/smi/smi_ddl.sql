-- ==============================================================================
-- DP7 — assist_dev.smi — Solicitation & Market Intelligence
-- Grain: contractor response per solicitation. Competition analytics. Should-cost benchmarking.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.smi.dim_posting_type
-- Solicitation posting type dimension. SCD Type 1. Sources: aasbs.solicit_posting_connect, solicit_pos
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.smi.dim_posting_type
(
    posting_type_sk                                BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    solicit_id                                     BIGINT NOT NULL                COMMENT 'NK — source aasbs.solicit.id.',
    posting_type_cd                                STRING NOT NULL                COMMENT 'Posting type code — DIRECT_CONNECT, DIRECT, OPEN.',
    posting_type_desc                              STRING                         COMMENT 'Posting type description.',
    open_dt                                        TIMESTAMP                      COMMENT 'Date solicitation opened.',
    pm_responsible_user_id                         STRING                         COMMENT 'Program Manager responsible for this posting.',
    ja_reason_cd                                   STRING                         COMMENT 'J&A reason code for direct (other-than-full-and-open).',
    external_source_ref                            STRING                         COMMENT 'External source reference for direct posting.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (posting_type_sk, solicit_id)
COMMENT 'Solicitation posting type dimension. SCD Type 1. Sources: aasbs.solicit_posting_connect, solicit_posting_direct.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'smi',
    'pipeline.table' = 'dim_posting_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP7'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.smi.dim_piid
-- PIID dimension — generated procurement instrument identifiers with component decomposition. SCD Type
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.smi.dim_piid
(
    piid_sk                                        BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    piid_id                                        BIGINT NOT NULL                COMMENT 'NK — source aasbs.piid.id.',
    piid_value                                     STRING NOT NULL                COMMENT 'Full PIID string.',
    base_piid                                      STRING                         COMMENT 'Base PIID without extension.',
    piid_ext                                       STRING                         COMMENT 'Modification/amendment extension.',
    aac                                            STRING                         COMMENT 'AAC component of PIID.',
    instrument_type_cd                             STRING                         COMMENT 'Instrument type component.',
    sequence_num                                   STRING                         COMMENT 'Sequence number component.',
    piid_fiscal_year                               INT                            COMMENT 'Fiscal year component of PIID.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (piid_sk, piid_id)
COMMENT 'PIID dimension — generated procurement instrument identifiers with component decomposition. SCD Type 1. Sources: aasbs.piid, piid_ext, vw_procurement_order.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'smi',
    'pipeline.table' = 'dim_piid',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP7'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.smi.fact_solicitation_response
-- Grain  : Contractor response (source_response_id) × solicitation (solicitation_sk)
-- Solicitation response fact — one row per contractor per solicitation. Captures winning and losing bi
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.smi.fact_solicitation_response
(
    sol_response_sk                                BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    solicitation_sk                                BIGINT NOT NULL                COMMENT 'FK to alt.dim_solicitation.',
    acquisition_sk                                 BIGINT NOT NULL                COMMENT 'FK to alt.dim_acquisition.',
    contractor_sk                                  BIGINT                         COMMENT 'FK to alt.dim_contractor (responding company).',
    posting_type_sk                                BIGINT                         COMMENT 'FK to smi.dim_posting_type.',
    piid_sk                                        BIGINT                         COMMENT 'FK to smi.dim_piid (resulting award PIID).',
    agency_sk                                      BIGINT NOT NULL                COMMENT 'FK to common.dim_agency.',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia.',
    response_date_sk                               INT                            COMMENT 'FK to common.dim_date — response received date.',
    award_date_sk                                  INT                            COMMENT 'FK to common.dim_date — award date (winners only).',
    source_response_id                             BIGINT                         COMMENT 'Source aasbs.solicit_response.id.',
    response_status_cd                             STRING                         COMMENT 'Response status — Submitted, Withdrawn, Awarded, Not Selected.',
    is_winner_flag                                 BOOLEAN                        COMMENT 'TRUE if this response resulted in an award.',
    is_small_business_flag                         BOOLEAN                        COMMENT 'TRUE if responding company was small business at time.',
    bid_total_amt                                  DECIMAL(15,2)                  COMMENT 'Total bid price from contractor response.',
    bid_clin_count                                 INT                            COMMENT 'Number of CLINs priced in this response.',
    award_total_amt                                DECIMAL(15,2)                  COMMENT 'Awarded amount (NULL for losing bids).',
    bid_vs_award_variance                          DECIMAL(15,2)                  COMMENT 'Variance between bid and award amount (winners only).',
    competition_rank                               INT                            COMMENT 'Rank of this bid by price among all responses.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (solicitation_sk, contractor_sk)
COMMENT 'Solicitation response fact — one row per contractor per solicitation. Captures winning and losing bids. Sources: aasbs.solicit_response, solicit_company, line_item_response, award_referenced_solicit.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'smi',
    'pipeline.table' = 'fact_solicitation_response',
    'pipeline.data_product' = 'DP7',
    'pipeline.consumer' = 'Contracting Officers, Cost/Price Analysts'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.smi.v_competition_analytics
-- Competition analytics view — response rates, small business participation, and award amounts by soli
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.smi.v_competition_analytics
COMMENT 'Competition analytics view — response rates, small business participation, and award amounts by solicitation type and FY. Supports CICA compliance and Section 809 reporting.'
AS
SELECT
    ag.agency_name, ia.ia_num,
    acq.acquisition_type_desc, acq.competition_type_desc,
    d_open.federal_fiscal_fy                        AS solicitation_fy,
    COUNT(DISTINCT f.solicitation_sk)               AS solicitation_count,
    AVG(COUNT(f.sol_response_sk)) OVER
        (PARTITION BY f.solicitation_sk)            AS avg_responses_per_sol,
    SUM(CASE WHEN f.is_small_business_flag THEN 1 ELSE 0 END) AS sb_responses,
    ROUND(100.0 * SUM(CASE WHEN f.is_small_business_flag THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2)                   AS sb_response_pct,
    SUM(f.award_total_amt)                          AS total_awarded
FROM assist_dev.smi.fact_solicitation_response f
JOIN assist_dev.alt.dim_acquisition     acq    ON f.acquisition_sk  = acq.acquisition_sk AND acq.is_current_flag
JOIN assist_dev.common.dim_agency       ag     ON f.agency_sk       = ag.agency_sk       AND ag.is_current_flag
JOIN assist_dev.common.dim_ia           ia     ON f.ia_sk           = ia.ia_sk           AND ia.is_current_flag
JOIN assist_dev.common.dim_date         d_open ON f.response_date_sk = d_open.date_sk
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.smi.v_bid_price_distribution
-- Bid price distribution view — min, max, median, and winning bid amounts per solicitation. Supports s
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.smi.v_bid_price_distribution
COMMENT 'Bid price distribution view — min, max, median, and winning bid amounts per solicitation. Supports should-cost analysis and price reasonableness benchmarking (FAR 15.404).'
AS
SELECT
    sol.solicit_num, sol.sol_status_desc,
    ag.agency_name, ia.ia_num,
    COUNT(f.sol_response_sk)                        AS total_responses,
    MIN(f.bid_total_amt)                            AS min_bid,
    MAX(f.bid_total_amt)                            AS max_bid,
    AVG(f.bid_total_amt)                            AS avg_bid,
    PERCENTILE(f.bid_total_amt, 0.5)               AS median_bid,
    MAX(CASE WHEN f.is_winner_flag THEN f.bid_total_amt END) AS winning_bid,
    MAX(CASE WHEN f.is_winner_flag THEN f.bid_total_amt END)
        - MIN(f.bid_total_amt)                      AS winning_vs_low_spread
FROM assist_dev.smi.fact_solicitation_response f
JOIN assist_dev.alt.dim_solicitation    sol ON f.solicitation_sk = sol.solicitation_sk
JOIN assist_dev.common.dim_ia           ia  ON f.ia_sk           = ia.ia_sk  AND ia.is_current_flag
JOIN assist_dev.common.dim_agency       ag  ON f.agency_sk       = ag.agency_sk AND ag.is_current_flag
GROUP BY ALL;
