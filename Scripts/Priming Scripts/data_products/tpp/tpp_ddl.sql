-- ==============================================================================
-- DP5 — assist_dev.tpp — Treasury Transmission & Payment Pipeline
-- Grain: transmittal batch event. Prompt Payment Act compliance. IPAC reconciliation.
-- ==============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.tpp.dim_transmittal_type
-- Transmittal type and specification dimension. SCD Type 1. Sources: aasbs_transmit.transmittal_envelo
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.tpp.dim_transmittal_type
(
    transmittal_type_sk                            BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    transmittal_type_cd                            STRING NOT NULL                COMMENT 'Transmittal type code — PO, RR, INV, BAAR, etc.',
    transmittal_type_desc                          STRING                         COMMENT 'Transmittal type description.',
    flat_file_code                                 STRING                         COMMENT 'Flat-file format code used in Pegasys transmissions.',
    spec_version                                   STRING                         COMMENT 'Current active specification version.',
    envelope_format                                STRING                         COMMENT 'Envelope structure format — EDI, CSV, XML.',
    poc_name                                       STRING                         COMMENT 'Point of contact name for this transmittal type.',
    poc_email                                      STRING                         COMMENT 'Point of contact email.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (transmittal_type_sk, transmittal_type_cd)
COMMENT 'Transmittal type and specification dimension. SCD Type 1. Sources: aasbs_transmit.transmittal_envelope, transmittal_specification, transmittal_spec_version, transmittal_poc.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'dim_transmittal_type',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP5'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.tpp.dim_aac_envelope
-- AAC-level transmission envelope dimension. SCD Type 1. Sources: aasbs_transmit.aac_envelope, transmi
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.tpp.dim_aac_envelope
(
    aac_envelope_sk                                BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    aac                                            STRING NOT NULL                COMMENT 'Activity Address Code — natural key.',
    envelope_status_cd                             STRING                         COMMENT 'Envelope status code.',
    poc_name                                       STRING                         COMMENT 'Transmission point of contact name.',
    poc_email                                      STRING                         COMMENT 'Transmission point of contact email.',
    last_transmission_dt                           TIMESTAMP                      COMMENT 'Date of most recent successful transmission.',
    is_suspended_flag                              BOOLEAN                        COMMENT 'TRUE if transmissions currently suspended for this AAC.',
    suspension_start_dt                            TIMESTAMP                      COMMENT 'Suspension window start date.',
    suspension_end_dt                              TIMESTAMP                      COMMENT 'Suspension window end date.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (aac_envelope_sk, aac)
COMMENT 'AAC-level transmission envelope dimension. SCD Type 1. Sources: aasbs_transmit.aac_envelope, transmittal_suspension.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'dim_aac_envelope',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP5'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.tpp.dim_agreement
-- BAAR agreement dimension. SCD Type 1. Sources: agreement.agreement, agreement_line, agreement_log.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.tpp.dim_agreement
(
    agreement_sk                                   BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    agreement_id                                   BIGINT NOT NULL                COMMENT 'NK — source agreement.agreement.id.',
    agreement_number                               STRING NOT NULL                COMMENT 'BAAR agreement number.',
    fund_status_cd                                 STRING                         COMMENT 'Agreement fund status.',
    obligations_available                          DECIMAL(15,2)                  COMMENT 'Available obligation authority remaining.',
    accounting_period                              STRING                         COMMENT 'BAAR accounting period.',
    total_agreement_amt                            DECIMAL(15,2)                  COMMENT 'Total agreement ceiling amount.',
    line_count                                     INT                            COMMENT 'Number of agreement lines.',
    last_transmitted_dt                            TIMESTAMP                      COMMENT 'Date agreement last transmitted to BAAR.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (agreement_sk, agreement_id)
COMMENT 'BAAR agreement dimension. SCD Type 1. Sources: agreement.agreement, agreement_line, agreement_log.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'dim_agreement',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP5'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.tpp.dim_receiving_report
-- Receiving report dimension. SCD Type 1. Sources: aasbs_transmit.rr_summary, rr_detail.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.tpp.dim_receiving_report
(
    rr_sk                                          BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    rr_summary_id                                  BIGINT NOT NULL                COMMENT 'NK — source aasbs_transmit.rr_summary.id.',
    pegasys_invoice_num                            STRING                         COMMENT 'Pegasys invoice number from receiving report.',
    rr_acceptance_dt                               TIMESTAMP                      COMMENT 'Receiving report acceptance date.',
    total_rr_amt                                   DECIMAL(15,2)                  COMMENT 'Total receiving report amount.',
    rr_line_count                                  INT                            COMMENT 'Number of lines on this receiving report.',
    mdl_amt                                        DECIMAL(15,2)                  COMMENT 'Maximum Dollar Limit amount per RR line.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (rr_sk, rr_summary_id)
COMMENT 'Receiving report dimension. SCD Type 1. Sources: aasbs_transmit.rr_summary, rr_detail.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'dim_receiving_report',
    'scd.type' = '1',
    'pipeline.data_product' = 'DP5'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.tpp.fact_transmission_event
-- Grain  : Transmittal batch (source_billing_summary_id) × transmission attempt
-- Treasury transmission event fact — one row per batch transmission attempt. Sources: aasbs_transmit.b
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.tpp.fact_transmission_event
(
    transmission_event_sk                          BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    transmittal_type_sk                            BIGINT NOT NULL                COMMENT 'FK to tpp.dim_transmittal_type.',
    aac_envelope_sk                                BIGINT                         COMMENT 'FK to tpp.dim_aac_envelope.',
    agreement_sk                                   BIGINT                         COMMENT 'FK to tpp.dim_agreement.',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia.',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency.',
    event_date_sk                                  INT                            COMMENT 'FK to common.dim_date — transmission date.',
    source_billing_summary_id                      BIGINT                         COMMENT 'Source aasbs_transmit.billing_summary.id.',
    transmittal_stage_cd                           STRING                         COMMENT 'Transmission pipeline stage — Staged, Transmitted, Confirmed, Error.',
    transmittal_status_cd                          STRING                         COMMENT 'Transmission status code.',
    transmitted_dt                                 TIMESTAMP                      COMMENT 'Timestamp transmission sent to Pegasys/VITAP.',
    confirmed_dt                                   TIMESTAMP                      COMMENT 'Timestamp confirmation received.',
    error_code                                     STRING                         COMMENT 'Error code if transmission failed.',
    error_description                              STRING                         COMMENT 'Error description text.',
    retry_count                                    INT                            COMMENT 'Number of retry attempts for failed transmissions.',
    batch_record_count                             INT                            COMMENT 'Number of records in this transmission batch.',
    transmitted_amt                                DECIMAL(15,2)                  COMMENT 'Total dollar amount in this transmission batch.',
    confirmed_amt                                  DECIMAL(15,2)                  COMMENT 'Amount confirmed by receiving system.',
    variance_amt                                   DECIMAL(15,2)                  COMMENT 'Variance between transmitted and confirmed amounts.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (transmittal_type_sk, aac_envelope_sk, event_date_sk)
COMMENT 'Treasury transmission event fact — one row per batch transmission attempt. Sources: aasbs_transmit.billing_summary, aac_envelope, transmit_error, agreement.agreement_log.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'fact_transmission_event',
    'pipeline.data_product' = 'DP5',
    'pipeline.consumer' = 'Treasury Ops, OIG, IPAC'
);

-- ────────────────────────────────────────────────────────────────────────────
-- assist_dev.tpp.fact_payment_reconciliation
-- Grain  : Invoice (source_inv_summary_id) × CLIN (line_item_sk)
-- Payment reconciliation fact — invoice to PO to RR to Pegasys payment. Prompt Payment Act compliance.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS assist_dev.tpp.fact_payment_reconciliation
(
    payment_recon_sk                               BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key.',
    line_item_sk                                   BIGINT NOT NULL                COMMENT 'FK to common.dim_line_item (CLIN).',
    rr_sk                                          BIGINT                         COMMENT 'FK to tpp.dim_receiving_report.',
    agreement_sk                                   BIGINT                         COMMENT 'FK to tpp.dim_agreement.',
    ia_sk                                          BIGINT                         COMMENT 'FK to common.dim_ia.',
    agency_sk                                      BIGINT                         COMMENT 'FK to common.dim_agency.',
    invoice_date_sk                                INT                            COMMENT 'FK to common.dim_date — invoice date.',
    payment_date_sk                                INT                            COMMENT 'FK to common.dim_date — Pegasys payment date.',
    source_inv_summary_id                          BIGINT                         COMMENT 'Source aasbs_transmit.inv_summary.id.',
    pegasys_invoice_num                            STRING                         COMMENT 'Pegasys invoice number.',
    invoice_amt                                    DECIMAL(15,2)                  COMMENT 'Invoice amount from inv_summary.',
    po_amt                                         DECIMAL(15,2)                  COMMENT 'PO line amount from po_line.',
    rr_amt                                         DECIMAL(15,2)                  COMMENT 'Receiving report accepted amount.',
    payment_amt                                    DECIMAL(15,2)                  COMMENT 'Actual Pegasys disbursement amount.',
    billing_amt                                    DECIMAL(15,2)                  COMMENT 'BAAR billing record amount.',
    invoice_to_payment_days                        INT                            COMMENT 'Calendar days from invoice date to payment (Prompt Payment metric).',
    is_prompt_payment_compliant                    BOOLEAN                        COMMENT 'TRUE if paid within 30 days per 31 U.S.C. 3901.',
    po_to_rr_variance_amt                          DECIMAL(15,2)                  COMMENT 'Variance between PO amount and RR accepted amount.',
    invoice_to_billing_variance_amt                DECIMAL(15,2)                  COMMENT 'Variance between invoice amount and billed amount.',
    _gold_created_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was first created by the pipeline.',
    _gold_updated_at                               TIMESTAMP                      COMMENT 'Timestamp this Gold row was last updated by the pipeline.',
    _source_batch_id                               STRING                         COMMENT 'Databricks job_run_id of pipeline run that last wrote this row.'
)
USING DELTA
CLUSTER BY (line_item_sk, invoice_date_sk)
COMMENT 'Payment reconciliation fact — invoice to PO to RR to Pegasys payment. Prompt Payment Act compliance. Sources: aasbs_transmit.inv_summary, inv_detail, po_line, po_accounting, rr_summary, billing.billing.'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.minReaderVersion' = '1',
    'delta.minWriterVersion' = '4',
    'pipeline.layer' = 'gold',
    'pipeline.catalog' = 'assist_catalog',
    'pipeline.schema' = 'tpp',
    'pipeline.table' = 'fact_payment_reconciliation',
    'pipeline.data_product' = 'DP5',
    'pipeline.consumer' = 'CFO Payment Ops, OIG, Prompt Payment'
);

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.tpp.v_prompt_payment_sla
-- Prompt Payment Act SLA dashboard — compliance rate and late payment amounts by agency and FY quarter
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.tpp.v_prompt_payment_sla
COMMENT 'Prompt Payment Act SLA dashboard — compliance rate and late payment amounts by agency and FY quarter. Required for OMB A-123 Appendix C reporting.'
AS
SELECT
    ag.agency_name, ia.ia_num,
    d_inv.federal_fiscal_fy, d_inv.federal_fiscal_qtr,
    COUNT(*)                                                            AS invoice_count,
    SUM(f.invoice_amt)                                                  AS total_invoiced,
    SUM(CASE WHEN f.is_prompt_payment_compliant THEN f.invoice_amt ELSE 0 END) AS compliant_amt,
    SUM(CASE WHEN NOT f.is_prompt_payment_compliant THEN f.invoice_amt ELSE 0 END) AS late_amt,
    AVG(f.invoice_to_payment_days)                                      AS avg_payment_days,
    MAX(f.invoice_to_payment_days)                                      AS max_payment_days,
    ROUND(100.0 * SUM(CASE WHEN f.is_prompt_payment_compliant THEN 1 ELSE 0 END) / COUNT(*), 2) AS compliance_pct
FROM assist_dev.tpp.fact_payment_reconciliation f
JOIN assist_dev.common.dim_agency  ag    ON f.agency_sk      = ag.agency_sk    AND ag.is_current_flag
JOIN assist_dev.common.dim_ia      ia    ON f.ia_sk          = ia.ia_sk        AND ia.is_current_flag
JOIN assist_dev.common.dim_date    d_inv ON f.invoice_date_sk = d_inv.date_sk
GROUP BY ALL;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.tpp.v_transmit_error_log
-- Active transmission error log — all failed batches with error codes and retry counts. Operational tr
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.tpp.v_transmit_error_log
COMMENT 'Active transmission error log — all failed batches with error codes and retry counts. Operational triage view for Financial Systems team.'
AS
SELECT
    tt.transmittal_type_cd, tt.transmittal_type_desc,
    ae.aac, ag.agency_name,
    f.transmitted_dt, f.error_code, f.error_description,
    f.retry_count, f.transmitted_amt,
    d.calendar_date AS transmission_date,
    DATEDIFF(CURRENT_TIMESTAMP(), f.transmitted_dt) AS hours_since_error
FROM assist_dev.tpp.fact_transmission_event f
JOIN assist_dev.tpp.dim_transmittal_type tt ON f.transmittal_type_sk = tt.transmittal_type_sk
JOIN assist_dev.tpp.dim_aac_envelope     ae ON f.aac_envelope_sk     = ae.aac_envelope_sk
JOIN assist_dev.common.dim_agency        ag ON f.agency_sk           = ag.agency_sk AND ag.is_current_flag
JOIN assist_dev.common.dim_date          d  ON f.event_date_sk       = d.date_sk
WHERE f.transmittal_status_cd = 'ERROR'
ORDER BY f.transmitted_dt DESC;

-- ────────────────────────────────────────────────────────────────────────────
-- VIEW: assist_dev.tpp.v_billing_vs_transmitted
-- Billing vs transmitted reconciliation — BAAR billed amounts compared to invoiced and paid. Major aud
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW assist_dev.tpp.v_billing_vs_transmitted
COMMENT 'Billing vs transmitted reconciliation — BAAR billed amounts compared to invoiced and paid. Major audit finding prevention view.'
AS
SELECT
    ia.ia_num, ag.agency_name,
    d.federal_fiscal_fy, d.federal_fiscal_qtr,
    SUM(f.billing_amt)         AS total_billed_baar,
    SUM(f.invoice_amt)         AS total_invoiced,
    SUM(f.payment_amt)         AS total_paid,
    SUM(f.invoice_to_billing_variance_amt) AS total_variance,
    SUM(f.po_to_rr_variance_amt)           AS total_po_rr_variance
FROM assist_dev.tpp.fact_payment_reconciliation f
JOIN assist_dev.common.dim_ia    ia ON f.ia_sk          = ia.ia_sk    AND ia.is_current_flag
JOIN assist_dev.common.dim_agency ag ON f.agency_sk     = ag.agency_sk AND ag.is_current_flag
JOIN assist_dev.common.dim_date   d ON f.invoice_date_sk = d.date_sk
GROUP BY ALL;
