-- -------------------------------------------------------------
-- Table: aasbs.acceptance
-- -------------------------------------------------------------

CREATE TABLE "aasbs"."acceptance" (
    "id" bigint NOT NULL,
    "invoice_id" bigint NOT NULL,
    "gsa_acceptance_status_cd" character varying(20) NOT NULL,
    "total_approved_invoice_amt" numeric(15,2) NOT NULL,
    "client_acceptance_status_cd" character varying(35),
    "client_receipt_dt" timestamp(6) without time zone,
    "client_acceptance_dt" timestamp(6) without time zone,
    "client_authorized_dt" timestamp(6) without time zone,
    "client_authorized_user_id" character varying(16),
    "gsa_receipt_dt" timestamp(6) without time zone,
    "gsa_acceptance_dt" timestamp(6) without time zone,
    "gsa_authorized_user_id" character varying(16),
    "gsa_authorized_dt" timestamp(6) without time zone,
    "draft_acceptance_status_cd" character varying(35),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "determination_reason" character varying(4000),
    CONSTRAINT "pk_acceptance" PRIMARY KEY (id),
    CONSTRAINT "ck_acpt_draft_null_when_final" CHECK ((gsa_acceptance_status_cd::text = ANY (ARRAY['ACCEPTED'::character varying::text, 'ACCEPTED_PARTIAL'::character varying::text, 'REJECTED'::character varying::text])) AND draft_acceptance_status_cd IS NULL OR (gsa_acceptance_status_cd::text <> ALL (ARRAY['ACCEPTED'::character varying::text, 'ACCEPTED_PARTIAL'::character varying::text, 'REJECTED'::character varying::text]))),
    CONSTRAINT "fk_acptance_accept_status" FOREIGN KEY (gsa_acceptance_status_cd) REFERENCES aasbs.lu_acceptance_status(cd),
    CONSTRAINT "fk_acptance_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acptance_cli_acpt_stat" FOREIGN KEY (client_acceptance_status_cd) REFERENCES aasbs.lu_acceptance_status(cd),
    CONSTRAINT "fk_acptance_cli_auth_userid" FOREIGN KEY (client_authorized_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acptance_draft_acpt_stat" FOREIGN KEY (draft_acceptance_status_cd) REFERENCES aasbs.lu_acceptance_status(cd),
    CONSTRAINT "fk_acptance_gsa_auth_userid" FOREIGN KEY (gsa_authorized_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acptance_invoice_id" FOREIGN KEY (invoice_id) REFERENCES aasbs.invoice(id),
    CONSTRAINT "fk_acptance_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_acptance_cbui ON aasbs.acceptance USING btree (created_by_user_id);
CREATE INDEX ix_acptance_cli_acpt_cli_stat ON aasbs.acceptance USING btree (client_acceptance_status_cd);
CREATE INDEX ix_acptance_cli_acpt_gsa_stat ON aasbs.acceptance USING btree (gsa_acceptance_status_cd);
CREATE INDEX ix_acptance_cli_auth_userid ON aasbs.acceptance USING btree (client_authorized_user_id);
CREATE INDEX ix_acptance_draft_acpt_stat ON aasbs.acceptance USING btree (draft_acceptance_status_cd);
CREATE INDEX ix_acptance_gsa_auth_userid ON aasbs.acceptance USING btree (gsa_authorized_user_id);
CREATE INDEX ix_acptance_invoice_id ON aasbs.acceptance USING btree (invoice_id);
CREATE INDEX ix_acptance_ubui ON aasbs.acceptance USING btree (updated_by_user_id);

CREATE TRIGGER acceptance_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.acceptance FOR EACH ROW EXECUTE FUNCTION aasbs.trg_acceptance_hst();

COMMENT ON COLUMN "aasbs"."acceptance"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACCEPTANCE_SEQ';
COMMENT ON COLUMN "aasbs"."acceptance"."invoice_id" IS 'Foreign Key to table AASBS.INVOICE';
COMMENT ON COLUMN "aasbs"."acceptance"."gsa_acceptance_status_cd" IS 'Foreign key to AASBS.LU_ACCEPTANCE_STATUS';
COMMENT ON COLUMN "aasbs"."acceptance"."total_approved_invoice_amt" IS 'Total approved amount for this Invoice';
COMMENT ON COLUMN "aasbs"."acceptance"."client_acceptance_status_cd" IS 'Foreign key to AASBS.LU_ACCEPTANCE_STATUS';
COMMENT ON COLUMN "aasbs"."acceptance"."client_receipt_dt" IS 'Client Receipt date of Invoice';
COMMENT ON COLUMN "aasbs"."acceptance"."client_acceptance_dt" IS 'Client Acceptance date of Invoice';
COMMENT ON COLUMN "aasbs"."acceptance"."client_authorized_dt" IS 'Client Authorized date of Invoice';
COMMENT ON COLUMN "aasbs"."acceptance"."client_authorized_user_id" IS 'Foreign Key to table ASSIST.USERS';
COMMENT ON COLUMN "aasbs"."acceptance"."gsa_receipt_dt" IS 'GSA Receipt date of Invoice';
COMMENT ON COLUMN "aasbs"."acceptance"."gsa_acceptance_dt" IS 'GSA Acceptance date of Invoice';
COMMENT ON COLUMN "aasbs"."acceptance"."gsa_authorized_user_id" IS 'Foreign Key to table ASSIST.USERS';
COMMENT ON COLUMN "aasbs"."acceptance"."gsa_authorized_dt" IS 'Date GSA Authorized the Acceptance Report';
COMMENT ON COLUMN "aasbs"."acceptance"."draft_acceptance_status_cd" IS 'Foreign Key to table AASBS.LU_DRAFT_ACCEPTANCE_STATUS';
COMMENT ON COLUMN "aasbs"."acceptance"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."acceptance"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."acceptance"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."acceptance"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."acceptance"."determination_reason" IS 'Reason for Rejection or Partial Acceptance from GSA user or VITAP';

COMMENT ON TABLE "aasbs"."acceptance" IS 'Contains high-level invoice details for all invoices within the ASSIST application';

-- ------------------------------------------------------------
-- Table: aasbs.acceptance_dist
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."acceptance_dist" (
    "id" bigint NOT NULL,
    "acceptance_id" bigint NOT NULL,
    "acceptance_item_id" bigint NOT NULL,
    "distribute_amt" numeric(15,2) NOT NULL,
    "tracking_num" character varying(20) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_acceptance_dist" PRIMARY KEY (id),
    CONSTRAINT "fk_acpt_dist_acpt_item_id" FOREIGN KEY (acceptance_item_id) REFERENCES aasbs.acceptance_item(id),
    CONSTRAINT "fk_acpt_dist_acptance_id" FOREIGN KEY (acceptance_id) REFERENCES aasbs.acceptance(id),
    CONSTRAINT "fk_acpt_dist_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acpt_dist_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_acpt_dist_acpt_item_id ON aasbs.acceptance_dist USING btree (acceptance_item_id);
CREATE INDEX ix_acpt_dist_acptance_id ON aasbs.acceptance_dist USING btree (acceptance_id);
CREATE INDEX ix_acpt_dist_cbui ON aasbs.acceptance_dist USING btree (created_by_user_id);
CREATE INDEX ix_acpt_dist_ubui ON aasbs.acceptance_dist USING btree (updated_by_user_id);

CREATE TRIGGER acceptance_dist_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.acceptance_dist FOR EACH ROW EXECUTE FUNCTION aasbs.trg_acceptance_dist_hst();

COMMENT ON COLUMN "aasbs"."acceptance_dist"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACCEPTANCE_DIST_SEQ';
COMMENT ON COLUMN "aasbs"."acceptance_dist"."acceptance_id" IS 'Foreign Key to table AASBS.ACCEPTANCE';
COMMENT ON COLUMN "aasbs"."acceptance_dist"."acceptance_item_id" IS 'Foreign Key to table AASBS.ACCEPTANCE_ITEM';
COMMENT ON COLUMN "aasbs"."acceptance_dist"."distribute_amt" IS 'Amount to be billed for an acceptance item';
COMMENT ON COLUMN "aasbs"."acceptance_dist"."tracking_num" IS 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via its Tracking_Num';
COMMENT ON COLUMN "aasbs"."acceptance_dist"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."acceptance_dist"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."acceptance_dist"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."acceptance_dist"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."acceptance_dist" IS 'Contains the one or more Line Items under a single Invoice';

-- ------------------------------------------------------------
-- Table: aasbs.acceptance_item
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."acceptance_item" (
    "id" bigint NOT NULL,
    "acceptance_id" bigint NOT NULL,
    "invoice_item_id" bigint NOT NULL,
    "acceptance_item_type_cd" character varying(20) NOT NULL,
    "line_item_accepted_id" bigint NOT NULL,
    "item_client_recommended_amt" numeric(15,2) NOT NULL,
    "item_gsa_approved_amt" numeric(15,2) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "distribution_status_cd" character varying(25),
    CONSTRAINT "pk_acceptance_item" PRIMARY KEY (id),
    CONSTRAINT "fk_acceptance_item_distribution_status" FOREIGN KEY (distribution_status_cd) REFERENCES aasbs.lu_ar_distribution_status(cd),
    CONSTRAINT "fk_acptance_item_acptance_id" FOREIGN KEY (acceptance_id) REFERENCES aasbs.acceptance(id),
    CONSTRAINT "fk_acptance_item_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acptance_item_invce_item_id" FOREIGN KEY (invoice_item_id) REFERENCES aasbs.invoice_item(id),
    CONSTRAINT "fk_acptance_item_li_accepted" FOREIGN KEY (line_item_accepted_id) REFERENCES aasbs.line_item_accepted(id),
    CONSTRAINT "fk_acptance_item_type_cd" FOREIGN KEY (acceptance_item_type_cd) REFERENCES aasbs.lu_acceptance_item_type(cd),
    CONSTRAINT "fk_acptance_item_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_acptance_item_acptance_id ON aasbs.acceptance_item USING btree (acceptance_id);
CREATE INDEX ix_acptance_item_cbui ON aasbs.acceptance_item USING btree (created_by_user_id);
CREATE INDEX ix_acptance_item_invc_item_id ON aasbs.acceptance_item USING btree (invoice_item_id);
CREATE INDEX ix_acptance_item_li_accepted ON aasbs.acceptance_item USING btree (line_item_accepted_id);
CREATE INDEX ix_acptance_item_type_cd ON aasbs.acceptance_item USING btree (acceptance_item_type_cd);
CREATE INDEX ix_acptance_item_ubui ON aasbs.acceptance_item USING btree (updated_by_user_id);
CREATE INDEX ix_ai_distribution_status_cd ON aasbs.acceptance_item USING btree (distribution_status_cd);

CREATE TRIGGER acceptance_item_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.acceptance_item FOR EACH ROW EXECUTE FUNCTION aasbs.trg_acceptance_item_hst();

COMMENT ON COLUMN "aasbs"."acceptance_item"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACCEPTANCE_ITEM_SEQ';
COMMENT ON COLUMN "aasbs"."acceptance_item"."acceptance_id" IS 'Foreign Key to table AASBS.ACCEPTANCE';
COMMENT ON COLUMN "aasbs"."acceptance_item"."invoice_item_id" IS 'Foreign Key to table AASBS.INVOICE_ITEM';
COMMENT ON COLUMN "aasbs"."acceptance_item"."acceptance_item_type_cd" IS 'Foreign Key to table AASBS.LU_ACCEPTANCE_ITEM_TYPE';
COMMENT ON COLUMN "aasbs"."acceptance_item"."line_item_accepted_id" IS 'Foreign Key to table AASBS.LINE_ITEM_ACCEPTED';
COMMENT ON COLUMN "aasbs"."acceptance_item"."item_client_recommended_amt" IS 'Client-Recommended amount for this Invoiced Item';
COMMENT ON COLUMN "aasbs"."acceptance_item"."item_gsa_approved_amt" IS 'GSA-Approved amount for this Invoiced Item';
COMMENT ON COLUMN "aasbs"."acceptance_item"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."acceptance_item"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."acceptance_item"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."acceptance_item"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."acceptance_item"."distribution_status_cd" IS 'Foreign key to AASBS.LU_AR_DISTRIBUTION_STATUS';

COMMENT ON TABLE "aasbs"."acceptance_item" IS 'Contains the one or more Line Items under a single Invoice';

-- ------------------------------------------------------------
-- Table: aasbs.accrual_expense
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."accrual_expense" (
    "id" bigint NOT NULL,
    "accrual_year" integer NOT NULL,
    "accrual_month" character varying(2) NOT NULL,
    "accrual_month_end_dt" timestamp(6) without time zone NOT NULL,
    "line_item_accepted_id" bigint NOT NULL,
    "acquisition_id" bigint NOT NULL,
    "award_id" bigint NOT NULL,
    "award_mod_id" bigint NOT NULL,
    "line_item_start_dt" timestamp(6) without time zone NOT NULL,
    "line_item_end_dt" timestamp(6) without time zone NOT NULL,
    "ceiling_amt" numeric(15,2) NOT NULL,
    "obligated_amt" numeric(15,2) NOT NULL,
    "invoice_approved_amt" numeric(15,2) NOT NULL,
    "udo_amt" numeric(15,2) NOT NULL,
    "pop_completed_pct" numeric(11,8) NOT NULL,
    "pop_delivered_amt" numeric(15,2) NOT NULL,
    "prelim_completed_accrual_amt" numeric(15,2) NOT NULL,
    "accrual_expense_amt" numeric(15,2) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "include_in_transmission_yn" character(1),
    "invalid_udo_amt" numeric(15,2),
    "contract_type_cd" character varying(30),
    "accrual_universal_holdback_pct" numeric(19,2),
    "usr_keyed_holdback" numeric(19,2),
    "udo_reduced_by_invalid_amt" numeric(15,2),
    "accrual_expense_amt_before_holdback" numeric(15,2),
    "accrual_expense_amt_with_holdback_applied" numeric(15,2),
    "accrual_expense_calc_comment" character varying(3000),
    "accrual_holdback_type_cd" character varying(30),
    "accrual_transmission_status_cd" character varying(40),
    "udo_saf_amt" numeric(15,2),
    CONSTRAINT "pk_accrual_expense" PRIMARY KEY (id),
    CONSTRAINT "fk_accrual_expense_ahtc" FOREIGN KEY (accrual_holdback_type_cd) REFERENCES aasbs.lu_accrual_holdback_type(cd),
    CONSTRAINT "fk_accrual_expense_atsc" FOREIGN KEY (accrual_transmission_status_cd) REFERENCES aasbs.lu_accrual_transmission_status(cd),
    CONSTRAINT "fk_acrl_exp_acqid" FOREIGN KEY (acquisition_id) REFERENCES aasbs.acquisition(id),
    CONSTRAINT "fk_acrl_exp_amid" FOREIGN KEY (award_mod_id) REFERENCES aasbs.award_mod(id),
    CONSTRAINT "fk_acrl_exp_awdid" FOREIGN KEY (award_id) REFERENCES aasbs.award(id),
    CONSTRAINT "fk_acrl_exp_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acrl_exp_liaid" FOREIGN KEY (line_item_accepted_id) REFERENCES aasbs.line_item_accepted(id),
    CONSTRAINT "fk_acrl_exp_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_acrl_exp_acqid ON aasbs.accrual_expense USING btree (acquisition_id);
CREATE INDEX ix_acrl_exp_amid ON aasbs.accrual_expense USING btree (award_mod_id);
CREATE INDEX ix_acrl_exp_awdid ON aasbs.accrual_expense USING btree (award_id);
CREATE INDEX ix_acrl_exp_cbui ON aasbs.accrual_expense USING btree (created_by_user_id);
CREATE INDEX ix_acrl_exp_liaid ON aasbs.accrual_expense USING btree (line_item_accepted_id);
CREATE INDEX ix_acrl_exp_ubui ON aasbs.accrual_expense USING btree (updated_by_user_id);
CREATE INDEX ix_ae_accrual_holdback_type_cd ON aasbs.accrual_expense USING btree (accrual_holdback_type_cd);
CREATE INDEX ix_ae_accrual_transmission_status_cd ON aasbs.accrual_expense USING btree (accrual_transmission_status_cd);
CREATE UNIQUE INDEX uk_acrl_exp_y_m_liaid ON aasbs.accrual_expense USING btree (accrual_year, accrual_month, line_item_accepted_id);

CREATE TRIGGER accrual_expense_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.accrual_expense FOR EACH ROW EXECUTE FUNCTION aasbs.trg_accrual_expense_hst();

COMMENT ON COLUMN "aasbs"."accrual_expense"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_EXPENSE_SEQ';
COMMENT ON COLUMN "aasbs"."accrual_expense"."accrual_year" IS '4 digit calendar year for accrual run.';
COMMENT ON COLUMN "aasbs"."accrual_expense"."accrual_month" IS '2 character calendar month for accrual run.';
COMMENT ON COLUMN "aasbs"."accrual_expense"."accrual_month_end_dt" IS 'Last calendar date of the month for accrual run.';
COMMENT ON COLUMN "aasbs"."accrual_expense"."line_item_accepted_id" IS 'Foreign Key to table AASBS.LINE_ITEM_ACCEPTED';
COMMENT ON COLUMN "aasbs"."accrual_expense"."acquisition_id" IS 'Foreign Key to table AASBS.ACQUISITION';
COMMENT ON COLUMN "aasbs"."accrual_expense"."award_id" IS 'Foreign Key to table AASBS.AWARD';
COMMENT ON COLUMN "aasbs"."accrual_expense"."award_mod_id" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs"."accrual_expense"."line_item_start_dt" IS 'Service Start date for the latest awarded line item';
COMMENT ON COLUMN "aasbs"."accrual_expense"."line_item_end_dt" IS 'Service End date for the latest awarded line item';
COMMENT ON COLUMN "aasbs"."accrual_expense"."ceiling_amt" IS 'Ceiling amount for the latest awarded line item';
COMMENT ON COLUMN "aasbs"."accrual_expense"."obligated_amt" IS 'Funded (Obligated) amount for the latest awarded line item';
COMMENT ON COLUMN "aasbs"."accrual_expense"."invoice_approved_amt" IS 'Sum of Approved Invoice amounts from Acceptance Reports';
COMMENT ON COLUMN "aasbs"."accrual_expense"."udo_amt" IS 'Undelivered Order (UDO) = Line Item Funded/Obligated Cost amount minus Invoice Approved amount';
COMMENT ON COLUMN "aasbs"."accrual_expense"."pop_completed_pct" IS 'Percentage of the Period of Performance that has passed:  (Accrual month end date - POP Start) / (POP End - POP Start)';
COMMENT ON COLUMN "aasbs"."accrual_expense"."pop_delivered_amt" IS 'Delivered amount based on POP Percent Completed:  (POP Percent Completed * Ceiling amount)';
COMMENT ON COLUMN "aasbs"."accrual_expense"."prelim_completed_accrual_amt" IS 'Preliminary Completed Accrual amount-can be negative:  (POP Delivered amount – Invoice Approved amount)';
COMMENT ON COLUMN "aasbs"."accrual_expense"."accrual_expense_amt" IS 'Expense Accrual amount:  If Prelim Completed Accrual < 0, then 0.  If Prelim Completed Accrual > UDO then UDO else Prelim Completed Accrual.';
COMMENT ON COLUMN "aasbs"."accrual_expense"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."accrual_expense"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."accrual_expense"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."accrual_expense"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."accrual_expense"."include_in_transmission_yn" IS 'Specifies if record will be included in transmission';
COMMENT ON COLUMN "aasbs"."accrual_expense"."invalid_udo_amt" IS 'Invalid udo amount';
COMMENT ON COLUMN "aasbs"."accrual_expense"."contract_type_cd" IS 'Contract type code';
COMMENT ON COLUMN "aasbs"."accrual_expense"."accrual_universal_holdback_pct" IS 'Accrual universal holdback percent';
COMMENT ON COLUMN "aasbs"."accrual_expense"."usr_keyed_holdback" IS 'User accrual holdback percent';
COMMENT ON COLUMN "aasbs"."accrual_expense"."udo_reduced_by_invalid_amt" IS 'Udo reduced by invalid udo amount';
COMMENT ON COLUMN "aasbs"."accrual_expense"."accrual_expense_amt_before_holdback" IS 'Accrual expense before holdback applied';
COMMENT ON COLUMN "aasbs"."accrual_expense"."accrual_expense_amt_with_holdback_applied" IS 'Accrual expense with holdback applied';
COMMENT ON COLUMN "aasbs"."accrual_expense"."accrual_expense_calc_comment" IS 'Accrual expense calculation comments';
COMMENT ON COLUMN "aasbs"."accrual_expense"."accrual_holdback_type_cd" IS 'Accrual Holdback Type';
COMMENT ON COLUMN "aasbs"."accrual_expense"."accrual_transmission_status_cd" IS 'Accrual Transmission Status Code';
COMMENT ON COLUMN "aasbs"."accrual_expense"."udo_saf_amt" IS 'Line item total SAF amount. When aasbs.loa.subject_to_availability_yn = Y, then SAF';

COMMENT ON TABLE "aasbs"."accrual_expense" IS 'Monthly Accrual Expense calculations for deliverable line items';

-- ------------------------------------------------------------
-- Table: aasbs.accrual_inclusion_tracker
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."accrual_inclusion_tracker" (
    "id" bigint NOT NULL,
    "li_tracking_num" bigint NOT NULL,
    "accrual_inclusion_status_cd" character varying(20),
    "was_manually_set_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "invalid_udo_amt" numeric(15,2),
    "usr_keyed_holdback" numeric(19,2),
    "comments" character varying(3000),
    "accrual_holdback_type_cd" character varying(30),
    CONSTRAINT "pk_accrual_inclusion_tracker" PRIMARY KEY (id),
    CONSTRAINT "ck_acrl_incl_trkr_actyn" CHECK (was_manually_set_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_accrual_inclusion_tracker_ahtc" FOREIGN KEY (accrual_holdback_type_cd) REFERENCES aasbs.lu_accrual_holdback_type(cd),
    CONSTRAINT "fk_acrl_incl_trkr_aoscd" FOREIGN KEY (accrual_inclusion_status_cd) REFERENCES aasbs.lu_accrual_inclusion_status(cd),
    CONSTRAINT "fk_acrl_incl_trkr_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acrl_incl_trkr_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX idx_accrual_inclusion_tracker_created_dt ON aasbs.accrual_inclusion_tracker USING btree (created_dt);
CREATE INDEX idx_accrual_inclusion_tracker_updated_dt ON aasbs.accrual_inclusion_tracker USING btree (updated_dt);
CREATE INDEX ix_acrl_incl_trkr_aoscd ON aasbs.accrual_inclusion_tracker USING btree (was_manually_set_yn);
CREATE INDEX ix_acrl_incl_trkr_awdid ON aasbs.accrual_inclusion_tracker USING btree (li_tracking_num);
CREATE INDEX ix_acrl_incl_trkr_cbui ON aasbs.accrual_inclusion_tracker USING btree (created_by_user_id);
CREATE INDEX ix_acrl_incl_trkr_liaid ON aasbs.accrual_inclusion_tracker USING btree (accrual_inclusion_status_cd);
CREATE INDEX ix_acrl_incl_trkr_ubui ON aasbs.accrual_inclusion_tracker USING btree (updated_by_user_id);
CREATE INDEX ix_ait_accrual_holdback_type_cd ON aasbs.accrual_inclusion_tracker USING btree (accrual_holdback_type_cd);
CREATE UNIQUE INDEX uk_acrl_incl_trkr_li_tnum ON aasbs.accrual_inclusion_tracker USING btree (li_tracking_num);

CREATE TRIGGER accrual_inclusion_tracker_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.accrual_inclusion_tracker FOR EACH ROW EXECUTE FUNCTION aasbs.trg_accrual_inclusion_tracker_hst();

COMMENT ON COLUMN "aasbs"."accrual_inclusion_tracker"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_INCLUSION_TRACKER_SEQ';
COMMENT ON COLUMN "aasbs"."accrual_inclusion_tracker"."li_tracking_num" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_INCLUSION_TRACKER_SEQ';
COMMENT ON COLUMN "aasbs"."accrual_inclusion_tracker"."accrual_inclusion_status_cd" IS 'Foreign Key to table AASBS.LU_ACCRUAL_INCLUSION_TRACKER_STATUS';
COMMENT ON COLUMN "aasbs"."accrual_inclusion_tracker"."was_manually_set_yn" IS 'Flag indicating if the acrrual inclusion status was manually set by a user (=Y) or not (=N).  Not/N indicates the status was set automatically';
COMMENT ON COLUMN "aasbs"."accrual_inclusion_tracker"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."accrual_inclusion_tracker"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."accrual_inclusion_tracker"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."accrual_inclusion_tracker"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."accrual_inclusion_tracker"."accrual_holdback_type_cd" IS 'Accrual Holdback Type';

COMMENT ON TABLE "aasbs"."accrual_inclusion_tracker" IS 'Via Financial Services, stores FSD decisions to override the default selection of line items for Accrual calculations';

-- ------------------------------------------------------------
-- Table: aasbs.accrual_income
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."accrual_income" (
    "id" bigint NOT NULL,
    "accrual_expense_id" bigint NOT NULL,
    "accrual_year" integer NOT NULL,
    "accrual_month" character varying(2) NOT NULL,
    "accrual_month_end_dt" timestamp(6) without time zone NOT NULL,
    "line_item_accepted_id" bigint NOT NULL,
    "surcharge_rate_pct" numeric(7,4) NOT NULL,
    "accrual_income_amt" numeric(15,2) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "include_in_transmission_yn" character(1),
    "li_obligated_amt" numeric(15,2),
    "li_billed_amt" numeric(15,2),
    "li_unbilled_amt" numeric(15,2),
    "li_surcharge_yn" character(1),
    "accrual_income_calc_comment" character varying(250),
    "accrual_transmission_status_cd" character varying(40),
    CONSTRAINT "pk_accrual_income" PRIMARY KEY (id),
    CONSTRAINT "fk_accrual_income_atsc" FOREIGN KEY (accrual_transmission_status_cd) REFERENCES aasbs.lu_accrual_transmission_status(cd),
    CONSTRAINT "fk_acrl_inc_accexp" FOREIGN KEY (accrual_expense_id) REFERENCES aasbs.accrual_expense(id),
    CONSTRAINT "fk_acrl_inc_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acrl_inc_liaid" FOREIGN KEY (line_item_accepted_id) REFERENCES aasbs.line_item_accepted(id),
    CONSTRAINT "fk_acrl_inc_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_acrl_inc_accexp ON aasbs.accrual_income USING btree (accrual_expense_id);
CREATE INDEX ix_acrl_inc_cbui ON aasbs.accrual_income USING btree (created_by_user_id);
CREATE INDEX ix_acrl_inc_liaid ON aasbs.accrual_income USING btree (line_item_accepted_id);
CREATE INDEX ix_acrl_inc_ubui ON aasbs.accrual_income USING btree (updated_by_user_id);
CREATE INDEX ix_ai_accrual_transmission_status_cd ON aasbs.accrual_income USING btree (accrual_transmission_status_cd);
CREATE UNIQUE INDEX uk_acrl_inc_y_m_liaid ON aasbs.accrual_income USING btree (accrual_year, accrual_month, line_item_accepted_id);

CREATE TRIGGER accrual_income_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.accrual_income FOR EACH ROW EXECUTE FUNCTION aasbs.trg_accrual_income_hst();

COMMENT ON COLUMN "aasbs"."accrual_income"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_INCOME_SEQ';
COMMENT ON COLUMN "aasbs"."accrual_income"."accrual_expense_id" IS 'Foreign Key to table AASBS.ACCRUAL_EXPENSE';
COMMENT ON COLUMN "aasbs"."accrual_income"."accrual_year" IS '4 digit calendar year for accrual run.';
COMMENT ON COLUMN "aasbs"."accrual_income"."accrual_month" IS '2 character calendar month for accrual run.';
COMMENT ON COLUMN "aasbs"."accrual_income"."accrual_month_end_dt" IS 'Last calendar date of the month for accrual run.';
COMMENT ON COLUMN "aasbs"."accrual_income"."line_item_accepted_id" IS 'Foreign Key to table AASBS.LINE_ITEM_ACCEPTED';
COMMENT ON COLUMN "aasbs"."accrual_income"."surcharge_rate_pct" IS 'Surcharge percentage rate from LI_AWARD.SURCHARGE_RATE_PCT';
COMMENT ON COLUMN "aasbs"."accrual_income"."accrual_income_amt" IS 'Income Accrual amount:  For Cost, ACCRUAL_EXPENSE.ACCRUAL_EXPEENSE_AMT.  For Fee, ACCRUAL_EXPENSE.ACCRUAL_EXPENSE_AMT * Surcharge Rate.';
COMMENT ON COLUMN "aasbs"."accrual_income"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."accrual_income"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."accrual_income"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."accrual_income"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."accrual_income"."include_in_transmission_yn" IS 'Specifies if record will be included in transmission';
COMMENT ON COLUMN "aasbs"."accrual_income"."li_obligated_amt" IS 'Line item obligated amount';
COMMENT ON COLUMN "aasbs"."accrual_income"."li_billed_amt" IS 'Line item billed amount';
COMMENT ON COLUMN "aasbs"."accrual_income"."li_unbilled_amt" IS 'Line item unbilled amount';
COMMENT ON COLUMN "aasbs"."accrual_income"."li_surcharge_yn" IS 'Line item is LI_AWARD_SURCHG_PCT';
COMMENT ON COLUMN "aasbs"."accrual_income"."accrual_income_calc_comment" IS 'Accrual income calculation comments';
COMMENT ON COLUMN "aasbs"."accrual_income"."accrual_transmission_status_cd" IS 'Accrual Transmission Status Code';

COMMENT ON TABLE "aasbs"."accrual_income" IS 'Monthly Accrual Income calculations for deliverable and surcharge line items';

-- ------------------------------------------------------------
-- Table: aasbs.accrual_income_dist
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."accrual_income_dist" (
    "id" bigint NOT NULL,
    "accrual_income_id" bigint NOT NULL,
    "tracking_num" character varying(20) NOT NULL,
    "distribute_amt" numeric(15,2) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "include_in_transmission_yn" character(1),
    "loa_obligated_amt" numeric(15,2),
    "loa_billed_amt" numeric(15,2),
    "loa_unbilled_amt" numeric(15,2),
    "burn_order" bigint,
    "proration_pct" numeric(19,16),
    "proration_dist_amt" numeric(15,2),
    "accrual_income_dist_comment" character varying(250),
    "accrual_transmission_status_cd" character varying(40),
    "subject_to_availability_yn" character(1),
    CONSTRAINT "pk_accrual_income_dist" PRIMARY KEY (id),
    CONSTRAINT "fk_accrual_income_dist_atsc" FOREIGN KEY (accrual_transmission_status_cd) REFERENCES aasbs.lu_accrual_transmission_status(cd),
    CONSTRAINT "fk_acrl_incd_aiid" FOREIGN KEY (accrual_income_id) REFERENCES aasbs.accrual_income(id),
    CONSTRAINT "fk_acrl_incd_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acrl_incd_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX idx_aid_tracking_num ON aasbs.accrual_income_dist USING btree (tracking_num);
CREATE INDEX ix_acrl_incd_aiid ON aasbs.accrual_income_dist USING btree (accrual_income_id);
CREATE INDEX ix_acrl_incd_cbui ON aasbs.accrual_income_dist USING btree (created_by_user_id);
CREATE INDEX ix_acrl_incd_ubui ON aasbs.accrual_income_dist USING btree (updated_by_user_id);
CREATE INDEX ix_aid_accrual_transmission_status_cd ON aasbs.accrual_income_dist USING btree (accrual_transmission_status_cd);

CREATE TRIGGER accrual_income_dist_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.accrual_income_dist FOR EACH ROW EXECUTE FUNCTION aasbs.trg_accrual_income_dist_hst();

COMMENT ON COLUMN "aasbs"."accrual_income_dist"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_INCOME_DIST_SEQ';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."accrual_income_id" IS 'Foreign Key to table AASBS.ACCRUAL_INCOME';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."tracking_num" IS 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via its Tracking_Num';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."distribute_amt" IS 'Amount from the LOA to distribute/allocate to this Income Accrual amount.';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."include_in_transmission_yn" IS 'Specifies if record will be included in transmission';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."loa_obligated_amt" IS 'Loa obligated amount';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."loa_billed_amt" IS 'Loa billed amount';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."loa_unbilled_amt" IS 'Loa unbilled amount';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."burn_order" IS 'Order of depletion';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."proration_pct" IS 'The percent of the total unbilled amount of the line item this LOA presents';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."proration_dist_amt" IS 'LOA Unbilled per Line Item / Total LOA Unbilled Amount for a given Line Item * Line Item Income Accrual Amount';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."accrual_income_dist_comment" IS 'Accrual income distribution comments';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."accrual_transmission_status_cd" IS 'Accrual Transmission Status Code';
COMMENT ON COLUMN "aasbs"."accrual_income_dist"."subject_to_availability_yn" IS 'Flag indicating whether funds were Subject to Availability for the LOA (=Y) or not (=N)';

COMMENT ON TABLE "aasbs"."accrual_income_dist" IS 'Monthly Accrual Income calculations for deliverable and surcharge line items';

-- ------------------------------------------------------------
-- Table: aasbs.accrual_income_dist_summary
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."accrual_income_dist_summary" (
    "id" bigint NOT NULL,
    "accrual_expense_id" bigint NOT NULL,
    "deliverable_accrual_income_dist_id" bigint,
    "tracking_num" character varying(20) NOT NULL,
    "distribute_amt" numeric(15,2) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "include_in_transmission_yn" character(1) NOT NULL,
    "surcharge_accrual_income_dist_id" bigint,
    "accrual_income_dist_summary_comment" character varying(250),
    "accrual_transmission_status_cd" character varying(40),
    CONSTRAINT "pk_accrual_income_dist_sum" PRIMARY KEY (id),
    CONSTRAINT "fk_accrual_income_dist_summary_atsc" FOREIGN KEY (accrual_transmission_status_cd) REFERENCES aasbs.lu_accrual_transmission_status(cd),
    CONSTRAINT "fk_acrl_incds_ae_id" FOREIGN KEY (accrual_expense_id) REFERENCES aasbs.accrual_expense(id),
    CONSTRAINT "fk_acrl_incds_aid_id_d" FOREIGN KEY (deliverable_accrual_income_dist_id) REFERENCES aasbs.accrual_income_dist(id),
    CONSTRAINT "fk_acrl_incds_aid_id_s" FOREIGN KEY (surcharge_accrual_income_dist_id) REFERENCES aasbs.accrual_income_dist(id),
    CONSTRAINT "fk_acrl_incds_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acrl_incds_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_acrl_incds_aid_id_d ON aasbs.accrual_income_dist_summary USING btree (deliverable_accrual_income_dist_id);
CREATE INDEX ix_acrl_incds_aid_id_s ON aasbs.accrual_income_dist_summary USING btree (surcharge_accrual_income_dist_id);
CREATE INDEX ix_acrl_incds_aiid ON aasbs.accrual_income_dist_summary USING btree (accrual_expense_id);
CREATE INDEX ix_acrl_incds_cbui ON aasbs.accrual_income_dist_summary USING btree (created_by_user_id);
CREATE INDEX ix_acrl_incds_ubui ON aasbs.accrual_income_dist_summary USING btree (updated_by_user_id);
CREATE INDEX ix_aids_accrual_transmission_status_cd ON aasbs.accrual_income_dist_summary USING btree (accrual_transmission_status_cd);

CREATE TRIGGER accrual_income_dist_summary_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.accrual_income_dist_summary FOR EACH ROW EXECUTE FUNCTION aasbs.trg_accrual_income_dist_summary_hst();

COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACCRUAL_INCOME_DIST_SUM_SEQ';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."accrual_expense_id" IS 'Foreign Key to table AASBS.ACCRUAL_EXPENSE';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."deliverable_accrual_income_dist_id" IS 'Foreign Key to table AASBS.ACCRUAL_INCOME_DIST for deliverable line item';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."tracking_num" IS 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via its Tracking_Num';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."distribute_amt" IS 'Amount from the LOA to distribute/allocate to this Income Accrual amount which is a sum of distributed amounts for deliverable and surcharge line items.';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."include_in_transmission_yn" IS 'Specifies if record will be included in transmission';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."surcharge_accrual_income_dist_id" IS 'Foreign Key to table aasbs.ACCRUAL_INCOME_DIST for surcharge line item';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."accrual_income_dist_summary_comment" IS 'Accrual income distribution summary comments';
COMMENT ON COLUMN "aasbs"."accrual_income_dist_summary"."accrual_transmission_status_cd" IS 'Accrual Transmission Status Code';

COMMENT ON TABLE "aasbs"."accrual_income_dist_summary" IS 'Monthly Accrual Income calculations for deliverable and surcharge line items';

-- ------------------------------------------------------------
-- Table: aasbs.acquisition
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."acquisition" (
    "id" bigint NOT NULL,
    "piid" character varying(50) NOT NULL,
    "piid_ext" character varying(6) NOT NULL,
    "procurement_id" bigint,
    "acquisition_status_cd" character varying(15) NOT NULL,
    "activity_address_cd" character varying(6) NOT NULL,
    "region_cd" character varying(2) NOT NULL,
    "title" character varying(250) NOT NULL,
    "description" character varying(4000) NOT NULL,
    "program_cd" character varying(10) NOT NULL,
    "agreement_type_cd" character varying(20),
    "fund_category_cd" character varying(20),
    "fund_cd" character varying(10),
    "client_id" character varying(16),
    "acquisition_type_cd" character varying(30),
    "performance_type_cd" character varying(30),
    "severability_type_cd" character varying(30),
    "includes_options_cnt" integer,
    "requirements_received_dt" timestamp(6) without time zone,
    "requirements_finalized_dt" timestamp(6) without time zone,
    "requirements_description" character varying(4000),
    "parent_acquisition_id" bigint,
    "continuity_type_cd" character varying(30),
    "transitional_bridge_yn" character(1),
    "opportunity_info" character varying(100),
    "preceding_reference_info" character varying(100),
    "incumbent_info" character varying(100),
    "attach_hash_id" character varying(64),
    "draft_frm_mgmt_yn" character(1),
    "draft_frm_reqs_yn" character(1),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "multi_tenant_yn" character(1),
    "acquisition_psc_cd" character varying(6),
    "apex_emp_org_id" bigint,
    CONSTRAINT "pk_acquisition" PRIMARY KEY (id),
    CONSTRAINT "ck_acq_multitenant_yn" CHECK (multi_tenant_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_acq_trans_brdge_yn" CHECK (transitional_bridge_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_draft_acq_mgmt_yn" CHECK (draft_frm_mgmt_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_draft_acq_reqs_yn" CHECK (draft_frm_reqs_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "nn_acq_draft_frm_mgmt_yn" CHECK (draft_frm_mgmt_yn IS NOT NULL),
    CONSTRAINT "nn_acq_draft_frm_reqs_yn" CHECK (draft_frm_reqs_yn IS NOT NULL),
    CONSTRAINT "fk_acq_aac" FOREIGN KEY (activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd),
    CONSTRAINT "fk_acq_acq_psc_cd" FOREIGN KEY (acquisition_psc_cd) REFERENCES aasbs.lus_psc(psc_code),
    CONSTRAINT "fk_acq_acq_stat" FOREIGN KEY (acquisition_status_cd) REFERENCES aasbs.lu_acquisition_status(cd),
    CONSTRAINT "fk_acq_acq_type" FOREIGN KEY (acquisition_type_cd) REFERENCES aasbs.lu_acquisition_type(cd),
    CONSTRAINT "fk_acq_agree_type" FOREIGN KEY (agreement_type_cd) REFERENCES aasbs.lu_agreement_type(cd),
    CONSTRAINT "fk_acq_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_client" FOREIGN KEY (client_id) REFERENCES table_master.clients(client_id),
    CONSTRAINT "fk_acq_continuity" FOREIGN KEY (continuity_type_cd) REFERENCES aasbs.lu_continuity_type(cd),
    CONSTRAINT "fk_acq_fund" FOREIGN KEY (fund_cd) REFERENCES aasbs.lu_fund(cd),
    CONSTRAINT "fk_acq_fund_cat" FOREIGN KEY (fund_category_cd) REFERENCES aasbs.lu_fund_category(cd),
    CONSTRAINT "fk_acq_parent_acq" FOREIGN KEY (parent_acquisition_id) REFERENCES aasbs.acquisition(id),
    CONSTRAINT "fk_acq_perf_type" FOREIGN KEY (performance_type_cd) REFERENCES aasbs.lu_performance_type(cd),
    CONSTRAINT "fk_acq_procurement" FOREIGN KEY (procurement_id) REFERENCES assist.procurement(procurement_id),
    CONSTRAINT "fk_acq_program" FOREIGN KEY (program_cd) REFERENCES aasbs.lu_program(cd),
    CONSTRAINT "fk_acq_region" FOREIGN KEY (region_cd) REFERENCES aasbs.lu_region(cd),
    CONSTRAINT "fk_acq_sever_type" FOREIGN KEY (severability_type_cd) REFERENCES aasbs.lu_severability_type(cd),
    CONSTRAINT "fk_acq_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acquisition_eoi" FOREIGN KEY (apex_emp_org_id) REFERENCES assist.lu_emp_orgs(emp_org_id)
);

CREATE INDEX ix_a_apex_emp_org_id ON aasbs.acquisition USING btree (apex_emp_org_id);
CREATE INDEX ix_acq_aac ON aasbs.acquisition USING btree (activity_address_cd);
CREATE INDEX ix_acq_acq_psc_cd ON aasbs.acquisition USING btree (acquisition_psc_cd);
CREATE INDEX ix_acq_acq_status ON aasbs.acquisition USING btree (acquisition_status_cd);
CREATE INDEX ix_acq_acq_type ON aasbs.acquisition USING btree (acquisition_type_cd);
CREATE INDEX ix_acq_agree_type ON aasbs.acquisition USING btree (agreement_type_cd);
CREATE INDEX ix_acq_cbui ON aasbs.acquisition USING btree (created_by_user_id);
CREATE INDEX ix_acq_client ON aasbs.acquisition USING btree (client_id);
CREATE INDEX ix_acq_continuity_type ON aasbs.acquisition USING btree (continuity_type_cd);
CREATE INDEX ix_acq_fund ON aasbs.acquisition USING btree (fund_cd);
CREATE INDEX ix_acq_fund_cat ON aasbs.acquisition USING btree (fund_category_cd);
CREATE INDEX ix_acq_parent_acq ON aasbs.acquisition USING btree (parent_acquisition_id);
CREATE INDEX ix_acq_perf_type ON aasbs.acquisition USING btree (performance_type_cd);
CREATE INDEX ix_acq_piid_pext ON aasbs.acquisition USING btree (piid, piid_ext);
CREATE INDEX ix_acq_proc_id ON aasbs.acquisition USING btree (procurement_id);
CREATE INDEX ix_acq_program ON aasbs.acquisition USING btree (program_cd);
CREATE INDEX ix_acq_region ON aasbs.acquisition USING btree (region_cd);
CREATE INDEX ix_acq_sever_type ON aasbs.acquisition USING btree (severability_type_cd);
CREATE INDEX ix_acq_ubui ON aasbs.acquisition USING btree (updated_by_user_id);

CREATE TRIGGER acquisition_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.acquisition FOR EACH ROW EXECUTE FUNCTION aasbs.trg_acquisition_hst();
CREATE TRIGGER trg_cclb_acquisition BEFORE DELETE ON aasbs.acquisition FOR EACH ROW EXECUTE FUNCTION aasbs."trg_cclb_acquisition$acquisition"();

COMMENT ON COLUMN "aasbs"."acquisition"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_SEQ';
COMMENT ON COLUMN "aasbs"."acquisition"."piid" IS 'Procurement Instrument Identifier';
COMMENT ON COLUMN "aasbs"."acquisition"."piid_ext" IS 'Acquisition PIID Extension';
COMMENT ON COLUMN "aasbs"."acquisition"."procurement_id" IS 'Foreign Key to table ASSIST.PROCUREMENT';
COMMENT ON COLUMN "aasbs"."acquisition"."acquisition_status_cd" IS 'Foreign Key to table AASBS.LU_ACQUISITION_STATUS';
COMMENT ON COLUMN "aasbs"."acquisition"."activity_address_cd" IS 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS';
COMMENT ON COLUMN "aasbs"."acquisition"."region_cd" IS 'Foreign Key to table AASBS.LU_REGION';
COMMENT ON COLUMN "aasbs"."acquisition"."title" IS 'Acquisition Title';
COMMENT ON COLUMN "aasbs"."acquisition"."description" IS 'Description of the Acquisition';
COMMENT ON COLUMN "aasbs"."acquisition"."program_cd" IS 'Foreign Key to table AASBS.LU_PROGRAM';
COMMENT ON COLUMN "aasbs"."acquisition"."agreement_type_cd" IS 'Foreign Key to table AASBS.LU_AGREEMENT_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition"."fund_category_cd" IS 'Foreign Key to table AASBS.LU_FUND_CATEGORY';
COMMENT ON COLUMN "aasbs"."acquisition"."fund_cd" IS 'Foreign Key to table AASBS.LU_FUND';
COMMENT ON COLUMN "aasbs"."acquisition"."client_id" IS 'Foreign Key to table TABLE_MASTER.CLIENTS';
COMMENT ON COLUMN "aasbs"."acquisition"."acquisition_type_cd" IS 'Foreign Key to table AASBS.LU_ACQUISITION_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition"."performance_type_cd" IS 'Foreign Key to table AASBS.LU_PERFORMANCE_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition"."severability_type_cd" IS 'Foreign Key to table AASBS.LU_SEVERABILITY_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition"."includes_options_cnt" IS 'Number of option periods included (when INCLUDES_OPTIONS_YN = Y)';
COMMENT ON COLUMN "aasbs"."acquisition"."requirements_received_dt" IS 'Date requirements were received';
COMMENT ON COLUMN "aasbs"."acquisition"."requirements_finalized_dt" IS 'Date requirements were finalized';
COMMENT ON COLUMN "aasbs"."acquisition"."requirements_description" IS 'Free text description of the Acquisition Requirements';
COMMENT ON COLUMN "aasbs"."acquisition"."parent_acquisition_id" IS 'Foreign Key to table AASBS.ACQUISITION.  When this column has a value, it indicates that this Acquisition falls under a BPA and the Foreign Key to the "parent" BPA Acquisition is captured in this column';
COMMENT ON COLUMN "aasbs"."acquisition"."continuity_type_cd" IS 'Foreign Key to table AASBS.LU_CONTINUITY_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition"."transitional_bridge_yn" IS 'Flag indicating whether a Acqusition is a Transitional Bridge Acquisition (=Y)  or not (=N)';
COMMENT ON COLUMN "aasbs"."acquisition"."opportunity_info" IS 'Free text field to hold Opportunity ID(s)';
COMMENT ON COLUMN "aasbs"."acquisition"."preceding_reference_info" IS 'Free text field to hold Preceeding Reference ID(s)';
COMMENT ON COLUMN "aasbs"."acquisition"."incumbent_info" IS 'Free text field to hold Incumbent Name';
COMMENT ON COLUMN "aasbs"."acquisition"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."acquisition"."draft_frm_mgmt_yn" IS 'Flag indicating whether Acquisition Management Form is (=Y) or is not (=N) in DRAFT state';
COMMENT ON COLUMN "aasbs"."acquisition"."draft_frm_reqs_yn" IS 'Flag indicating whether Acquisition Requirements Form is (=Y) or is not (=N) in DRAFT state';
COMMENT ON COLUMN "aasbs"."acquisition"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."acquisition"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."acquisition"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."acquisition"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."acquisition"."multi_tenant_yn" IS 'Flag indicating whether an Acquisition is Multitenant (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."acquisition"."acquisition_psc_cd" IS 'Foreign Key to table AASBS.LUS_PSC';
COMMENT ON COLUMN "aasbs"."acquisition"."apex_emp_org_id" IS 'References ASSIST.LU_EMP_ORGS.EMP_ORG_ID';

COMMENT ON TABLE "aasbs"."acquisition" IS 'CENTRAL TABLE - this table is the foundation for Assist and parent table to the SOLICITATION table';

-- ------------------------------------------------------------
-- Table: aasbs.acquisition_closeout
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."acquisition_closeout" (
    "id" bigint NOT NULL,
    "acquisition_id" bigint NOT NULL,
    "prohibition_cd" character varying(30),
    "attach_hash_id" character varying(64),
    "co_checked_certified_dt" timestamp(6) without time zone,
    "fma_certified_user_id" character varying(16),
    "fma_certified_dt" timestamp(6) without time zone,
    "last_call_or_order_num" character varying(20),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "lead_closeout_rep_user_id" character varying(16),
    "ytd_total_vendor_invoiced_amt" numeric(15,2) NOT NULL,
    "ytd_total_customer_billed_amt" numeric(15,2) NOT NULL,
    "contract_funds_reconciled_yn" character(1),
    "award_id" bigint,
    "draft_frm_closeout_yn" character(1),
    CONSTRAINT "pk_acquisition_closeout" PRIMARY KEY (id),
    CONSTRAINT "ck_acq_clsout_funds_reconc_yn" CHECK (contract_funds_reconciled_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "nn_acq_clsout_funds_reconc_yn" CHECK (contract_funds_reconciled_yn IS NOT NULL),
    CONSTRAINT "fk_acq_clsout_acq_id" FOREIGN KEY (acquisition_id) REFERENCES aasbs.acquisition(id),
    CONSTRAINT "fk_acq_clsout_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_clsout_fma_uid" FOREIGN KEY (fma_certified_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_clsout_lead_rep_user_id" FOREIGN KEY (lead_closeout_rep_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_clsout_prohib" FOREIGN KEY (prohibition_cd) REFERENCES aasbs.lu_closeout_prohibition(cd),
    CONSTRAINT "fk_acq_clsout_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_acq_clsout_acq_id ON aasbs.acquisition_closeout USING btree (acquisition_id);
CREATE INDEX ix_acq_clsout_attach_hash_id ON aasbs.acquisition_closeout USING btree (attach_hash_id);
CREATE INDEX ix_acq_clsout_cbui ON aasbs.acquisition_closeout USING btree (created_by_user_id);
CREATE INDEX ix_acq_clsout_co_chk_cert_dt ON aasbs.acquisition_closeout USING btree (co_checked_certified_dt);
CREATE INDEX ix_acq_clsout_fma_uid ON aasbs.acquisition_closeout USING btree (fma_certified_user_id);
CREATE INDEX ix_acq_clsout_lead_rep_user_id ON aasbs.acquisition_closeout USING btree (lead_closeout_rep_user_id);
CREATE INDEX ix_acq_clsout_prohib ON aasbs.acquisition_closeout USING btree (prohibition_cd);
CREATE INDEX ix_acq_clsout_ubui ON aasbs.acquisition_closeout USING btree (updated_by_user_id);

CREATE TRIGGER acquisition_closeout_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.acquisition_closeout FOR EACH ROW EXECUTE FUNCTION aasbs.trg_acquisition_closeout_hst();
CREATE TRIGGER trg_ccom_acquisition_closeout BEFORE DELETE ON aasbs.acquisition_closeout FOR EACH ROW EXECUTE FUNCTION aasbs."trg_ccom_acquisition_closeout$acquisition_closeout"();

COMMENT ON COLUMN "aasbs"."acquisition_closeout"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_CLOSEOUT_SEQ';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."acquisition_id" IS 'Foreign Key to table AASBS.ACQUISITION';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."prohibition_cd" IS 'Foreign Key to table AASBS.LU_CLOSEOUT_PROHIBITION';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."co_checked_certified_dt" IS 'Date and time that the Contracting Officer Certification checkbox was checked';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."fma_certified_user_id" IS 'Financial Management Analyst that certified the Closeout';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."fma_certified_dt" IS 'Date and time that the Financial Management Analyst certified the Closeout';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."last_call_or_order_num" IS 'The Award PIID of the final Call or Order issued - for IDVs only - Not Applicable otherwise';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."lead_closeout_rep_user_id" IS 'Foreign Key to table ASSIST.USERS';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."ytd_total_vendor_invoiced_amt" IS 'Year to Date Total Vendor Invoiced Amount';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."ytd_total_customer_billed_amt" IS 'Year to Date Total Customer Billed Amount';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."contract_funds_reconciled_yn" IS 'Flag indicating whether Contract Funds have (=Y) or have not (=N) been reconciled';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."award_id" IS 'Foreign Key to table AASBS.AWARD';
COMMENT ON COLUMN "aasbs"."acquisition_closeout"."draft_frm_closeout_yn" IS 'Flag indicating whether the Closeout Form is (=Y) or is not (=N) in DRAFT state';

COMMENT ON TABLE "aasbs"."acquisition_closeout" IS 'Central Acquisition Closeout Table - One record per Acquisition Closeout';

-- ------------------------------------------------------------
-- Table: aasbs.acquisition_closeout_checklist
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."acquisition_closeout_checklist" (
    "id" bigint NOT NULL,
    "acquisition_closeout_id" bigint NOT NULL,
    "checklist_item_cd" character varying(100),
    "checklist_value_cd" character varying(30),
    "checklist_item_dt" timestamp(6) without time zone,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_acq_closeout_checklist" PRIMARY KEY (id),
    CONSTRAINT "fk_acq_clsoutchklst_acqc_id" FOREIGN KEY (acquisition_closeout_id) REFERENCES aasbs.acquisition_closeout(id),
    CONSTRAINT "fk_acq_clsoutchklst_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_clsoutchklst_item" FOREIGN KEY (checklist_item_cd) REFERENCES aasbs.lu_checklist_item(cd),
    CONSTRAINT "fk_acq_clsoutchklst_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_clsoutchklst_value" FOREIGN KEY (checklist_value_cd) REFERENCES aasbs.lu_checklist_value(cd)
);

CREATE INDEX ix_acq_clsoutchklst_acqc_id ON aasbs.acquisition_closeout_checklist USING btree (acquisition_closeout_id);
CREATE INDEX ix_acq_clsoutchklst_cbui ON aasbs.acquisition_closeout_checklist USING btree (created_by_user_id);
CREATE INDEX ix_acq_clsoutchklst_item ON aasbs.acquisition_closeout_checklist USING btree (checklist_item_cd);
CREATE INDEX ix_acq_clsoutchklst_ubui ON aasbs.acquisition_closeout_checklist USING btree (updated_by_user_id);
CREATE INDEX ix_acq_clsoutchklst_value ON aasbs.acquisition_closeout_checklist USING btree (checklist_value_cd);

CREATE TRIGGER acquisition_closeout_checklist_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.acquisition_closeout_checklist FOR EACH ROW EXECUTE FUNCTION aasbs.trg_acquisition_closeout_checklist_hst();

COMMENT ON COLUMN "aasbs"."acquisition_closeout_checklist"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACQ_CLOSEOUT_CHECKLIST_SEQ';
COMMENT ON COLUMN "aasbs"."acquisition_closeout_checklist"."acquisition_closeout_id" IS 'Foreign Key to table AASBS.ACQUISITION_CLOSEOUT';
COMMENT ON COLUMN "aasbs"."acquisition_closeout_checklist"."checklist_item_cd" IS 'Foreign Key to table AASBS.LU_CHECKLIST_ITEM';
COMMENT ON COLUMN "aasbs"."acquisition_closeout_checklist"."checklist_value_cd" IS 'Foreign Key to table AASBS.LU_CHECKLIST_VALUE';
COMMENT ON COLUMN "aasbs"."acquisition_closeout_checklist"."checklist_item_dt" IS 'Checklist Item Date either chosen or automatically set';
COMMENT ON COLUMN "aasbs"."acquisition_closeout_checklist"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."acquisition_closeout_checklist"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."acquisition_closeout_checklist"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."acquisition_closeout_checklist"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."acquisition_closeout_checklist" IS 'Join table - links between Acquisition Closeout, Checklist Items, and their related values';

-- ------------------------------------------------------------
-- Table: aasbs.acquisition_mod
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."acquisition_mod" (
    "id" bigint NOT NULL,
    "acquisition_id" bigint NOT NULL,
    "mod_num" character varying(4) NOT NULL,
    "acquisition_mod_status_cd" character varying(30) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_acquisition_mod" PRIMARY KEY (id),
    CONSTRAINT "fk_acq_mod_acq" FOREIGN KEY (acquisition_id) REFERENCES aasbs.acquisition(id),
    CONSTRAINT "fk_acq_mod_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_mod_mod_status" FOREIGN KEY (acquisition_mod_status_cd) REFERENCES aasbs.lu_acquisition_mod_status(cd),
    CONSTRAINT "fk_acq_mod_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX idx_acq_mod_modnum ON aasbs.acquisition_mod USING btree (mod_num);
CREATE INDEX ix_acq_mod_ai ON aasbs.acquisition_mod USING btree (acquisition_id);
CREATE INDEX ix_acq_mod_cbui ON aasbs.acquisition_mod USING btree (created_by_user_id);
CREATE INDEX ix_acq_mod_mod_status ON aasbs.acquisition_mod USING btree (acquisition_mod_status_cd);
CREATE INDEX ix_acq_mod_ubui ON aasbs.acquisition_mod USING btree (updated_by_user_id);

CREATE TRIGGER acquisition_mod_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.acquisition_mod FOR EACH ROW EXECUTE FUNCTION aasbs.trg_acquisition_mod_hst();

COMMENT ON COLUMN "aasbs"."acquisition_mod"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_MOD_SEQ';
COMMENT ON COLUMN "aasbs"."acquisition_mod"."acquisition_id" IS 'Foreign Key to table AASBS.ACQUISITION';
COMMENT ON COLUMN "aasbs"."acquisition_mod"."mod_num" IS 'This column represented Award Mods during the initial rollout of ASSIST2.0.  That functionality will now be in AWARD_MOD.MOD_NUM';
COMMENT ON COLUMN "aasbs"."acquisition_mod"."acquisition_mod_status_cd" IS 'Foreign Key to table AASBS.LU_ACQUISITION_MOD_STATUS';
COMMENT ON COLUMN "aasbs"."acquisition_mod"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."acquisition_mod"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."acquisition_mod"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."acquisition_mod"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."acquisition_mod" IS 'Acquisition Mod';

-- ------------------------------------------------------------
-- Table: aasbs.acquisition_mod_address
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."acquisition_mod_address" (
    "id" bigint NOT NULL,
    "acquisition_mod_id" bigint NOT NULL,
    "address_id" bigint NOT NULL,
    "address_type_cd" character varying(16) NOT NULL,
    "multi_address_large_data_id" bigint,
    "same_as_client_address_yn" character(1),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_acquisition_mod_address" PRIMARY KEY (id),
    CONSTRAINT "ck_acq_mod_addr_same_client_yn" CHECK (same_as_client_address_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_acq_mod_addr_acq" FOREIGN KEY (acquisition_mod_id) REFERENCES aasbs.acquisition_mod(id),
    CONSTRAINT "fk_acq_mod_addr_addr" FOREIGN KEY (address_id) REFERENCES aasbs.address(id) NOT VALID,
    CONSTRAINT "fk_acq_mod_addr_addr_type" FOREIGN KEY (address_type_cd) REFERENCES aasbs.lu_address_type(cd),
    CONSTRAINT "fk_acq_mod_addr_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_mod_addr_large_data" FOREIGN KEY (multi_address_large_data_id) REFERENCES aasbs.large_data(id),
    CONSTRAINT "fk_acq_mod_addr_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX idx_ama_acquisition_mod_id ON aasbs.acquisition_mod_address USING btree (acquisition_mod_id);
CREATE INDEX ix_acq_mod_addr_addr ON aasbs.acquisition_mod_address USING btree (address_id);
CREATE INDEX ix_acq_mod_addr_addr_type ON aasbs.acquisition_mod_address USING btree (address_type_cd);
CREATE INDEX ix_acq_mod_addr_cbui ON aasbs.acquisition_mod_address USING btree (created_by_user_id);
CREATE INDEX ix_acq_mod_addr_large_data ON aasbs.acquisition_mod_address USING btree (multi_address_large_data_id);
CREATE INDEX ix_acq_mod_addr_ubui ON aasbs.acquisition_mod_address USING btree (updated_by_user_id);
CREATE UNIQUE INDEX ux_acq_mod_addr_acq_addr ON aasbs.acquisition_mod_address USING btree (acquisition_mod_id, address_id);
CREATE UNIQUE INDEX ux_acq_mod_addr_acq_addr_type ON aasbs.acquisition_mod_address USING btree (acquisition_mod_id, address_type_cd);

CREATE TRIGGER acquisition_mod_address_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.acquisition_mod_address FOR EACH ROW EXECUTE FUNCTION aasbs.trg_acquisition_mod_address_hst();

COMMENT ON COLUMN "aasbs"."acquisition_mod_address"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_MOD_ADDRESS_SEQ';
COMMENT ON COLUMN "aasbs"."acquisition_mod_address"."acquisition_mod_id" IS 'Foreign Key to table AASBS.ACQUISITION_MOD';
COMMENT ON COLUMN "aasbs"."acquisition_mod_address"."address_id" IS 'Foreign Key to table AASBS.ADDRESS';
COMMENT ON COLUMN "aasbs"."acquisition_mod_address"."address_type_cd" IS 'Foreign Key to table AASBS.LU_ADDRESS_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition_mod_address"."multi_address_large_data_id" IS 'Foreign Key to table AASBS.MULTI_ADDRESS_LARGE_DATA';
COMMENT ON COLUMN "aasbs"."acquisition_mod_address"."same_as_client_address_yn" IS 'Flag indicating whether the address is the same as the client address (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."acquisition_mod_address"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."acquisition_mod_address"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."acquisition_mod_address"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."acquisition_mod_address"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."acquisition_mod_address" IS 'Join table - links between Acquisition and associated addresses of various types (shipping, accepting, etc)';

-- ------------------------------------------------------------
-- Table: aasbs.acquisition_mod_ia
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."acquisition_mod_ia" (
    "id" bigint NOT NULL,
    "acquisition_mod_id" bigint NOT NULL,
    "ia_id" bigint,
    "legacy_ia_num" character varying(60),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "created_from_ia_yn" character(1) NOT NULL,
    CONSTRAINT "pk_acquisition_mod_ia" PRIMARY KEY (id),
    CONSTRAINT "ck_acq_mod_ia_created_from_ia_yn" CHECK (created_from_ia_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "nn_acq_mod_ia_id_or_lgcy_ianum" CHECK (ia_id IS NOT NULL AND legacy_ia_num IS NULL OR ia_id IS NULL AND legacy_ia_num IS NOT NULL),
    CONSTRAINT "fk_acq_mod_ia_acq_mod" FOREIGN KEY (acquisition_mod_id) REFERENCES aasbs.acquisition_mod(id),
    CONSTRAINT "fk_acq_mod_ia_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_mod_ia_ia" FOREIGN KEY (ia_id) REFERENCES aasbs.ia(id),
    CONSTRAINT "fk_acq_mod_ia_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_acq_mod_ia_acq_mod_id ON aasbs.acquisition_mod_ia USING btree (acquisition_mod_id);
CREATE INDEX ix_acq_mod_ia_cbui ON aasbs.acquisition_mod_ia USING btree (created_by_user_id);
CREATE INDEX ix_acq_mod_ia_ia_id ON aasbs.acquisition_mod_ia USING btree (ia_id);
CREATE INDEX ix_acq_mod_ia_ubui ON aasbs.acquisition_mod_ia USING btree (updated_by_user_id);
CREATE UNIQUE INDEX uk_acq_mod_ia_amodid_iaid_inum ON aasbs.acquisition_mod_ia USING btree (acquisition_mod_id, ia_id, legacy_ia_num);

CREATE TRIGGER acquisition_mod_ia_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.acquisition_mod_ia FOR EACH ROW EXECUTE FUNCTION aasbs.trg_acquisition_mod_ia_hst();

COMMENT ON COLUMN "aasbs"."acquisition_mod_ia"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_MOD_IA_SEQ';
COMMENT ON COLUMN "aasbs"."acquisition_mod_ia"."acquisition_mod_id" IS 'Foreign Key to table AASBS.ACQUISITION_MOD';
COMMENT ON COLUMN "aasbs"."acquisition_mod_ia"."ia_id" IS 'Foreign Key to table AASBS.IA';
COMMENT ON COLUMN "aasbs"."acquisition_mod_ia"."legacy_ia_num" IS 'Original IA Number used in Legacy ASSIST';
COMMENT ON COLUMN "aasbs"."acquisition_mod_ia"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."acquisition_mod_ia"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."acquisition_mod_ia"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."acquisition_mod_ia"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."acquisition_mod_ia"."created_from_ia_yn" IS 'Flag indicating whether the acquisition was created from an IA or from Acquisition Search page.';

COMMENT ON TABLE "aasbs"."acquisition_mod_ia" IS 'Join table associating Acquisition Mods with Interagency Agreements';

-- ------------------------------------------------------------
-- Table: aasbs.acquisition_mod_responsible
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."acquisition_mod_responsible" (
    "id" bigint NOT NULL,
    "acquisition_mod_id" bigint NOT NULL,
    "user_id" character varying(16),
    "responsible_role_cd" character varying(30),
    "is_primary_yn" character(1),
    "group_contact_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_acq_mod_responsible" PRIMARY KEY (id),
    CONSTRAINT "ck_acq_mod_resp_is_primeyn" CHECK (is_primary_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_acq_mod_resp_uid_or_gid" CHECK (user_id IS NOT NULL AND responsible_role_cd IS NOT NULL AND is_primary_yn IS NOT NULL AND group_contact_id IS NULL OR user_id IS NULL AND responsible_role_cd IS NULL AND is_primary_yn IS NULL AND group_contact_id IS NOT NULL),
    CONSTRAINT "fk_acq_mod_resp_acq" FOREIGN KEY (acquisition_mod_id) REFERENCES aasbs.acquisition_mod(id),
    CONSTRAINT "fk_acq_mod_resp_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_mod_resp_emp" FOREIGN KEY (user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_mod_resp_grp_contact" FOREIGN KEY (group_contact_id) REFERENCES aasbs.group_contact(id),
    CONSTRAINT "fk_acq_mod_resp_resp_role" FOREIGN KEY (responsible_role_cd) REFERENCES aasbs.lu_responsible_role(cd),
    CONSTRAINT "fk_acq_mod_resp_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_acq_mod_resp_acq ON aasbs.acquisition_mod_responsible USING btree (acquisition_mod_id);
CREATE INDEX ix_acq_mod_resp_cbui ON aasbs.acquisition_mod_responsible USING btree (created_by_user_id);
CREATE INDEX ix_acq_mod_resp_grp_contact ON aasbs.acquisition_mod_responsible USING btree (group_contact_id);
CREATE INDEX ix_acq_mod_resp_resp_role ON aasbs.acquisition_mod_responsible USING btree (responsible_role_cd);
CREATE INDEX ix_acq_mod_resp_ubui ON aasbs.acquisition_mod_responsible USING btree (updated_by_user_id);
CREATE INDEX ix_acq_mod_resp_ui ON aasbs.acquisition_mod_responsible USING btree (user_id);

CREATE TRIGGER acquisition_mod_responsible_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.acquisition_mod_responsible FOR EACH ROW EXECUTE FUNCTION aasbs.trg_acquisition_mod_responsible_hst();

COMMENT ON COLUMN "aasbs"."acquisition_mod_responsible"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_MOD_RESPONSIBLE_SEQ';
COMMENT ON COLUMN "aasbs"."acquisition_mod_responsible"."acquisition_mod_id" IS 'Foreign Key to table AASBS.ACQUISITION';
COMMENT ON COLUMN "aasbs"."acquisition_mod_responsible"."user_id" IS 'Foreign Key to table AASBS.USER';
COMMENT ON COLUMN "aasbs"."acquisition_mod_responsible"."responsible_role_cd" IS 'Foreign Key to table AASBS.LU_ROLE';
COMMENT ON COLUMN "aasbs"."acquisition_mod_responsible"."is_primary_yn" IS 'Flag indicating whether a person associated with an Acqusition is the primary for their role (=Y)  or not (=N)';
COMMENT ON COLUMN "aasbs"."acquisition_mod_responsible"."group_contact_id" IS 'Foreign Key to table AASBS.GROUP_CONTACT';
COMMENT ON COLUMN "aasbs"."acquisition_mod_responsible"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."acquisition_mod_responsible"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."acquisition_mod_responsible"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."acquisition_mod_responsible"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."acquisition_mod_responsible" IS 'Contains list of personnel associated with an Acquisition';

-- ------------------------------------------------------------
-- Table: aasbs.acquisition_plan
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."acquisition_plan" (
    "id" bigint NOT NULL,
    "acquisition_id" bigint NOT NULL,
    "instrument_type_cd" character varying(3),
    "vehicle_type_cd" character varying(30),
    "contract_type_cd" character varying(30),
    "acquisition_start_dt" timestamp(6) without time zone,
    "expedited_type_cd" character varying(30),
    "planned_fy_of_award" integer,
    "expedited_reqd_award_dt" timestamp(6) without time zone,
    "expedited_reason" character varying(200),
    "expedited_comments" character varying(4000),
    "negotiated_award_dt" timestamp(6) without time zone,
    "planned_palt_days_cnt" bigint,
    "funding_received_dt" timestamp(6) without time zone,
    "funding_end_dt" timestamp(6) without time zone,
    "comments" character varying(4000),
    "idv_referenced_class_cd" character varying(20),
    "idv_type_of_idc_cd" character varying(12),
    "idv_who_can_use_cd" character varying(20),
    "idv_fips95_other_stmt" character varying(200),
    "idv_funds_obligated_yn" character(1),
    "idv_family_name" character varying(200),
    "competition_type_cd" character varying(30),
    "processing_speed_cd" character varying(12),
    "attach_hash_id" character varying(64),
    "national_emergency_yn" character(1),
    "independent_gov_estimate_amt" numeric(15,2) NOT NULL,
    "commercial_type_cd" character varying(30),
    "draft_frm_plan_yn" character(1) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "ap_bundled_contract_cd" character varying(20),
    "ap_consolidated_contract_cd" character varying(30),
    "ap_emergency_acq_cd" character varying(30),
    "ap_sm_business_admin_cd" character varying(20),
    "ap_transfer_cd" character varying(20),
    "ap_perf_based_svc_contract_cd" character varying(20),
    "ap_transitional_bridge_yn" character(1),
    "ap_commercial_solution_open_yn" character(1),
    "ap_cor_delegation_cd" character varying(20),
    "ap_intel_community_cd" character varying(20),
    "ap_supply_chain_risk_cd" character varying(20),
    "ap_surveillance_spec_cd" character varying(20),
    "ap_oconus_support_yn" character(1),
    "ap_tech_dir_of_letters_yn" character(1),
    "ap_other_flags" character varying(100),
    "idv_referenced_family_id" integer,
    "ap_dpas_rated_yn" character(1),
    "combatant_command_cd" character varying(20),
    "isr_cd" character varying(20),
    "sbir_sttr_p_i_sol_topic_code" character varying(100),
    "sbir_sttr_p_i_sol_topic_name" character varying(100),
    "sbir_sttr_p_i_funding_agr_num" character varying(100),
    "sbir_sttr_p_i_sol_topic_agency_branch" character varying(100),
    "sbir_sttr_p_ii_sol_topic_code" character varying(100),
    "sbir_sttr_p_ii_sol_topic_name" character varying(100),
    "sbir_sttr_p_ii_funding_agr_num" character varying(100),
    "sbir_sttr_p_ii_sol_topic_agency_branch" character varying(100),
    "sbir_sttr_p_iii_fund_agr_award_office" character varying(100),
    "sbir_sttr_p_iii_fund_agr_number" character varying(100),
    "service_delivery_model_cd" character varying(35),
    "no_cost_contract_yn" character(1),
    "commerciality_cd" character varying(35),
    CONSTRAINT "pk_acquisition_plan" PRIMARY KEY (id),
    CONSTRAINT "ck_acq_plan_cso_yn" CHECK (ap_commercial_solution_open_yn = 'Y'::bpchar OR ap_commercial_solution_open_yn = 'N'::bpchar),
    CONSTRAINT "ck_acq_plan_emergyn" CHECK (national_emergency_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_acq_plan_funds_obligyn" CHECK (idv_funds_obligated_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_acq_plan_oconus_support_yn" CHECK (ap_oconus_support_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_acq_plan_tech_dir_yn" CHECK (ap_tech_dir_of_letters_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_acq_plan_trans_bridge_yn" CHECK (ap_transitional_bridge_yn = 'Y'::bpchar OR ap_transitional_bridge_yn = 'N'::bpchar),
    CONSTRAINT "ck_draft_acq_plan_plan_yn" CHECK (draft_frm_plan_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_acq_emergency_acq" FOREIGN KEY (ap_emergency_acq_cd) REFERENCES aasbs.lu_emergency_acq(cd),
    CONSTRAINT "fk_acq_plan_acq" FOREIGN KEY (acquisition_id) REFERENCES aasbs.acquisition(id),
    CONSTRAINT "fk_acq_plan_bundled_contract" FOREIGN KEY (ap_bundled_contract_cd) REFERENCES aasbs.lu_bundled_contract(cd),
    CONSTRAINT "fk_acq_plan_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_plan_cmrcial_typ" FOREIGN KEY (commercial_type_cd) REFERENCES aasbs.lu_commercial_type(cd),
    CONSTRAINT "fk_acq_plan_combatant_command" FOREIGN KEY (combatant_command_cd) REFERENCES aasbs.lu_combatant_command(cd),
    CONSTRAINT "fk_acq_plan_commerciality" FOREIGN KEY (commerciality_cd) REFERENCES aasbs.lu_commerciality(cd),
    CONSTRAINT "fk_acq_plan_competition_type" FOREIGN KEY (competition_type_cd) REFERENCES aasbs.lu_competition_type(cd),
    CONSTRAINT "fk_acq_plan_consolidated_contract" FOREIGN KEY (ap_consolidated_contract_cd) REFERENCES aasbs.lu_consolidated_contract(cd),
    CONSTRAINT "fk_acq_plan_contract_families" FOREIGN KEY (idv_referenced_family_id) REFERENCES table_master.contract_families(contract_family_id),
    CONSTRAINT "fk_acq_plan_contract_type" FOREIGN KEY (contract_type_cd) REFERENCES aasbs.lu_contract_type(cd),
    CONSTRAINT "fk_acq_plan_cor_delegate" FOREIGN KEY (ap_cor_delegation_cd) REFERENCES aasbs.lu_cor_delegation(cd),
    CONSTRAINT "fk_acq_plan_exped_type" FOREIGN KEY (expedited_type_cd) REFERENCES aasbs.lu_expedited_type(cd),
    CONSTRAINT "fk_acq_plan_idvrefcls" FOREIGN KEY (idv_referenced_class_cd) REFERENCES aasbs.lu_idv_referenced_class(cd),
    CONSTRAINT "fk_acq_plan_instr_type" FOREIGN KEY (instrument_type_cd) REFERENCES aasbs.lu_instrument_type(cd),
    CONSTRAINT "fk_acq_plan_intl_commnity" FOREIGN KEY (ap_intel_community_cd) REFERENCES aasbs.lu_intel_community(cd),
    CONSTRAINT "fk_acq_plan_isr" FOREIGN KEY (isr_cd) REFERENCES aasbs.lu_isr(cd),
    CONSTRAINT "fk_acq_plan_perf_based_svc_contract_cd" FOREIGN KEY (ap_perf_based_svc_contract_cd) REFERENCES aasbs.lu_performance_type(cd),
    CONSTRAINT "fk_acq_plan_sdm" FOREIGN KEY (service_delivery_model_cd) REFERENCES aasbs.lu_service_delivery_model(cd),
    CONSTRAINT "fk_acq_plan_sm_business_admin" FOREIGN KEY (ap_sm_business_admin_cd) REFERENCES aasbs.lu_small_business_admin(cd),
    CONSTRAINT "fk_acq_plan_supply_chain_risk" FOREIGN KEY (ap_supply_chain_risk_cd) REFERENCES aasbs.lu_supply_chain_risk(cd),
    CONSTRAINT "fk_acq_plan_surveil_spec" FOREIGN KEY (ap_surveillance_spec_cd) REFERENCES aasbs.lu_surveillance_spec(cd),
    CONSTRAINT "fk_acq_plan_transfer" FOREIGN KEY (ap_transfer_cd) REFERENCES aasbs.lu_transfer(cd),
    CONSTRAINT "fk_acq_plan_type_of_idc" FOREIGN KEY (idv_type_of_idc_cd) REFERENCES aasbs.lu_idv_type_of_idc(cd),
    CONSTRAINT "fk_acq_plan_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acq_plan_vehicle_type" FOREIGN KEY (vehicle_type_cd) REFERENCES aasbs.lu_vehicle_type(cd),
    CONSTRAINT "fk_acq_plan_who_can_use" FOREIGN KEY (idv_who_can_use_cd) REFERENCES aasbs.lu_idv_who_can_use(cd),
    CONSTRAINT "fk_acq_processing_speed" FOREIGN KEY (processing_speed_cd) REFERENCES aasbs.lu_processing_speed(cd)
);

CREATE INDEX ix_acq_combatant_command ON aasbs.acquisition_plan USING btree (combatant_command_cd);
CREATE INDEX ix_acq_emergency_acq ON aasbs.acquisition_plan USING btree (ap_emergency_acq_cd);
CREATE INDEX ix_acq_isr ON aasbs.acquisition_plan USING btree (isr_cd);
CREATE INDEX ix_acq_plan_ai ON aasbs.acquisition_plan USING btree (acquisition_id);
CREATE INDEX ix_acq_plan_attach ON aasbs.acquisition_plan USING btree (attach_hash_id);
CREATE INDEX ix_acq_plan_bundled_contract ON aasbs.acquisition_plan USING btree (ap_bundled_contract_cd);
CREATE INDEX ix_acq_plan_cbui ON aasbs.acquisition_plan USING btree (created_by_user_id);
CREATE INDEX ix_acq_plan_cmrcial_typ ON aasbs.acquisition_plan USING btree (commercial_type_cd);
CREATE INDEX ix_acq_plan_competition_type ON aasbs.acquisition_plan USING btree (competition_type_cd);
CREATE INDEX ix_acq_plan_contract_type ON aasbs.acquisition_plan USING btree (contract_type_cd);
CREATE INDEX ix_acq_plan_cor_delegate ON aasbs.acquisition_plan USING btree (ap_cor_delegation_cd);
CREATE INDEX ix_acq_plan_exped_type ON aasbs.acquisition_plan USING btree (expedited_type_cd);
CREATE INDEX ix_acq_plan_idvrefcls ON aasbs.acquisition_plan USING btree (idv_referenced_class_cd);
CREATE INDEX ix_acq_plan_instr_type ON aasbs.acquisition_plan USING btree (instrument_type_cd);
CREATE INDEX ix_acq_plan_intl_commnity ON aasbs.acquisition_plan USING btree (ap_intel_community_cd);
CREATE INDEX ix_acq_plan_sm_business_admin ON aasbs.acquisition_plan USING btree (ap_sm_business_admin_cd);
CREATE INDEX ix_acq_plan_supply_chain_risk ON aasbs.acquisition_plan USING btree (ap_supply_chain_risk_cd);
CREATE INDEX ix_acq_plan_surveil_spec ON aasbs.acquisition_plan USING btree (ap_surveillance_spec_cd);
CREATE INDEX ix_acq_plan_transfer ON aasbs.acquisition_plan USING btree (ap_transfer_cd);
CREATE INDEX ix_acq_plan_type_of_idc ON aasbs.acquisition_plan USING btree (idv_type_of_idc_cd);
CREATE INDEX ix_acq_plan_ubui ON aasbs.acquisition_plan USING btree (updated_by_user_id);
CREATE INDEX ix_acq_plan_vehicle_type ON aasbs.acquisition_plan USING btree (vehicle_type_cd);
CREATE INDEX ix_acq_plan_who_can_use ON aasbs.acquisition_plan USING btree (idv_who_can_use_cd);
CREATE INDEX ix_acq_processing_speed ON aasbs.acquisition_plan USING btree (processing_speed_cd);
CREATE INDEX ix_ap_ap_consolidated_contract_cd ON aasbs.acquisition_plan USING btree (ap_consolidated_contract_cd);
CREATE INDEX ix_ap_ap_perf_based_svc_contract_cd ON aasbs.acquisition_plan USING btree (ap_perf_based_svc_contract_cd);
CREATE INDEX ix_ap_idv_referenced_family_id ON aasbs.acquisition_plan USING btree (idv_referenced_family_id);

CREATE TRIGGER acquisition_plan_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.acquisition_plan FOR EACH ROW EXECUTE FUNCTION aasbs.trg_acquisition_plan_hst();
CREATE TRIGGER trg_ccom_acquisition_plan BEFORE DELETE ON aasbs.acquisition_plan FOR EACH ROW EXECUTE FUNCTION aasbs."trg_ccom_acquisition_plan$acquisition_plan"();

COMMENT ON COLUMN "aasbs"."acquisition_plan"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ACQUISITION_PLAN_SEQ';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."acquisition_id" IS 'Foreign Key to table AASBS.ACQUISITION';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."instrument_type_cd" IS 'Foreign Key to table AASBS.LU_INSTRUMENT_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."vehicle_type_cd" IS 'Foreign Key to table AASBS.LU_VEHICLE_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."contract_type_cd" IS 'Foreign Key to table AASBS.LU_CONTRACT_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."acquisition_start_dt" IS 'Date of the Acquisition Start';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."expedited_type_cd" IS 'Foreign Key to table AASBS.LU_EXPEDITED_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."planned_fy_of_award" IS 'Planned fiscal year of the Award';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."expedited_reqd_award_dt" IS 'Date of Required Award Date (only applicable when Acquisition Plan is "Expedited"';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."expedited_reason" IS 'Free text containing the reason an Acquisition Plan is being expedited';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."expedited_comments" IS 'Free text Comments about the Expedited aspects of the Acquisition Plan';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."negotiated_award_dt" IS 'Negotiated Award Date';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."planned_palt_days_cnt" IS 'Planned PALT (this column is going to go away - dynamically calculated days between two other column dates)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."funding_received_dt" IS 'Date Funding was received';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."funding_end_dt" IS 'Date of end of Funding';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."comments" IS 'Free text Comments about the Acquisition Plan';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."idv_referenced_class_cd" IS 'Foreign Key to table AASBS.LU_IDV_REFERENCED_CLASS';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."idv_type_of_idc_cd" IS 'Foreign Key to table AASBS.LU_IDV_TYPE_OF_IDC';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."idv_who_can_use_cd" IS 'Foreign Key to table AASBS.LU_IDV_WHO_CAN_USE';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."idv_fips95_other_stmt" IS 'Explanatory Text related to FIPS Codes';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."idv_funds_obligated_yn" IS 'Flag indicating whether Funds are Obligated for this Acquisition Plan (=Y)  or not (=N)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."idv_family_name" IS 'IDV Family Name';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."competition_type_cd" IS 'Foreign Key to table AASBS.LU_COMPETITION_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."processing_speed_cd" IS 'Foreign Key to table AASBS.LU_PROCESSING_SPEED';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."national_emergency_yn" IS 'Flag indicating whether a Acqusition Plan is a national emergency (=Y)  or not (=N)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."independent_gov_estimate_amt" IS 'Independent Government Estimate (IGE) of Acquisition Cost';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."commercial_type_cd" IS 'Foreign Key to table AASBS.LU_COMMERCIAL_TYPE';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."draft_frm_plan_yn" IS 'Flag indicating whether Acquisition Plan Form is (=Y) or is not (=N) in DRAFT state';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_bundled_contract_cd" IS 'Identify whether this acquisition meets the definition of a bundled requirement, as defined in the FAR.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_consolidated_contract_cd" IS 'Identify whether this acquisition meets the definition of a consolidated requirement, as defined in the FAR.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_emergency_acq_cd" IS 'Identify whether this acquisition is being conducted under FAR Part 18 Emergency Acquisition procedures.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_sm_business_admin_cd" IS 'Identify whether this acquisition is being conducted using the Small Business Administration's (SBA's) Small Business Innovation Research (SBIR) / Small Business Technology Transfer (STTR) procedures.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_transfer_cd" IS 'Identify whether this action represent the transfer of contract administration responsibilities.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_perf_based_svc_contract_cd" IS 'Identify whether this acquisition meets the requirements for performance-based, as defined in the FAR.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_transitional_bridge_yn" IS 'Identify whether this is a new short-term acquisition on a sole source basis to avoid lapses in service caused by delays in awarding subsequent contracts.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_commercial_solution_open_yn" IS 'Identify whether this acquisition is being conducted using Commercial Solutions Opening (CSO) procedures, in accordance with GSAM Part 571.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_cor_delegation_cd" IS 'Identify the method of Contracting Officer''s Representative (COR) delegation being used for this acquisition.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_intel_community_cd" IS 'Identify whether this acquisition is being conducted on behalf of a member of the Intelligence Community, as identified by the Director of National Intelligence';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_supply_chain_risk_cd" IS 'Identify whether the acquisition is required at Cybersecurity Maturity Model Certifications (CMMC) Level 1, 2, 3, 4, or 5';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_surveillance_spec_cd" IS 'Identify whether this acquisition requires a Contractor Security Classification Specification (DD-254).';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_oconus_support_yn" IS 'Identify whether this acquisition involves the performance of work outside the Continental United States';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_tech_dir_of_letters_yn" IS 'Identify whether this acquisition will use technical directions (or similar method) to further clarify work requirements in the post-award administration.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_other_flags" IS 'This field may be used to identify additional user-specific flags, as needed.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."idv_referenced_family_id" IS 'Foreign key to table table_master.contract_families.';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."ap_dpas_rated_yn" IS 'Defense Priorities and Allocations Systems (DPAS), Flag indicating whether DPAS Rated should be Yes (=Y) or NO (=N)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."combatant_command_cd" IS 'identify whether this acquisition is being conducted on behalf of a u.s. department of defense combatant command';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."isr_cd" IS 'Identify whether this acquisition has an associated ISR code';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."sbir_sttr_p_i_sol_topic_code" IS 'Additional SBIR and STTR data: Phase I Solicitation Topic Code(s)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."sbir_sttr_p_i_sol_topic_name" IS 'Additional SBIR and STTR data: Phase I Solicitation Topic Name';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."sbir_sttr_p_i_funding_agr_num" IS 'Additional SBIR and STTR data: Phase I Funding Agreement Number(s)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."sbir_sttr_p_i_sol_topic_agency_branch" IS 'Additional SBIR and STTR data: Phase I Solicitation Topic Agency/Agencies and Branch(es)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."sbir_sttr_p_ii_sol_topic_code" IS 'Additional SBIR and STTR data: Phase II Solicitation Topic Code(s)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."sbir_sttr_p_ii_sol_topic_name" IS 'Additional SBIR and STTR data: Phase II Solicitation Topic Name';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."sbir_sttr_p_ii_funding_agr_num" IS 'Additional SBIR and STTR data: Phase II Funding Agreement Number(s)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."sbir_sttr_p_ii_sol_topic_agency_branch" IS 'Additional SBIR and STTR data: Phase II Solicitation Topic Agency/Agencies and Branch(es)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."sbir_sttr_p_iii_fund_agr_award_office" IS 'Additional SBIR and STTR data: Phase III Funding Agreement Awarding Office(s)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."sbir_sttr_p_iii_fund_agr_number" IS 'Additional SBIR and STTR data: Phase III Funding Agreement Number(s)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."service_delivery_model_cd" IS 'Foreign Key to table AASBS.LU_SERVICE_DELIVERY_MODEL';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."no_cost_contract_yn" IS 'Flag indicating whether the acquisition is associated to an award that has no associated costs. Should be Yes (=Y) or NO (=N)';
COMMENT ON COLUMN "aasbs"."acquisition_plan"."commerciality_cd" IS 'Foreign Key to table AASBS.LU_commerciality';

COMMENT ON TABLE "aasbs"."acquisition_plan" IS 'Contains Acquisition Plans related to a parent Acquisition';

-- ------------------------------------------------------------
-- Table: aasbs.address
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."address" (
    "id" bigint NOT NULL,
    "office_name" character varying(100),
    "attention" character varying(100),
    "email" character varying(100),
    "address1" character varying(100) NOT NULL,
    "address2" character varying(100),
    "city" character varying(100) NOT NULL,
    "state_cd" character varying(16) NOT NULL,
    "country_cd" character varying(16) NOT NULL,
    "zip" character varying(16) NOT NULL,
    "fax_num" character varying(24),
    "phone_num" character varying(50),
    "phone_ext" character varying(5),
    "address_info" character varying(2000),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_address" PRIMARY KEY (id),
    CONSTRAINT "fk_addr_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_addr_country" FOREIGN KEY (country_cd) REFERENCES aasbs.lu_country(cd),
    CONSTRAINT "fk_addr_state" FOREIGN KEY (state_cd) REFERENCES aasbs.lu_state(cd),
    CONSTRAINT "fk_addr_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_addr_cbui ON aasbs.address USING btree (created_by_user_id);
CREATE INDEX ix_addr_country ON aasbs.address USING btree (country_cd);
CREATE INDEX ix_addr_state ON aasbs.address USING btree (state_cd);
CREATE INDEX ix_addr_ubui ON aasbs.address USING btree (updated_by_user_id);

CREATE TRIGGER address_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.address FOR EACH ROW EXECUTE FUNCTION aasbs.trg_address_hst();

COMMENT ON COLUMN "aasbs"."address"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.ADDRESS_SEQ';
COMMENT ON COLUMN "aasbs"."address"."office_name" IS 'Office name associated with Address record';
COMMENT ON COLUMN "aasbs"."address"."attention" IS 'Person designated as the contact person with Address record';
COMMENT ON COLUMN "aasbs"."address"."email" IS 'Email associated with Address record';
COMMENT ON COLUMN "aasbs"."address"."address1" IS 'Address Line 1';
COMMENT ON COLUMN "aasbs"."address"."address2" IS 'Address Line 2';
COMMENT ON COLUMN "aasbs"."address"."city" IS 'Address City';
COMMENT ON COLUMN "aasbs"."address"."state_cd" IS 'Foreign Key to table AASBS.LU_STATE';
COMMENT ON COLUMN "aasbs"."address"."country_cd" IS 'Foreign Key to table AASBS.LU_COUNTRY';
COMMENT ON COLUMN "aasbs"."address"."zip" IS 'Zip Code';
COMMENT ON COLUMN "aasbs"."address"."fax_num" IS 'Address Fax Number';
COMMENT ON COLUMN "aasbs"."address"."phone_num" IS 'Address Phone Number';
COMMENT ON COLUMN "aasbs"."address"."phone_ext" IS 'Address Phone Extension';
COMMENT ON COLUMN "aasbs"."address"."address_info" IS 'Free text field to hold general information for each Address record';
COMMENT ON COLUMN "aasbs"."address"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."address"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."address"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."address"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."address" IS 'Contains all address information in Assist';

-- ------------------------------------------------------------
-- Table: aasbs.award
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award" (
    "id" bigint NOT NULL,
    "acquisition_id" bigint NOT NULL,
    "lead_svc_activity_address_cd" character varying(6),
    "latest_award_mod_id" bigint,
    "solicit_id" bigint NOT NULL,
    "solicit_response_id" bigint NOT NULL,
    "award_status_cd" character varying(50) NOT NULL,
    "award_piid" character varying(50) NOT NULL,
    "award_title" character varying(250) NOT NULL,
    "award_fy" integer NOT NULL,
    "award_fin" character varying(9),
    "multi_tenant_yn" character(1) DEFAULT 'N'::character(1) NOT NULL,
    "fpds_cancelled_yn" character(1) NOT NULL,
    "fpds_cancel_reason_cd" character varying(20),
    "project_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "far_attachments_yn" character(1),
    "cor_delegation_cd" character varying(20),
    "intel_community_cd" character varying(20),
    "supply_chain_risk_cd" character varying(20),
    "surveillance_spec_cd" character varying(20),
    "oconus_support_yn" character(1),
    "tech_dir_of_letters_yn" character(1),
    "other_flags" character varying(100),
    "bundled_contract_cd" character varying(20),
    "consolidated_contract_cd" character varying(30),
    "emergency_acq_cd" character varying(30),
    "sm_business_admin_cd" character varying(20),
    "transfer_cd" character varying(20),
    "perf_based_svc_contract_cd" character varying(20),
    "transitional_bridge_yn" character(1),
    "commercial_solution_opening_yn" character(1),
    "award_aac_visibility_cd" character varying(20),
    "cor_report_suppress_reason_cd" character varying(35),
    "show_award_cor_report_yn" character(1),
    "dpas_rated_yn" character(1),
    "apex_emp_org_id" bigint,
    "combatant_command_cd" character varying(20),
    "isr_cd" character varying(20),
    "sbir_sttr_p_i_sol_topic_code" character varying(100),
    "sbir_sttr_p_i_sol_topic_name" character varying(100),
    "sbir_sttr_p_i_funding_agr_num" character varying(100),
    "sbir_sttr_p_i_sol_topic_agency_branch" character varying(100),
    "sbir_sttr_p_ii_sol_topic_code" character varying(100),
    "sbir_sttr_p_ii_sol_topic_name" character varying(100),
    "sbir_sttr_p_ii_funding_agr_num" character varying(100),
    "sbir_sttr_p_ii_sol_topic_agency_branch" character varying(100),
    "sbir_sttr_p_iii_fund_agr_award_office" character varying(100),
    "sbir_sttr_p_iii_fund_agr_number" character varying(100),
    "service_delivery_model_cd" character varying(35),
    "commerciality_cd" character varying(35),
    CONSTRAINT "pk_award" PRIMARY KEY (id),
    CONSTRAINT "ck_award_fpds_can_yn" CHECK (fpds_cancelled_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_award_oconus_support_yn" CHECK (oconus_support_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_award_tech_dir_of_letters_yn" CHECK (tech_dir_of_letters_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_award_yn" CHECK (multi_tenant_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_awd_plan_cso_yn" CHECK (commercial_solution_opening_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_awd_plan_trans_bridge_yn" CHECK (transitional_bridge_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_show_award_cor_report_yn" CHECK (show_award_cor_report_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "nn_award_lead_aac" CHECK (lead_svc_activity_address_cd IS NOT NULL),
    CONSTRAINT "nn_awd_aac_visibility" CHECK (award_aac_visibility_cd IS NOT NULL),
    CONSTRAINT "fk_award_acq_id" FOREIGN KEY (acquisition_id) REFERENCES aasbs.acquisition(id),
    CONSTRAINT "fk_award_award_aac_vis" FOREIGN KEY (award_aac_visibility_cd) REFERENCES aasbs.lu_award_aac_visibility(cd),
    CONSTRAINT "fk_award_awd_fin_log" FOREIGN KEY (award_fin) REFERENCES aasbs.award_fin_log(fin),
    CONSTRAINT "fk_award_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_award_combatant_command" FOREIGN KEY (combatant_command_cd) REFERENCES aasbs.lu_combatant_command(cd),
    CONSTRAINT "fk_award_commerciality" FOREIGN KEY (commerciality_cd) REFERENCES aasbs.lu_commerciality(cd),
    CONSTRAINT "fk_award_cor_delegation" FOREIGN KEY (cor_delegation_cd) REFERENCES aasbs.lu_cor_delegation(cd),
    CONSTRAINT "fk_award_cor_report_suppress_reason" FOREIGN KEY (cor_report_suppress_reason_cd) REFERENCES aasbs.lu_award_cor_report_suppress_reason(cd),
    CONSTRAINT "fk_award_eoi" FOREIGN KEY (apex_emp_org_id) REFERENCES assist.lu_emp_orgs(emp_org_id),
    CONSTRAINT "fk_award_fpds_canc_reason" FOREIGN KEY (fpds_cancel_reason_cd) REFERENCES aasbs.lu_fpds_cancel_reason(cd),
    CONSTRAINT "fk_award_intl_commnity" FOREIGN KEY (intel_community_cd) REFERENCES aasbs.lu_intel_community(cd),
    CONSTRAINT "fk_award_isr" FOREIGN KEY (isr_cd) REFERENCES aasbs.lu_isr(cd),
    CONSTRAINT "fk_award_latest_awd_mod_id" FOREIGN KEY (latest_award_mod_id) REFERENCES aasbs.award_mod(id),
    CONSTRAINT "fk_award_lead_aac" FOREIGN KEY (lead_svc_activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd),
    CONSTRAINT "fk_award_proj_id" FOREIGN KEY (project_id) REFERENCES aasbs.project(id),
    CONSTRAINT "fk_award_sdm" FOREIGN KEY (service_delivery_model_cd) REFERENCES aasbs.lu_service_delivery_model(cd),
    CONSTRAINT "fk_award_solicit_id" FOREIGN KEY (solicit_id) REFERENCES aasbs.solicit(id),
    CONSTRAINT "fk_award_solresp_id" FOREIGN KEY (solicit_response_id) REFERENCES aasbs.solicit_response(id),
    CONSTRAINT "fk_award_status" FOREIGN KEY (award_status_cd) REFERENCES aasbs.lu_award_status(cd),
    CONSTRAINT "fk_award_supply_chain_risk" FOREIGN KEY (supply_chain_risk_cd) REFERENCES aasbs.lu_supply_chain_risk(cd),
    CONSTRAINT "fk_award_surveillance_spec" FOREIGN KEY (surveillance_spec_cd) REFERENCES aasbs.lu_surveillance_spec(cd),
    CONSTRAINT "fk_award_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awd_emergency_acq" FOREIGN KEY (emergency_acq_cd) REFERENCES aasbs.lu_emergency_acq(cd),
    CONSTRAINT "fk_awd_plan_bundled_contract" FOREIGN KEY (bundled_contract_cd) REFERENCES aasbs.lu_bundled_contract(cd),
    CONSTRAINT "fk_awd_plan_consolidated_contract" FOREIGN KEY (consolidated_contract_cd) REFERENCES aasbs.lu_consolidated_contract(cd),
    CONSTRAINT "fk_awd_plan_perf_based_svc_contract_cd" FOREIGN KEY (perf_based_svc_contract_cd) REFERENCES aasbs.lu_performance_type(cd),
    CONSTRAINT "fk_awd_plan_sm_business_admin" FOREIGN KEY (sm_business_admin_cd) REFERENCES aasbs.lu_small_business_admin(cd),
    CONSTRAINT "fk_awd_plan_transfer" FOREIGN KEY (transfer_cd) REFERENCES aasbs.lu_transfer(cd)
);

CREATE INDEX indx_award_piid ON aasbs.award USING btree (award_piid);
CREATE INDEX ix_award_acq_id ON aasbs.award USING btree (acquisition_id);
CREATE INDEX ix_award_cbui ON aasbs.award USING btree (created_by_user_id);
CREATE INDEX ix_award_cor_delegation ON aasbs.award USING btree (cor_delegation_cd);
CREATE INDEX ix_award_fpds_canc_reason ON aasbs.award USING btree (fpds_cancel_reason_cd);
CREATE INDEX ix_award_intl_commnity ON aasbs.award USING btree (intel_community_cd);
CREATE INDEX ix_award_latest_awd_mod_id ON aasbs.award USING btree (latest_award_mod_id);
CREATE INDEX ix_award_lead_aac ON aasbs.award USING btree (lead_svc_activity_address_cd);
CREATE INDEX ix_award_proj_id ON aasbs.award USING btree (project_id);
CREATE INDEX ix_award_solicit_id ON aasbs.award USING btree (solicit_id);
CREATE INDEX ix_award_solresp_id ON aasbs.award USING btree (solicit_response_id);
CREATE INDEX ix_award_status ON aasbs.award USING btree (award_status_cd);
CREATE INDEX ix_award_supply_chain_risk ON aasbs.award USING btree (supply_chain_risk_cd);
CREATE INDEX ix_award_surveillance_spec ON aasbs.award USING btree (surveillance_spec_cd);
CREATE INDEX ix_award_ubui ON aasbs.award USING btree (updated_by_user_id);
CREATE INDEX ix_awd_apex_emp_org_id ON aasbs.award USING btree (apex_emp_org_id);
CREATE INDEX ix_awd_award_aac_visibility_cd ON aasbs.award USING btree (award_aac_visibility_cd);
CREATE INDEX ix_awd_combatant_command ON aasbs.award USING btree (combatant_command_cd);
CREATE INDEX ix_awd_consolidated_contract_cd ON aasbs.award USING btree (consolidated_contract_cd);
CREATE INDEX ix_awd_cor_report_suppress_reason_cd ON aasbs.award USING btree (cor_report_suppress_reason_cd);
CREATE INDEX ix_awd_emergency_acq ON aasbs.award USING btree (emergency_acq_cd);
CREATE INDEX ix_awd_isr ON aasbs.award USING btree (isr_cd);
CREATE INDEX ix_awd_perf_based_svc_contract_cd ON aasbs.award USING btree (perf_based_svc_contract_cd);
CREATE INDEX ix_awd_plan_bundled_contract ON aasbs.award USING btree (bundled_contract_cd);
CREATE INDEX ix_awd_plan_sm_business_admin ON aasbs.award USING btree (sm_business_admin_cd);
CREATE INDEX ix_awd_plan_transfer ON aasbs.award USING btree (transfer_cd);
CREATE UNIQUE INDEX uk_award_awd_fin ON aasbs.award USING btree (award_fin);

CREATE TRIGGER award_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_hst();
CREATE TRIGGER trg_cclb_award BEFORE DELETE ON aasbs.award FOR EACH ROW EXECUTE FUNCTION aasbs."trg_cclb_award$award"();

COMMENT ON COLUMN "aasbs"."award"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_SEQ';
COMMENT ON COLUMN "aasbs"."award"."acquisition_id" IS 'Foreign Key to table AASBS.ACQUISITION';
COMMENT ON COLUMN "aasbs"."award"."lead_svc_activity_address_cd" IS 'Lead Servicing AAC - same as the ACTIVITY_ADDRESS_CD on the parent ACQUISITION record';
COMMENT ON COLUMN "aasbs"."award"."latest_award_mod_id" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs"."award"."solicit_id" IS 'Foreign Key to table AASBS.SOLICIT';
COMMENT ON COLUMN "aasbs"."award"."solicit_response_id" IS 'Foreign Key to table AASBS.SOLICIT_RESPONSE';
COMMENT ON COLUMN "aasbs"."award"."award_status_cd" IS 'Foreign Key to table AASBS.LU_AWARD_STATUS';
COMMENT ON COLUMN "aasbs"."award"."award_piid" IS 'Award Procurement Instrument Identifier (PIID)';
COMMENT ON COLUMN "aasbs"."award"."award_title" IS 'User-entered Award Title';
COMMENT ON COLUMN "aasbs"."award"."award_fy" IS 'Award Fiscal Year';
COMMENT ON COLUMN "aasbs"."award"."award_fin" IS 'Foreign Key to table AASBS.AWARD_FIN_LOG. Financial Identification Number - Award Level (AWARD_FIN = Award FIN) - see LINE_ITEM_ACCEPTED table for SC_FIN (Service Charge FIN)';
COMMENT ON COLUMN "aasbs"."award"."multi_tenant_yn" IS 'Indicates if the Award is for a Multi-Tenant Acquisition';
COMMENT ON COLUMN "aasbs"."award"."fpds_cancelled_yn" IS 'Flag indicating whether FPDS was cancelled (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."award"."fpds_cancel_reason_cd" IS 'Foreign Key to table AASBS.LU_FPDS_CANCEL_REASON';
COMMENT ON COLUMN "aasbs"."award"."project_id" IS 'Foreign Key to table AASBS.PROJECT';
COMMENT ON COLUMN "aasbs"."award"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."award"."far_attachments_yn" IS 'Flag indicating whether Contract/Purchase Order FAR Attachments exist (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."award"."cor_delegation_cd" IS 'Copied from Acquisition upon award creation, then editable. Identify the method of Contracting Officer''s Representative (COR) delegation being used for this acquisition.';
COMMENT ON COLUMN "aasbs"."award"."intel_community_cd" IS 'Copied from Acquisition upon award creation, then editable. Identify whether this acquisition is being conducted on behalf of a member of the Intelligence Community, as identified by the Director of National Intelligence';
COMMENT ON COLUMN "aasbs"."award"."supply_chain_risk_cd" IS 'Copied from Acquisition upon award creation, then editable. Identify whether the acquisition is required at Cybersecurity Maturity Model Certifications (CMMC) Level 1, 2, 3, 4, or 5';
COMMENT ON COLUMN "aasbs"."award"."surveillance_spec_cd" IS 'Copied from Acquisition upon award creation, then editable. Identify whether this acquisition requires a Contractor Security Classification Specification (DD-254).';
COMMENT ON COLUMN "aasbs"."award"."oconus_support_yn" IS 'Copied from Acquisition upon award creation, then editable. Identify whether this acquisition involves the performance of work outside the Continental United States';
COMMENT ON COLUMN "aasbs"."award"."tech_dir_of_letters_yn" IS 'Copied from Acquisition upon award creation, then editable. Identify whether this acquisition will use technical directions (or similar method) to further clarify work requirements in the post-award administration.';
COMMENT ON COLUMN "aasbs"."award"."other_flags" IS 'Copied from Acquisition upon award creation, then editable. This field may be used to identify additional user-specific flags, as needed.';
COMMENT ON COLUMN "aasbs"."award"."bundled_contract_cd" IS 'Identify whether this acquisition meets the definition of a bundled requirement, as defined in the FAR.';
COMMENT ON COLUMN "aasbs"."award"."consolidated_contract_cd" IS 'Identify whether this acquisition meets the definition of a consolidated requirement, as defined in the FAR.';
COMMENT ON COLUMN "aasbs"."award"."emergency_acq_cd" IS 'Identify whether this acquisition is being conducted under FAR Part 18 Emergency Acquisition procedures.';
COMMENT ON COLUMN "aasbs"."award"."sm_business_admin_cd" IS 'Identify whether this acquisition is being conducted using the Small Business Administration's (SBA's) Small Business Innovation Research (SBIR) / Small Business Technology Transfer (STTR) procedures.';
COMMENT ON COLUMN "aasbs"."award"."transfer_cd" IS 'Identify whether this action represent the transfer of contract administration responsibilities.';
COMMENT ON COLUMN "aasbs"."award"."perf_based_svc_contract_cd" IS 'Identify whether this acquisition meets the requirements for performance-based, as defined in the FAR.';
COMMENT ON COLUMN "aasbs"."award"."transitional_bridge_yn" IS 'Identify whether this is a new short-term acquisition on a sole source basis to avoid lapses in service caused by delays in awarding subsequent contracts.';
COMMENT ON COLUMN "aasbs"."award"."commercial_solution_opening_yn" IS 'Identify whether this acquisition is being conducted using Commercial Solutions Opening (CSO) procedures, in accordance with GSAM Part 571.';
COMMENT ON COLUMN "aasbs"."award"."cor_report_suppress_reason_cd" IS 'FOREIGN KEY TO TABLE AASBS.LU_AWARD_COR_REPORT_SUPPRESS_REASON';
COMMENT ON COLUMN "aasbs"."award"."show_award_cor_report_yn" IS 'Flag indicating whether cor report should be created and displayed (=Y) or suppressed (=N)';
COMMENT ON COLUMN "aasbs"."award"."dpas_rated_yn" IS 'Defense Priorities and Allocations Systems (DPAS), Flag indicating whether DPAS Rated should be Yes (=Y) or NO (=N)';
COMMENT ON COLUMN "aasbs"."award"."apex_emp_org_id" IS 'Apex office associated to this award';
COMMENT ON COLUMN "aasbs"."award"."combatant_command_cd" IS 'copied from acquisition upon award creation, then editable. identify whether this acquisition is being conducted on behalf of a u.s. department of defense combatant command';
COMMENT ON COLUMN "aasbs"."award"."isr_cd" IS 'copied from acquisition upon award creation, then editable. identify whether this acquisition has an associated isr code';
COMMENT ON COLUMN "aasbs"."award"."sbir_sttr_p_i_sol_topic_code" IS 'Additional SBIR and STTR data: Phase I Solicitation Topic Code(s)';
COMMENT ON COLUMN "aasbs"."award"."sbir_sttr_p_i_sol_topic_name" IS 'Additional SBIR and STTR data: Phase I Solicitation Topic Name';
COMMENT ON COLUMN "aasbs"."award"."sbir_sttr_p_i_funding_agr_num" IS 'Additional SBIR and STTR data: Phase I Funding Agreement Number(s)';
COMMENT ON COLUMN "aasbs"."award"."sbir_sttr_p_i_sol_topic_agency_branch" IS 'Additional SBIR and STTR data: Phase I Solicitation Topic Agency/Agencies and Branch(es)';
COMMENT ON COLUMN "aasbs"."award"."sbir_sttr_p_ii_sol_topic_code" IS 'Additional SBIR and STTR data: Phase II Solicitation Topic Code(s)';
COMMENT ON COLUMN "aasbs"."award"."sbir_sttr_p_ii_sol_topic_name" IS 'Additional SBIR and STTR data: Phase II Solicitation Topic Name';
COMMENT ON COLUMN "aasbs"."award"."sbir_sttr_p_ii_funding_agr_num" IS 'Additional SBIR and STTR data: Phase II Funding Agreement Number(s)';
COMMENT ON COLUMN "aasbs"."award"."sbir_sttr_p_ii_sol_topic_agency_branch" IS 'Additional SBIR and STTR data: Phase II Solicitation Topic Agency/Agencies and Branch(es)';
COMMENT ON COLUMN "aasbs"."award"."sbir_sttr_p_iii_fund_agr_award_office" IS 'Additional SBIR and STTR data: Phase III Funding Agreement Awarding Office(s)';
COMMENT ON COLUMN "aasbs"."award"."sbir_sttr_p_iii_fund_agr_number" IS 'Additional SBIR and STTR data: Phase III Funding Agreement Number(s)';
COMMENT ON COLUMN "aasbs"."award"."service_delivery_model_cd" IS 'Foreign Key to table AASBS.LU_SERVICE_DELIVERY_MODEL';
COMMENT ON COLUMN "aasbs"."award"."commerciality_cd" IS 'Foreign Key to table AASBS.LU_commerciality';

COMMENT ON TABLE "aasbs"."award" IS 'Contains high-level award details for all awards within the ASSIST application';

-- ------------------------------------------------------------
-- Table: aasbs.award_fin_log
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award_fin_log" (
    "id" bigint NOT NULL,
    "fin" character varying(9) NOT NULL,
    "fin_base_10" bigint NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_award_fin_log" PRIMARY KEY (id),
    CONSTRAINT "fk_award_fin_log_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_award_fin_log_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX idx_afl_trim_fin ON aasbs.award_fin_log USING btree (TRIM(BOTH FROM fin));
CREATE INDEX idx_award_fin_log_fb10 ON aasbs.award_fin_log USING btree (fin_base_10);
CREATE INDEX ix_award_fin_log_cbui ON aasbs.award_fin_log USING btree (created_by_user_id);
CREATE INDEX ix_award_fin_log_ubui ON aasbs.award_fin_log USING btree (updated_by_user_id);

CREATE TRIGGER award_fin_log_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award_fin_log FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_fin_log_hst();

COMMENT ON COLUMN "aasbs"."award_fin_log"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_FIN_LOG_SEQ';
COMMENT ON COLUMN "aasbs"."award_fin_log"."fin" IS 'Alphanumeric Financial Identification Number (FIN) - (up to) 8-digit base 36 value (optional A prefix). Unique system-wide.';
COMMENT ON COLUMN "aasbs"."award_fin_log"."fin_base_10" IS 'Base 10 value of the (up to) 8-character base 36 FIN (without the optional A prefix)';
COMMENT ON COLUMN "aasbs"."award_fin_log"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award_fin_log"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award_fin_log"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award_fin_log"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."award_fin_log" IS 'Award-level Financial Identification Number values, including all legacy ACT Numbers';

-- ------------------------------------------------------------
-- Table: aasbs.award_ia
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award_ia" (
    "id" bigint NOT NULL,
    "award_id" bigint NOT NULL,
    "ia_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_award_ia" PRIMARY KEY (id),
    CONSTRAINT "fk_awd_ia_addr" FOREIGN KEY (ia_id) REFERENCES aasbs.ia(id),
    CONSTRAINT "fk_awd_ia_award" FOREIGN KEY (award_id) REFERENCES aasbs.award(id),
    CONSTRAINT "fk_awd_ia_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awd_ia_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_awd_ia_awdmodid ON aasbs.award_ia USING btree (award_id);
CREATE INDEX ix_awd_ia_cbui ON aasbs.award_ia USING btree (created_by_user_id);
CREATE INDEX ix_awd_ia_iaid ON aasbs.award_ia USING btree (ia_id);
CREATE INDEX ix_awd_ia_ubui ON aasbs.award_ia USING btree (updated_by_user_id);
CREATE UNIQUE INDEX ux_awd_ia_awdmodid_iaid ON aasbs.award_ia USING btree (award_id, ia_id);

CREATE TRIGGER award_ia_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award_ia FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_ia_hst();

COMMENT ON COLUMN "aasbs"."award_ia"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_IA_SEQ';
COMMENT ON COLUMN "aasbs"."award_ia"."award_id" IS 'Foreign Key to table AASBS.AWARD';
COMMENT ON COLUMN "aasbs"."award_ia"."ia_id" IS 'Foreign Key to table AASBS.IA';
COMMENT ON COLUMN "aasbs"."award_ia"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award_ia"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award_ia"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award_ia"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."award_ia" IS 'Join table associating Awards with Interagency Agreements';

-- ------------------------------------------------------------
-- Table: aasbs.award_mod
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award_mod" (
    "id" bigint NOT NULL,
    "award_id" bigint NOT NULL,
    "mod_num" character varying(5) NOT NULL,
    "award_piid_ext" character varying(6) NOT NULL,
    "award_mod_status_cd" character varying(50) NOT NULL,
    "fpds_car_status_cd" character varying(20),
    "mod_reviews_required_yn" character(1),
    "m1p_award_mod_fpds_reason_cd" character varying(50),
    "m1p_sf30_mod_type_cd" character varying(20),
    "m1p_expedited_type_cd" character varying(30),
    "m1p_processing_speed_cd" character varying(12),
    "m1p_mod_start_dt" timestamp(6) without time zone,
    "m1p_required_award_dt" timestamp(6) without time zone,
    "m1p_rationale" character varying(500),
    "m1p_award_mod_authority_text" character varying(500),
    "award_mod_title" character varying(250) NOT NULL,
    "award_form_cd" character varying(50) NOT NULL,
    "award_form_version" character varying(15),
    "budget_fy" integer,
    "at_award_cost_recovery_cd" character varying(35),
    "post_award_cost_recovery_cd" character varying(35),
    "contractor_signature_reqd_yn" character(1),
    "net_days_cnt" bigint,
    "discount_days_cnt" bigint,
    "discount_pct" numeric(6,3),
    "award_mod_description" character varying(2000) NOT NULL,
    "explanation_if_no_fee" character varying(400),
    "award_doc_text_large_data_id" bigint,
    "co_signature_user_id" character varying(16),
    "co_signature_dt" timestamp(6) without time zone,
    "contractor_signature_user_id" character varying(16),
    "contractor_signature_dt" timestamp(6) without time zone,
    "draft_mod_line_items_yn" character(1),
    "po_transmitted_yn" character(1),
    "li_migrated_yn" character(1),
    "attach_hash_id" character varying(64),
    "is_assist2_migration_yn" character(1),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "m1p_fpds_mod_num" character varying(6),
    "notes_to_contracting_large_data_id" bigint,
    "negotiated_award_dt" timestamp(6) without time zone,
    "apex_emp_org_id" bigint,
    "version" bigint,
    "esign_yn" character(1),
    "esign_document_id" bigint,
    CONSTRAINT "pk_award_mod" PRIMARY KEY (id),
    CONSTRAINT "ck_award_mod_a2migrated_yn" CHECK (is_assist2_migration_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_award_mod_contrctr_sign_yn" CHECK (contractor_signature_reqd_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_award_mod_li_migrated_yn" CHECK (li_migrated_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_award_mod_line_items_yn" CHECK (draft_mod_line_items_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_award_mod_po_trans_yn" CHECK (po_transmitted_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_award_mod_reviews_reqd_yn" CHECK (mod_reviews_required_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_esign_yn" CHECK (esign_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_award_mod_awd_costrecover" FOREIGN KEY (at_award_cost_recovery_cd) REFERENCES aasbs.lu_cost_recovery(cd),
    CONSTRAINT "fk_award_mod_awd_id" FOREIGN KEY (award_id) REFERENCES aasbs.award(id),
    CONSTRAINT "fk_award_mod_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_award_mod_co_sign_userid" FOREIGN KEY (co_signature_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_award_mod_contr_sign_userid" FOREIGN KEY (contractor_signature_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_award_mod_eoi" FOREIGN KEY (apex_emp_org_id) REFERENCES assist.lu_emp_orgs(emp_org_id),
    CONSTRAINT "fk_award_mod_exped_type" FOREIGN KEY (m1p_expedited_type_cd) REFERENCES aasbs.lu_expedited_type(cd),
    CONSTRAINT "fk_award_mod_fpds_car_status" FOREIGN KEY (fpds_car_status_cd) REFERENCES aasbs.lu_fpds_car_status(cd),
    CONSTRAINT "fk_award_mod_fpds_rsn" FOREIGN KEY (m1p_award_mod_fpds_reason_cd) REFERENCES aasbs.lu_award_mod_fpds_reason(cd),
    CONSTRAINT "fk_award_mod_large_data" FOREIGN KEY (award_doc_text_large_data_id) REFERENCES aasbs.large_data(id),
    CONSTRAINT "fk_award_mod_post_costrecover" FOREIGN KEY (post_award_cost_recovery_cd) REFERENCES aasbs.lu_cost_recovery(cd),
    CONSTRAINT "fk_award_mod_processing_speed" FOREIGN KEY (m1p_processing_speed_cd) REFERENCES aasbs.lu_processing_speed(cd),
    CONSTRAINT "fk_award_mod_sf30_mod_type" FOREIGN KEY (m1p_sf30_mod_type_cd) REFERENCES aasbs.lu_sf30_mod_type(cd),
    CONSTRAINT "fk_award_mod_status" FOREIGN KEY (award_mod_status_cd) REFERENCES aasbs.lu_award_mod_status(cd),
    CONSTRAINT "fk_award_mod_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_lu_award_form" FOREIGN KEY (award_form_cd) REFERENCES aasbs.lu_award_form(cd),
    CONSTRAINT "fk_notes_to_contracting_large_data" FOREIGN KEY (notes_to_contracting_large_data_id) REFERENCES aasbs.large_data(id)
);

CREATE INDEX idx_aasbs_award_mod_esign_document_id ON aasbs.award_mod USING btree (esign_document_id);
CREATE INDEX idx_award_mod_mod_num ON aasbs.award_mod USING btree (mod_num);
CREATE INDEX indx_award_mod_status_cd ON aasbs.award_mod USING btree (award_mod_status_cd);
CREATE INDEX ix_am_apex_emp_org_id ON aasbs.award_mod USING btree (apex_emp_org_id);
CREATE INDEX ix_am_notes_to_contracting_large_data_id ON aasbs.award_mod USING btree (notes_to_contracting_large_data_id);
CREATE INDEX ix_award_mod_awd_costrecover ON aasbs.award_mod USING btree (at_award_cost_recovery_cd);
CREATE INDEX ix_award_mod_awd_id ON aasbs.award_mod USING btree (award_id);
CREATE INDEX ix_award_mod_cbui ON aasbs.award_mod USING btree (created_by_user_id);
CREATE INDEX ix_award_mod_co_sign_userid ON aasbs.award_mod USING btree (co_signature_user_id);
CREATE INDEX ix_award_mod_contr_sign_userid ON aasbs.award_mod USING btree (contractor_signature_user_id);
CREATE INDEX ix_award_mod_exped_type ON aasbs.award_mod USING btree (m1p_expedited_type_cd);
CREATE INDEX ix_award_mod_fpds_car_status ON aasbs.award_mod USING btree (fpds_car_status_cd);
CREATE INDEX ix_award_mod_fpds_rsn ON aasbs.award_mod USING btree (m1p_award_mod_fpds_reason_cd);
CREATE INDEX ix_award_mod_large_data ON aasbs.award_mod USING btree (award_doc_text_large_data_id);
CREATE INDEX ix_award_mod_post_costrecover ON aasbs.award_mod USING btree (post_award_cost_recovery_cd);
CREATE INDEX ix_award_mod_processing_speed ON aasbs.award_mod USING btree (m1p_processing_speed_cd);
CREATE INDEX ix_award_mod_sf30_mod_type ON aasbs.award_mod USING btree (m1p_sf30_mod_type_cd);
CREATE INDEX ix_award_mod_status ON aasbs.award_mod USING btree (award_mod_status_cd);
CREATE INDEX ix_award_mod_ubui ON aasbs.award_mod USING btree (updated_by_user_id);
CREATE INDEX ix_lu_award_form ON aasbs.award_mod USING btree (award_form_cd);

CREATE TRIGGER award_mod_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award_mod FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_mod_hst();

COMMENT ON COLUMN "aasbs"."award_mod"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_MOD_SEQ';
COMMENT ON COLUMN "aasbs"."award_mod"."award_id" IS 'Foreign Key to table AASBS.AWARD';
COMMENT ON COLUMN "aasbs"."award_mod"."mod_num" IS 'Award Mod Number';
COMMENT ON COLUMN "aasbs"."award_mod"."award_piid_ext" IS 'Award Mod Number / Procurement Instrument Identifier Extention (PIID Extension)';
COMMENT ON COLUMN "aasbs"."award_mod"."award_mod_status_cd" IS 'Foreign Key to table AASBS.LU_AWARD_MOD_STATUS';
COMMENT ON COLUMN "aasbs"."award_mod"."fpds_car_status_cd" IS 'Foreign Key to table AASBS.LU_FPDS_CAR_STATUS';
COMMENT ON COLUMN "aasbs"."award_mod"."mod_reviews_required_yn" IS 'Flag indicating if additional Award Mod reviews are required (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."award_mod"."m1p_award_mod_fpds_reason_cd" IS 'Foreign Key to table AASBS.LU_AWARD_MOD_FPDS_REASON - not applicable to Mod 00000';
COMMENT ON COLUMN "aasbs"."award_mod"."m1p_sf30_mod_type_cd" IS 'Foreign Key to table AASBS.LU_SF30_MOD_TYPE - not applicable to Mod 00000';
COMMENT ON COLUMN "aasbs"."award_mod"."m1p_expedited_type_cd" IS 'Foreign Key to table AASBS.LU_EXPEDITED_TYPE - not applicable to Mod 00000';
COMMENT ON COLUMN "aasbs"."award_mod"."m1p_processing_speed_cd" IS 'Foreign Key to table AASBS.LU_PROCESSING_SPEED - not applicable to Mod 00000';
COMMENT ON COLUMN "aasbs"."award_mod"."m1p_mod_start_dt" IS 'Award Mod Start Date - not applicable to Mod 00000';
COMMENT ON COLUMN "aasbs"."award_mod"."m1p_required_award_dt" IS 'Award Mod Required Award Date - not applicable to Mod 00000';
COMMENT ON COLUMN "aasbs"."award_mod"."m1p_rationale" IS 'Award Mod Rationale - not applicable to Mod 00000';
COMMENT ON COLUMN "aasbs"."award_mod"."m1p_award_mod_authority_text" IS 'Award Mod Authority - not applicable to Mod 00000';
COMMENT ON COLUMN "aasbs"."award_mod"."award_mod_title" IS 'User-entered Award Mod Title';
COMMENT ON COLUMN "aasbs"."award_mod"."award_form_cd" IS 'Foreign Key to table AASBS.LU_AWARD_FORM';
COMMENT ON COLUMN "aasbs"."award_mod"."award_form_version" IS 'Version of the Award Form that was en vogue at the time of Award Signing';
COMMENT ON COLUMN "aasbs"."award_mod"."budget_fy" IS 'Award Mod Budget Fiscal Year';
COMMENT ON COLUMN "aasbs"."award_mod"."at_award_cost_recovery_cd" IS 'Foreign Key to table AASBS.LU_COST_RECOVERY';
COMMENT ON COLUMN "aasbs"."award_mod"."post_award_cost_recovery_cd" IS 'Foreign Key to table AASBS.LU_COST_RECOVERY';
COMMENT ON COLUMN "aasbs"."award_mod"."contractor_signature_reqd_yn" IS 'Flag indicating whether a Contractor Signature is required (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."award_mod"."net_days_cnt" IS 'Number of days defining the discount period';
COMMENT ON COLUMN "aasbs"."award_mod"."discount_days_cnt" IS 'Number of days to achieve the discount percentage (DISCOUNT_PCT column)';
COMMENT ON COLUMN "aasbs"."award_mod"."discount_pct" IS 'Discount percentage if paid within specified number of days (DISCOUNT_DAYS_CNT column)';
COMMENT ON COLUMN "aasbs"."award_mod"."award_mod_description" IS 'User-entered Award Mod Description';
COMMENT ON COLUMN "aasbs"."award_mod"."explanation_if_no_fee" IS 'If NO_FEE is selected, explanation is required';
COMMENT ON COLUMN "aasbs"."award_mod"."award_doc_text_large_data_id" IS 'Foreign Key to table AASBS.LARGE_DATA';
COMMENT ON COLUMN "aasbs"."award_mod"."co_signature_user_id" IS 'Foreign Key to table ASSIST.USERS';
COMMENT ON COLUMN "aasbs"."award_mod"."co_signature_dt" IS 'Date the Award Mod was signed by Contracting Officer';
COMMENT ON COLUMN "aasbs"."award_mod"."contractor_signature_user_id" IS 'Foreign Key to table ASSIST.USERS';
COMMENT ON COLUMN "aasbs"."award_mod"."contractor_signature_dt" IS 'Date the Award Mod was signed by Contractor';
COMMENT ON COLUMN "aasbs"."award_mod"."draft_mod_line_items_yn" IS 'Flag indicating if the Line Items under this Award Mod are currently in Draft state (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."award_mod"."po_transmitted_yn" IS 'Flag indicating whether the Award Mod has been packaged in a flat file and transmitted';
COMMENT ON COLUMN "aasbs"."award_mod"."li_migrated_yn" IS 'Flag indicating if a Line Item record was Migrated from Legacy Assist (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."award_mod"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."award_mod"."is_assist2_migration_yn" IS 'Flag indicating whether an Award Mod was (=Y) migrated or not (=N)';
COMMENT ON COLUMN "aasbs"."award_mod"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award_mod"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award_mod"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award_mod"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."award_mod"."m1p_fpds_mod_num" IS 'Mod Number value to be sent to FPDS-NG for pre-uPIID orders migrated from legacy.';
COMMENT ON COLUMN "aasbs"."award_mod"."notes_to_contracting_large_data_id" IS 'Foreign Key to table AASBS.LARGE_DATA';
COMMENT ON COLUMN "aasbs"."award_mod"."negotiated_award_dt" IS 'The Negotiated Award Dt is initially populated from the associated Acquisition Planning form';
COMMENT ON COLUMN "aasbs"."award_mod"."apex_emp_org_id" IS 'Apex office associated to this award mod';
COMMENT ON COLUMN "aasbs"."award_mod"."version" IS 'This is the version attribute which is used by the app to detect concurrent update actions.';
COMMENT ON COLUMN "aasbs"."award_mod"."esign_yn" IS 'Flag indicating whether the user has selected the eSign method (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."award_mod"."esign_document_id" IS 'ID of the related record in AASBS_ESIGN_DOCUMENT.DOCUMENT and created index idx_aasbs_award_mod_esign_document_id';

COMMENT ON TABLE "aasbs"."award_mod" IS 'Contains high-level Award Mod details for all Award Mod within the ASSIST application';

-- ------------------------------------------------------------
-- Table: aasbs.award_mod_address
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award_mod_address" (
    "id" bigint NOT NULL,
    "award_mod_id" bigint NOT NULL,
    "address_id" bigint NOT NULL,
    "address_type_cd" character varying(30) NOT NULL,
    "multi_address_large_data_id" bigint,
    "same_as_client_address_yn" character(1),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_award_mod_address" PRIMARY KEY (id),
    CONSTRAINT "ck_awd_mod_addr_same_client_yn" CHECK (same_as_client_address_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_awd_mod_addr_acq" FOREIGN KEY (award_mod_id) REFERENCES aasbs.award_mod(id),
    CONSTRAINT "fk_awd_mod_addr_addr" FOREIGN KEY (address_id) REFERENCES aasbs.address(id),
    CONSTRAINT "fk_awd_mod_addr_addr_type" FOREIGN KEY (address_type_cd) REFERENCES aasbs.lu_address_type(cd),
    CONSTRAINT "fk_awd_mod_addr_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awd_mod_addr_large_data" FOREIGN KEY (multi_address_large_data_id) REFERENCES aasbs.large_data(id),
    CONSTRAINT "fk_awd_mod_addr_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_awd_mod_addr_addr ON aasbs.award_mod_address USING btree (address_id);
CREATE INDEX ix_awd_mod_addr_addr_type ON aasbs.award_mod_address USING btree (address_type_cd);
CREATE INDEX ix_awd_mod_addr_cbui ON aasbs.award_mod_address USING btree (created_by_user_id);
CREATE INDEX ix_awd_mod_addr_large_data ON aasbs.award_mod_address USING btree (multi_address_large_data_id);
CREATE INDEX ix_awd_mod_addr_ubui ON aasbs.award_mod_address USING btree (updated_by_user_id);
CREATE UNIQUE INDEX ux_awd_mod_addr_acq_addr ON aasbs.award_mod_address USING btree (award_mod_id, address_id);
CREATE UNIQUE INDEX ux_awd_mod_addr_acq_addr_type ON aasbs.award_mod_address USING btree (award_mod_id, address_type_cd);

CREATE TRIGGER award_mod_address_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award_mod_address FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_mod_address_hst();

COMMENT ON COLUMN "aasbs"."award_mod_address"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_MOD_ADDRESS_SEQ';
COMMENT ON COLUMN "aasbs"."award_mod_address"."award_mod_id" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs"."award_mod_address"."address_id" IS 'Foreign Key to table AASBS.ADDRESS';
COMMENT ON COLUMN "aasbs"."award_mod_address"."address_type_cd" IS 'Foreign Key to table AASBS.LU_ADDRESS_TYPE';
COMMENT ON COLUMN "aasbs"."award_mod_address"."multi_address_large_data_id" IS 'Foreign Key to table AASBS.LARGE_DATA';
COMMENT ON COLUMN "aasbs"."award_mod_address"."same_as_client_address_yn" IS 'Flag indicating whether the address is the same as the client address (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."award_mod_address"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award_mod_address"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award_mod_address"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award_mod_address"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."award_mod_address" IS 'Join table associating Award Mods with zero or more Addresses';

-- ------------------------------------------------------------
-- Table: aasbs.award_mod_company
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award_mod_company" (
    "id" bigint NOT NULL,
    "award_mod_id" bigint NOT NULL,
    "solicit_company_id" bigint NOT NULL,
    "solicit_company_name" character varying(120) NOT NULL,
    "contract_id" character varying(16),
    "remittance_company_name" character varying(200) NOT NULL,
    "contractor_classification_cd" character varying(20) NOT NULL,
    "business_type_cd" character varying(50) NOT NULL,
    "ipartner_id" character varying(16),
    "contractor_tax_id" character varying(16),
    "fob_type_cd" character varying(20),
    "company_duns" character varying(13),
    "company_duns_plus4" character varying(4),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "company_uei" character varying(16),
    "company_uei_plus4" character varying(8),
    "company_vuei" character varying(16) GENERATED ALWAYS AS (
CASE
    WHEN (NULLIF((company_uei)::text, ''::text) IS NULL) THEN (NULLIF((company_duns)::text, ''::text))::character varying
    ELSE company_uei
END) STORED,
    "company_vuei_plus4" character varying(13) GENERATED ALWAYS AS (
CASE
    WHEN (NULLIF((company_uei_plus4)::text, ''::text) IS NULL) THEN (NULLIF((company_duns_plus4)::text, ''::text))::character varying
    ELSE company_uei_plus4
END) STORED,
    CONSTRAINT "pk_award_mod_company" PRIMARY KEY (id),
    CONSTRAINT "fk_awdmodcomp_awd_mod_id" FOREIGN KEY (award_mod_id) REFERENCES aasbs.award_mod(id),
    CONSTRAINT "fk_awdmodcomp_bus_type" FOREIGN KEY (business_type_cd) REFERENCES aasbs.lu_business_type(cd),
    CONSTRAINT "fk_awdmodcomp_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awdmodcomp_contract_id" FOREIGN KEY (contract_id) REFERENCES table_master.contracts(contract_id),
    CONSTRAINT "fk_awdmodcomp_contractor_class" FOREIGN KEY (contractor_classification_cd) REFERENCES aasbs.lu_contractor_classification(cd),
    CONSTRAINT "fk_awdmodcomp_fobytpe" FOREIGN KEY (fob_type_cd) REFERENCES aasbs.lu_fob_type(cd),
    CONSTRAINT "fk_awdmodcomp_ipartner_id" FOREIGN KEY (ipartner_id) REFERENCES table_master.industry_partners(ipartner_id),
    CONSTRAINT "fk_awdmodcomp_sol_comp_id" FOREIGN KEY (solicit_company_id) REFERENCES aasbs.solicit_company(id),
    CONSTRAINT "fk_awdmodcomp_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_awdmodcomp_bus_type ON aasbs.award_mod_company USING btree (business_type_cd);
CREATE INDEX ix_awdmodcomp_cbui ON aasbs.award_mod_company USING btree (created_by_user_id);
CREATE INDEX ix_awdmodcomp_contract_id ON aasbs.award_mod_company USING btree (contract_id);
CREATE INDEX ix_awdmodcomp_contractor_class ON aasbs.award_mod_company USING btree (contractor_classification_cd);
CREATE INDEX ix_awdmodcomp_contractor_tax ON aasbs.award_mod_company USING btree (contractor_tax_id);
CREATE INDEX ix_awdmodcomp_fobytpe ON aasbs.award_mod_company USING btree (fob_type_cd);
CREATE INDEX ix_awdmodcomp_ipartner_id ON aasbs.award_mod_company USING btree (ipartner_id);
CREATE INDEX ix_awdmodcomp_sol_comp_id ON aasbs.award_mod_company USING btree (solicit_company_id);
CREATE INDEX ix_awdmodcomp_ubui ON aasbs.award_mod_company USING btree (updated_by_user_id);
CREATE INDEX ux_awdmodcomp_awdmid_solcompid ON aasbs.award_mod_company USING btree (award_mod_id, solicit_company_id);

CREATE TRIGGER award_mod_company_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award_mod_company FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_mod_company_hst();

COMMENT ON COLUMN "aasbs"."award_mod_company"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_MOD_COMPANY_SEQ';
COMMENT ON COLUMN "aasbs"."award_mod_company"."award_mod_id" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs"."award_mod_company"."solicit_company_id" IS 'Foreign Key to table AASBS.SOLICIT_COMPANY';
COMMENT ON COLUMN "aasbs"."award_mod_company"."solicit_company_name" IS 'The name of the Awarded Company';
COMMENT ON COLUMN "aasbs"."award_mod_company"."contract_id" IS 'Foreign Key to table TABLE_MASTER.CONTRACTS';
COMMENT ON COLUMN "aasbs"."award_mod_company"."remittance_company_name" IS 'Name of the Remittance Company';
COMMENT ON COLUMN "aasbs"."award_mod_company"."contractor_classification_cd" IS 'Foreign Key to table AASBS.LU_CONTRACTOR_CLASSIFICATION';
COMMENT ON COLUMN "aasbs"."award_mod_company"."business_type_cd" IS 'Foreign Key to table AASBS.LU_BUSINESS_TYPE';
COMMENT ON COLUMN "aasbs"."award_mod_company"."ipartner_id" IS 'Foreign Key to table TABLE_MASTER.INDUSTRY_PARTNERS';
COMMENT ON COLUMN "aasbs"."award_mod_company"."contractor_tax_id" IS 'Contractor Tax ID';
COMMENT ON COLUMN "aasbs"."award_mod_company"."fob_type_cd" IS 'Foreign Key to table AASBS.LU_FOB_TYPE';
COMMENT ON COLUMN "aasbs"."award_mod_company"."company_duns" IS 'The Awarded Company''s DUNS Number';
COMMENT ON COLUMN "aasbs"."award_mod_company"."company_duns_plus4" IS 'DUNS number assigned to a Client';
COMMENT ON COLUMN "aasbs"."award_mod_company"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award_mod_company"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award_mod_company"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award_mod_company"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."award_mod_company"."company_uei" IS 'Unique Entity Identifier (UEI) - replaces DUNS';
COMMENT ON COLUMN "aasbs"."award_mod_company"."company_uei_plus4" IS 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4';
COMMENT ON COLUMN "aasbs"."award_mod_company"."company_vuei" IS 'Generated column:  show COMPANY_UEI if present, otherwise show COMPANY_DUNS';
COMMENT ON COLUMN "aasbs"."award_mod_company"."company_vuei_plus4" IS 'Generated column:  show COMPANY_UEI_PLUS4 if present, otherwise show COMPANY_DUNS_PLUS4';

COMMENT ON TABLE "aasbs"."award_mod_company" IS 'Contains Awarded Company and Remittance Company info';

-- ------------------------------------------------------------
-- Table: aasbs.award_mod_responsible
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award_mod_responsible" (
    "id" bigint NOT NULL,
    "award_mod_id" bigint NOT NULL,
    "user_id" character varying(16) NOT NULL,
    "responsible_role_cd" character varying(30) NOT NULL,
    "is_primary_yn" character(1) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_awd_mod_responsible" PRIMARY KEY (id),
    CONSTRAINT "ck_awd_mod_resp_is_primeyn" CHECK (is_primary_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_awd_mod_resp_acq" FOREIGN KEY (award_mod_id) REFERENCES aasbs.award_mod(id),
    CONSTRAINT "fk_awd_mod_resp_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awd_mod_resp_emp" FOREIGN KEY (user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awd_mod_resp_resp_role" FOREIGN KEY (responsible_role_cd) REFERENCES aasbs.lu_responsible_role(cd),
    CONSTRAINT "fk_awd_mod_resp_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_awd_mod_resp_awd_mod_id ON aasbs.award_mod_responsible USING btree (award_mod_id);
CREATE INDEX ix_awd_mod_resp_cbui ON aasbs.award_mod_responsible USING btree (created_by_user_id);
CREATE INDEX ix_awd_mod_resp_resp_role ON aasbs.award_mod_responsible USING btree (responsible_role_cd);
CREATE INDEX ix_awd_mod_resp_ubui ON aasbs.award_mod_responsible USING btree (updated_by_user_id);
CREATE INDEX ix_awd_mod_resp_ui ON aasbs.award_mod_responsible USING btree (user_id);

CREATE TRIGGER award_mod_responsible_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award_mod_responsible FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_mod_responsible_hst();

COMMENT ON COLUMN "aasbs"."award_mod_responsible"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_MOD_RESPONSIBLE_SEQ';
COMMENT ON COLUMN "aasbs"."award_mod_responsible"."award_mod_id" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs"."award_mod_responsible"."user_id" IS 'Foreign Key to table ASSIST.USERS - the responsible user';
COMMENT ON COLUMN "aasbs"."award_mod_responsible"."responsible_role_cd" IS 'Foreign Key to table AASBS.LU_RESPONSIBLE_ROLE';
COMMENT ON COLUMN "aasbs"."award_mod_responsible"."is_primary_yn" IS 'Flag indicating whether a person associated with an Award is the primary for their role (=Y)  or not (=N)';
COMMENT ON COLUMN "aasbs"."award_mod_responsible"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award_mod_responsible"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award_mod_responsible"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award_mod_responsible"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."award_mod_responsible" IS 'Join table associating Award Mods with zero or more Responsible People';

-- ------------------------------------------------------------
-- Table: aasbs.award_mod_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award_mod_type" (
    "id" bigint NOT NULL,
    "award_mod_id" bigint NOT NULL,
    "award_mod_type_cd" character varying(30) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_award_mod_type" PRIMARY KEY (id),
    CONSTRAINT "fk_awd_mod_typ_awd_mod" FOREIGN KEY (award_mod_type_cd) REFERENCES aasbs.lu_award_mod_type(cd),
    CONSTRAINT "fk_awd_mod_typ_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awd_mod_typ_typ_awd_mod" FOREIGN KEY (award_mod_id) REFERENCES aasbs.award_mod(id),
    CONSTRAINT "fk_awd_mod_typ_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_awd_mod_typ_awd_id ON aasbs.award_mod_type USING btree (award_mod_id);
CREATE INDEX ix_awd_mod_typ_cbui ON aasbs.award_mod_type USING btree (created_by_user_id);
CREATE INDEX ix_awd_mod_typ_resp_role ON aasbs.award_mod_type USING btree (award_mod_type_cd);
CREATE INDEX ix_awd_mod_typ_ubui ON aasbs.award_mod_type USING btree (updated_by_user_id);

CREATE TRIGGER award_mod_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award_mod_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_mod_type_hst();

COMMENT ON COLUMN "aasbs"."award_mod_type"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_MOD_TYPE_SEQ';
COMMENT ON COLUMN "aasbs"."award_mod_type"."award_mod_id" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs"."award_mod_type"."award_mod_type_cd" IS 'Foreign Key to table AASBS.LU_AWARD_MOD_TYPE';
COMMENT ON COLUMN "aasbs"."award_mod_type"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award_mod_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award_mod_type"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award_mod_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."award_mod_type" IS 'Associates an Award Mod with the "types" (i.e., reasons) for an Award Mod';

-- ------------------------------------------------------------
-- Table: aasbs.award_referenced_solicit
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award_referenced_solicit" (
    "id" bigint NOT NULL,
    "award_id" bigint NOT NULL,
    "referenced_solicit_id" bigint NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_award_referenced_solicit" PRIMARY KEY (id),
    CONSTRAINT "fk_awd_refd_sol_acq" FOREIGN KEY (award_id) REFERENCES aasbs.award(id),
    CONSTRAINT "fk_awd_refd_sol_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awd_refd_sol_refsol_id" FOREIGN KEY (referenced_solicit_id) REFERENCES aasbs.solicit(id),
    CONSTRAINT "fk_awd_refd_sol_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_awd_refd_sol_awdid ON aasbs.award_referenced_solicit USING btree (award_id);
CREATE INDEX ix_awd_refd_sol_cbui ON aasbs.award_referenced_solicit USING btree (created_by_user_id);
CREATE INDEX ix_awd_refd_sol_solid ON aasbs.award_referenced_solicit USING btree (referenced_solicit_id);
CREATE INDEX ix_awd_refd_sol_ubui ON aasbs.award_referenced_solicit USING btree (updated_by_user_id);
CREATE UNIQUE INDEX ux_awd_refd_awdid_solid ON aasbs.award_referenced_solicit USING btree (award_id, referenced_solicit_id);

CREATE TRIGGER award_referenced_solicit_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award_referenced_solicit FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_referenced_solicit_hst();

COMMENT ON COLUMN "aasbs"."award_referenced_solicit"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_REFERENCED_SOLICIT_SEQ';
COMMENT ON COLUMN "aasbs"."award_referenced_solicit"."award_id" IS 'Foreign Key to table AASBS.AWARD';
COMMENT ON COLUMN "aasbs"."award_referenced_solicit"."referenced_solicit_id" IS 'Foreign Key to table AASBS.SOLICIT';
COMMENT ON COLUMN "aasbs"."award_referenced_solicit"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award_referenced_solicit"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award_referenced_solicit"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award_referenced_solicit"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."award_referenced_solicit" IS 'Solictations that are referenced from an Award';

-- ------------------------------------------------------------
-- Table: aasbs.award_responsible
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award_responsible" (
    "id" bigint NOT NULL,
    "award_id" bigint NOT NULL,
    "user_id" character varying(16) NOT NULL,
    "responsible_role_cd" character varying(30) NOT NULL,
    "is_primary_yn" character(1) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_awd_responsible" PRIMARY KEY (id),
    CONSTRAINT "ck_awd_resp_is_primeyn" CHECK (is_primary_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_awd_resp_acq" FOREIGN KEY (award_id) REFERENCES aasbs.award(id),
    CONSTRAINT "fk_awd_resp_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awd_resp_emp" FOREIGN KEY (user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awd_resp_resp_role" FOREIGN KEY (responsible_role_cd) REFERENCES aasbs.lu_responsible_role(cd),
    CONSTRAINT "fk_awd_resp_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_awd_resp_awd_id ON aasbs.award_responsible USING btree (award_id);
CREATE INDEX ix_awd_resp_cbui ON aasbs.award_responsible USING btree (created_by_user_id);
CREATE INDEX ix_awd_resp_resp_role ON aasbs.award_responsible USING btree (responsible_role_cd);
CREATE INDEX ix_awd_resp_ubui ON aasbs.award_responsible USING btree (updated_by_user_id);
CREATE INDEX ix_awd_resp_ui ON aasbs.award_responsible USING btree (user_id);
CREATE UNIQUE INDEX ux_awd_respnsbl_one_primary ON aasbs.award_responsible USING btree ((
CASE
    WHEN (is_primary_yn = 'Y'::bpchar) THEN (((responsible_role_cd)::text || (award_id)::text))::character varying
    ELSE (id)::character varying(10)
END));

CREATE TRIGGER award_responsible_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award_responsible FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_responsible_hst();

COMMENT ON COLUMN "aasbs"."award_responsible"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.award_responsible_SEQ';
COMMENT ON COLUMN "aasbs"."award_responsible"."award_id" IS 'Foreign Key to table AASBS.AWARD';
COMMENT ON COLUMN "aasbs"."award_responsible"."user_id" IS 'Foreign Key to table ASSIST.USERS - the responsible user';
COMMENT ON COLUMN "aasbs"."award_responsible"."responsible_role_cd" IS 'Foreign Key to table AASBS.LU_RESPONSIBLE_ROLE';
COMMENT ON COLUMN "aasbs"."award_responsible"."is_primary_yn" IS 'Flag indicating whether a person associated with an Award is the primary for their role (=Y)  or not (=N)';
COMMENT ON COLUMN "aasbs"."award_responsible"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award_responsible"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award_responsible"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award_responsible"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."award_responsible" IS 'Contains the always-current list of contacts for an Award';

-- ------------------------------------------------------------
-- Table: aasbs.award_servicing_aac
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award_servicing_aac" (
    "id" bigint NOT NULL,
    "award_id" bigint NOT NULL,
    "activity_address_cd" character varying(6) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_award_servicing_aac" PRIMARY KEY (id),
    CONSTRAINT "fk_awd_svc_aac_aac" FOREIGN KEY (activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd),
    CONSTRAINT "fk_awd_svc_aac_awd_id" FOREIGN KEY (award_id) REFERENCES aasbs.award(id),
    CONSTRAINT "fk_awd_svc_aac_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awd_svc_aac_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_awd_svc_aac_aac ON aasbs.award_servicing_aac USING btree (activity_address_cd);
CREATE INDEX ix_awd_svc_aac_awd_id ON aasbs.award_servicing_aac USING btree (award_id);
CREATE INDEX ix_awd_svc_aac_cbui ON aasbs.award_servicing_aac USING btree (created_by_user_id);
CREATE INDEX ix_awd_svc_aac_ubui ON aasbs.award_servicing_aac USING btree (updated_by_user_id);

CREATE TRIGGER award_servicing_aac_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award_servicing_aac FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_servicing_aac_hst();

COMMENT ON COLUMN "aasbs"."award_servicing_aac"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_SERVICING_AAC_SEQ';
COMMENT ON COLUMN "aasbs"."award_servicing_aac"."award_id" IS 'Foreign Key to table AASBS.AWARD';
COMMENT ON COLUMN "aasbs"."award_servicing_aac"."activity_address_cd" IS 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE';
COMMENT ON COLUMN "aasbs"."award_servicing_aac"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award_servicing_aac"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award_servicing_aac"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award_servicing_aac"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."award_servicing_aac" IS 'Contains Servicing AACs for the Award';

-- ------------------------------------------------------------
-- Table: aasbs.award_validation_override
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."award_validation_override" (
    "id" bigint NOT NULL,
    "award_mod_id" bigint NOT NULL,
    "award_validation_cd" character varying(30) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_award_validation_override" PRIMARY KEY (id),
    CONSTRAINT "fk_awd_val_ovrrd_awd_mod_id" FOREIGN KEY (award_mod_id) REFERENCES aasbs.award_mod(id),
    CONSTRAINT "fk_awd_val_ovrrd_awd_val_cd" FOREIGN KEY (award_validation_cd) REFERENCES aasbs.lu_award_validation(cd),
    CONSTRAINT "fk_awd_val_ovrrd_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_awd_val_ovrrd_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_awd_val_ovrrd_awd_mod_id ON aasbs.award_validation_override USING btree (award_mod_id);
CREATE INDEX ix_awd_val_ovrrd_cbui ON aasbs.award_validation_override USING btree (created_by_user_id);
CREATE INDEX ix_awd_val_ovrrd_resp_role ON aasbs.award_validation_override USING btree (award_validation_cd);
CREATE INDEX ix_awd_val_ovrrd_ubui ON aasbs.award_validation_override USING btree (updated_by_user_id);

CREATE TRIGGER award_validation_override_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.award_validation_override FOR EACH ROW EXECUTE FUNCTION aasbs.trg_award_validation_override_hst();

COMMENT ON COLUMN "aasbs"."award_validation_override"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.AWARD_VALIDATION_OVERRIDE_SEQ';
COMMENT ON COLUMN "aasbs"."award_validation_override"."award_mod_id" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs"."award_validation_override"."award_validation_cd" IS 'Foreign Key to table AASBS.LU_AWARD_VALIDATION';
COMMENT ON COLUMN "aasbs"."award_validation_override"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."award_validation_override"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."award_validation_override"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."award_validation_override"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."award_validation_override" IS 'Contains the always-current list of contacts for an Award';

-- ------------------------------------------------------------
-- Table: aasbs.bill_correction
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."bill_correction" (
    "id" bigint NOT NULL,
    "financial_services_id" bigint NOT NULL,
    "cost_amt" numeric(10,2) NOT NULL,
    "service_charge_amt" numeric(10,2) NOT NULL,
    "to_loa_tracking_num" character varying(20),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_bill_correction" PRIMARY KEY (id),
    CONSTRAINT "fk_billc_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_billc_fsid" FOREIGN KEY (financial_services_id) REFERENCES aasbs.financial_services(id),
    CONSTRAINT "fk_billc_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_billc_cbui ON aasbs.bill_correction USING btree (created_by_user_id);
CREATE INDEX ix_billc_fsid ON aasbs.bill_correction USING btree (financial_services_id);
CREATE INDEX ix_billc_ubui ON aasbs.bill_correction USING btree (updated_by_user_id);

CREATE TRIGGER bill_correction_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.bill_correction FOR EACH ROW EXECUTE FUNCTION aasbs.trg_bill_correction_hst();

COMMENT ON COLUMN "aasbs"."bill_correction"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.BILL_CORRECTION_SEQ';
COMMENT ON COLUMN "aasbs"."bill_correction"."financial_services_id" IS 'Foreign Key to table AASBS.FINANCIAL_SERVICES';
COMMENT ON COLUMN "aasbs"."bill_correction"."cost_amt" IS 'Correcting amount related to cost/deliverable billings';
COMMENT ON COLUMN "aasbs"."bill_correction"."service_charge_amt" IS 'Correcting amount related to service charge billings';
COMMENT ON COLUMN "aasbs"."bill_correction"."to_loa_tracking_num" IS 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via it''s Tracking_Num';
COMMENT ON COLUMN "aasbs"."bill_correction"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."bill_correction"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."bill_correction"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."bill_correction"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."bill_correction" IS 'For a correction to a BILL_ITEM entry, capture user entered values';

-- ------------------------------------------------------------
-- Table: aasbs.bill_item
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."bill_item" (
    "id" bigint NOT NULL,
    "tracking_num" character varying(20) NOT NULL,
    "line_item_accepted_id" bigint,
    "actual_bill_dt" timestamp(6) without time zone,
    "eligible_for_transmit_yn" character(1),
    "bill_item_amt" numeric(15,2) NOT NULL,
    "bill_item_type_cd" character varying(20) NOT NULL,
    "funding_id" bigint NOT NULL,
    "ia_id" bigint NOT NULL,
    "billing_id" bigint,
    "bill_item_source_cd" character varying(20),
    "src_service_charge_schedule_id" bigint,
    "src_tkeeping_monthly_hours_id" bigint,
    "src_financial_services_id" bigint,
    "src_preassist_order_loa_map_id" bigint,
    "src_cost_acceptance_dist_id" bigint,
    "src_surchg_acceptance_dist_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "src_travel_voucher_id" bigint,
    CONSTRAINT "pk_bill_item" PRIMARY KEY (id),
    CONSTRAINT "ck_bill_item_elig_4_trans_yn" CHECK (eligible_for_transmit_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_bill_item_bill_item_type" FOREIGN KEY (bill_item_type_cd) REFERENCES aasbs.lu_bill_item_type(cd),
    CONSTRAINT "fk_bill_item_billing_id" FOREIGN KEY (billing_id) REFERENCES billing.billing(billing_id),
    CONSTRAINT "fk_bill_item_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_bill_item_funding_id" FOREIGN KEY (funding_id) REFERENCES aasbs.funding(id),
    CONSTRAINT "fk_bill_item_ia_id" FOREIGN KEY (ia_id) REFERENCES aasbs.ia(id),
    CONSTRAINT "fk_bill_item_liaccept_id" FOREIGN KEY (line_item_accepted_id) REFERENCES aasbs.line_item_accepted(id),
    CONSTRAINT "fk_bill_item_source_cd" FOREIGN KEY (bill_item_source_cd) REFERENCES aasbs.lu_bill_item_source(cd),
    CONSTRAINT "fk_bill_item_src_cost_dist" FOREIGN KEY (src_cost_acceptance_dist_id) REFERENCES aasbs.acceptance_dist(id),
    CONSTRAINT "fk_bill_item_src_sc_sched" FOREIGN KEY (src_service_charge_schedule_id) REFERENCES aasbs.service_charge_schedule(id),
    CONSTRAINT "fk_bill_item_src_schg_dist" FOREIGN KEY (src_surchg_acceptance_dist_id) REFERENCES aasbs.acceptance_dist(id),
    CONSTRAINT "fk_bill_item_src_tkeep_mon_hrs" FOREIGN KEY (src_tkeeping_monthly_hours_id) REFERENCES aasbs_timekeeping.monthly_hours(id),
    CONSTRAINT "fk_bill_item_src_trav_vch_id" FOREIGN KEY (src_travel_voucher_id) REFERENCES aasbs.travel_voucher(id),
    CONSTRAINT "fk_bill_item_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_finsrv_src_fin_srv_id" FOREIGN KEY (src_financial_services_id) REFERENCES aasbs.financial_services(id)
);

CREATE INDEX ix_bill_item_bill_amt ON aasbs.bill_item USING btree (bill_item_amt);
CREATE INDEX ix_bill_item_bill_item_type ON aasbs.bill_item USING btree (bill_item_type_cd);
CREATE INDEX ix_bill_item_billing_id ON aasbs.bill_item USING btree (billing_id);
CREATE INDEX ix_bill_item_cbui ON aasbs.bill_item USING btree (created_by_user_id);
CREATE INDEX ix_bill_item_funding_id ON aasbs.bill_item USING btree (funding_id);
CREATE INDEX ix_bill_item_ia_id ON aasbs.bill_item USING btree (ia_id);
CREATE INDEX ix_bill_item_liaccept_id ON aasbs.bill_item USING btree (line_item_accepted_id);
CREATE INDEX ix_bill_item_source_cd ON aasbs.bill_item USING btree (bill_item_source_cd);
CREATE INDEX ix_bill_item_src_cost_dist ON aasbs.bill_item USING btree (src_cost_acceptance_dist_id);
CREATE INDEX ix_bill_item_src_fin_srv_id ON aasbs.bill_item USING btree (src_financial_services_id);
CREATE INDEX ix_bill_item_src_sc_sched ON aasbs.bill_item USING btree (src_service_charge_schedule_id);
CREATE INDEX ix_bill_item_src_schg_dist ON aasbs.bill_item USING btree (src_surchg_acceptance_dist_id);
CREATE INDEX ix_bill_item_src_tkeep_mon_hrs ON aasbs.bill_item USING btree (src_tkeeping_monthly_hours_id);
CREATE INDEX ix_bill_item_src_trav_vch_id ON aasbs.bill_item USING btree (src_travel_voucher_id);
CREATE INDEX ix_bill_item_trk_num_source ON aasbs.bill_item USING btree (tracking_num, bill_item_source_cd);
CREATE INDEX ix_bill_item_ubui ON aasbs.bill_item USING btree (updated_by_user_id);

CREATE TRIGGER bill_item_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.bill_item FOR EACH ROW EXECUTE FUNCTION aasbs.trg_bill_item_hst();

COMMENT ON COLUMN "aasbs"."bill_item"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.BILL_ITEM_SEQ';
COMMENT ON COLUMN "aasbs"."bill_item"."tracking_num" IS 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via it''s Tracking_Num';
COMMENT ON COLUMN "aasbs"."bill_item"."line_item_accepted_id" IS 'Foreign Key to table AASBS.LINE_ITEM_ACCEPTED';
COMMENT ON COLUMN "aasbs"."bill_item"."actual_bill_dt" IS 'Date a Bill was succesfully submitted to PEGASYS';
COMMENT ON COLUMN "aasbs"."bill_item"."eligible_for_transmit_yn" IS 'Indicator that determines whether a BILL_ITEM record should be transmitted to Pegasys (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."bill_item"."bill_item_amt" IS 'Amount billed against LOA_Ledger "Line_Item_Obligated_Amt" or "Service_Charge_Amt" column';
COMMENT ON COLUMN "aasbs"."bill_item"."bill_item_type_cd" IS 'Foreign Key to table AASBS.LU_BILL_ITEM_TYPE';
COMMENT ON COLUMN "aasbs"."bill_item"."funding_id" IS 'Foreign Key to table AASBS.FUNDING';
COMMENT ON COLUMN "aasbs"."bill_item"."ia_id" IS 'Foreign Key to table AASBS.IA';
COMMENT ON COLUMN "aasbs"."bill_item"."billing_id" IS 'Foreign Key to table BILLING.BILLING';
COMMENT ON COLUMN "aasbs"."bill_item"."bill_item_source_cd" IS 'Foreign Key to table AASBS.LU_BILL_ITEM_SOURCE';
COMMENT ON COLUMN "aasbs"."bill_item"."src_service_charge_schedule_id" IS 'Foreign Key to table AASBS.SERVICE_CHARGE_SCHEDULE';
COMMENT ON COLUMN "aasbs"."bill_item"."src_tkeeping_monthly_hours_id" IS 'Foreign Key to table AASBS_TIMEKEEPING.MONTHLY_HOURS';
COMMENT ON COLUMN "aasbs"."bill_item"."src_financial_services_id" IS 'Foreign Key to table AASBS.FINANCIAL_SERVICES';
COMMENT ON COLUMN "aasbs"."bill_item"."src_preassist_order_loa_map_id" IS 'Foreign Key to table AASBS.PREASSIST_ORDER_LOA_MAP';
COMMENT ON COLUMN "aasbs"."bill_item"."src_cost_acceptance_dist_id" IS 'Foreign Key to table AASBS.ACCEPTANCE_DIST';
COMMENT ON COLUMN "aasbs"."bill_item"."src_surchg_acceptance_dist_id" IS 'Foreign Key to table AASBS.ACCEPTANCE_DIST';
COMMENT ON COLUMN "aasbs"."bill_item"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."bill_item"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."bill_item"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."bill_item"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."bill_item" IS 'Contains the individual billed amounts against LOA_Ledger records';

-- ------------------------------------------------------------
-- Table: aasbs.bill_item_correction
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."bill_item_correction" (
    "id" bigint NOT NULL,
    "bill_correction_id" bigint NOT NULL,
    "bill_item_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "tracking_item_id" bigint,
    CONSTRAINT "pk_bill_item_correction" PRIMARY KEY (id),
    CONSTRAINT "ck_bill_item_correction_fk_missing" CHECK (bill_item_id IS NOT NULL OR tracking_item_id IS NOT NULL),
    CONSTRAINT "fk_billic_bcid" FOREIGN KEY (bill_correction_id) REFERENCES aasbs.bill_correction(id),
    CONSTRAINT "fk_billic_biid" FOREIGN KEY (bill_item_id) REFERENCES aasbs.bill_item(id),
    CONSTRAINT "fk_billic_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_billic_tiid" FOREIGN KEY (tracking_item_id) REFERENCES aasbs.tracking_item(id),
    CONSTRAINT "fk_billic_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_billic_bcid ON aasbs.bill_item_correction USING btree (bill_correction_id);
CREATE INDEX ix_billic_biid ON aasbs.bill_item_correction USING btree (bill_item_id);
CREATE INDEX ix_billic_cbui ON aasbs.bill_item_correction USING btree (created_by_user_id);
CREATE INDEX ix_billic_tiid ON aasbs.bill_item_correction USING btree (tracking_item_id);
CREATE INDEX ix_billic_ubui ON aasbs.bill_item_correction USING btree (updated_by_user_id);

CREATE TRIGGER bill_item_correction_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.bill_item_correction FOR EACH ROW EXECUTE FUNCTION aasbs.trg_bill_item_correction_hst();

COMMENT ON COLUMN "aasbs"."bill_item_correction"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.BILL_ITEM_CORRECTION_SEQ';
COMMENT ON COLUMN "aasbs"."bill_item_correction"."bill_correction_id" IS 'Foreign Key to AASBS.BILL_CORRECTION - Link to parent table.';
COMMENT ON COLUMN "aasbs"."bill_item_correction"."bill_item_id" IS 'Foreign Key to AASBS.BILL_ITEM - Link to record being corrected.';
COMMENT ON COLUMN "aasbs"."bill_item_correction"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."bill_item_correction"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."bill_item_correction"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."bill_item_correction"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."bill_item_correction"."tracking_item_id" IS 'Foreign Key to AASBS.TRACKING_ITEM - Link to tracking record being corrected.';

COMMENT ON TABLE "aasbs"."bill_item_correction" IS 'Captures AASBS.BILL_ITEM records credited by a Financial Services transaction.  Child table to AASBS.BILL_CORRECTION.';

-- ------------------------------------------------------------
-- Table: aasbs.central_collab
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."central_collab" (
    "id" bigint NOT NULL,
    "collab_type_cd" character varying(30) NOT NULL,
    "collab_status_cd" character varying(30) NOT NULL,
    "collab_num" bigint NOT NULL,
    "due_dt" timestamp(6) without time zone,
    "subject" character varying(1000),
    "contractor_access_yn" character(1) NOT NULL,
    "client_access_yn" character(1) NOT NULL,
    "attach_form_name" character varying(200),
    "table_name_cd" character varying(30) NOT NULL,
    "table_record_id" bigint NOT NULL,
    "attach_hash_id" character varying(64),
    "submitted_by_name" character varying(75),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_central_collab" PRIMARY KEY (id),
    CONSTRAINT "fk_centcolab_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_centcolab_colstat" FOREIGN KEY (collab_status_cd) REFERENCES aasbs.lu_collab_status(cd),
    CONSTRAINT "fk_centcolab_coltype" FOREIGN KEY (collab_type_cd) REFERENCES aasbs.lu_collab_type(cd),
    CONSTRAINT "fk_centcolab_table_name" FOREIGN KEY (table_name_cd) REFERENCES aasbs.lu_table_name(cd),
    CONSTRAINT "fk_centcolab_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_centcolab_cbui ON aasbs.central_collab USING btree (created_by_user_id);
CREATE INDEX ix_centcolab_colstat ON aasbs.central_collab USING btree (collab_status_cd);
CREATE INDEX ix_centcolab_coltype ON aasbs.central_collab USING btree (collab_type_cd);
CREATE INDEX ix_centcolab_table_name ON aasbs.central_collab USING btree (table_name_cd);
CREATE INDEX ix_centcolab_table_rec_id ON aasbs.central_collab USING btree (table_record_id);
CREATE INDEX ix_centcolab_ubui ON aasbs.central_collab USING btree (updated_by_user_id);

CREATE TRIGGER central_collab_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.central_collab FOR EACH ROW EXECUTE FUNCTION aasbs.trg_central_collab_hst();
CREATE TRIGGER trg_cclb BEFORE INSERT OR UPDATE ON aasbs.central_collab FOR EACH ROW EXECUTE FUNCTION aasbs."trg_cclb$central_collab"();
CREATE TRIGGER trg_ccom_central_collab BEFORE DELETE ON aasbs.central_collab FOR EACH ROW EXECUTE FUNCTION aasbs."trg_ccom_central_collab$central_collab"();

COMMENT ON COLUMN "aasbs"."central_collab"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.CENTRAL_COLLAB_SEQ';
COMMENT ON COLUMN "aasbs"."central_collab"."collab_type_cd" IS 'Foreign Key to table AASBS.LU_COLLAB_TYPE';
COMMENT ON COLUMN "aasbs"."central_collab"."collab_status_cd" IS 'Foreign Key to table AASBS.LU_COLLAB_STATUS';
COMMENT ON COLUMN "aasbs"."central_collab"."collab_num" IS 'The Collaboration numbering 0001,0002,0003,..n - sequence repeats for each distinct PROCUREMENT_ID in this table';
COMMENT ON COLUMN "aasbs"."central_collab"."due_dt" IS 'Collaboration Due Date';
COMMENT ON COLUMN "aasbs"."central_collab"."subject" IS 'User-entered Subject of the Collaboration';
COMMENT ON COLUMN "aasbs"."central_collab"."contractor_access_yn" IS 'Flag indicating whether Contractor can (=Y) or cannot (=N) access this Collaboration';
COMMENT ON COLUMN "aasbs"."central_collab"."client_access_yn" IS 'Flag indicating whether Client can (=Y) or cannot (=N) access this Collaboration';
COMMENT ON COLUMN "aasbs"."central_collab"."attach_form_name" IS 'Form Name attached to the Collaboration';
COMMENT ON COLUMN "aasbs"."central_collab"."table_name_cd" IS 'Foreign Key to table AASBS.LU_TABLE_NAME';
COMMENT ON COLUMN "aasbs"."central_collab"."table_record_id" IS 'Logical foreign key from the table in CENTRAL_COLLAB.TABLE_NAME_CD column (enforced via triggers)';
COMMENT ON COLUMN "aasbs"."central_collab"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."central_collab"."submitted_by_name" IS 'Free-text Full Name of the person who created the collaboration';
COMMENT ON COLUMN "aasbs"."central_collab"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."central_collab"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."central_collab"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."central_collab"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."central_collab" IS 'Central Collaboration details shared by all tables using Central Collab system';

-- ------------------------------------------------------------
-- Table: aasbs.central_collab_acquisition
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."central_collab_acquisition" (
    "id" bigint NOT NULL,
    "central_collab_id" bigint NOT NULL,
    "acquisition_id" bigint NOT NULL,
    "funds_usage_cd" character varying(20) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_central_collab_acquisition" PRIMARY KEY (id),
    CONSTRAINT "fk_collab_acq_acq_id" FOREIGN KEY (acquisition_id) REFERENCES aasbs.acquisition(id),
    CONSTRAINT "fk_collab_acq_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_collab_acq_collab_id" FOREIGN KEY (central_collab_id) REFERENCES aasbs.central_collab(id),
    CONSTRAINT "fk_collab_acq_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_collab_acq_usage_cd" FOREIGN KEY (funds_usage_cd) REFERENCES aasbs.lu_funds_usage(cd)
);

CREATE INDEX ix_collab_acq_acq_id ON aasbs.central_collab_acquisition USING btree (acquisition_id);
CREATE INDEX ix_collab_acq_cbui ON aasbs.central_collab_acquisition USING btree (created_by_user_id);
CREATE INDEX ix_collab_acq_collab_id ON aasbs.central_collab_acquisition USING btree (central_collab_id);
CREATE INDEX ix_collab_acq_ubui ON aasbs.central_collab_acquisition USING btree (updated_by_user_id);
CREATE INDEX ix_collab_acq_usage_cd ON aasbs.central_collab_acquisition USING btree (funds_usage_cd);

CREATE TRIGGER central_collab_acquisition_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.central_collab_acquisition FOR EACH ROW EXECUTE FUNCTION aasbs.trg_central_collab_acquisition_hst();

COMMENT ON COLUMN "aasbs"."central_collab_acquisition"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.CENTRAL_COLLAB_ACQUISITION_SEQ';
COMMENT ON COLUMN "aasbs"."central_collab_acquisition"."central_collab_id" IS 'Foreign Key to table AASBS.CENTRAL_COLLAB';
COMMENT ON COLUMN "aasbs"."central_collab_acquisition"."acquisition_id" IS 'Foreign Key to table AASBS.ACQUISITION';
COMMENT ON COLUMN "aasbs"."central_collab_acquisition"."funds_usage_cd" IS 'Foreign Key to table AASBS.LU_FUNDS_USAGE';
COMMENT ON COLUMN "aasbs"."central_collab_acquisition"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."central_collab_acquisition"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."central_collab_acquisition"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."central_collab_acquisition"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."central_collab_acquisition" IS 'This table connects collaborations to related acquisitions';

-- ------------------------------------------------------------
-- Table: aasbs.central_collab_ia
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."central_collab_ia" (
    "id" bigint NOT NULL,
    "central_collab_id" bigint NOT NULL,
    "fund_return_yn" character(1) NOT NULL,
    "funding_num" character varying(20),
    "amendment_num" character varying(4),
    "acquisition" character varying(20),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "funds_usage_cd" character varying(30),
    "service_charge_info" character varying(20),
    "modified_by_fm_user_id" character varying(16),
    CONSTRAINT "pk_central_collab_ia" PRIMARY KEY (id),
    CONSTRAINT "ck_centcolab_ia_fund_returnyn" CHECK (fund_return_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_centcolab_funds_use_cd" FOREIGN KEY (funds_usage_cd) REFERENCES aasbs.lu_funds_usage(cd),
    CONSTRAINT "fk_centcolab_ia_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_centcolab_ia_cent_collab_id" FOREIGN KEY (central_collab_id) REFERENCES aasbs.central_collab(id),
    CONSTRAINT "fk_centcolab_ia_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_central_collab_ia_mod_fm_users" FOREIGN KEY (modified_by_fm_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_cci_modified_by_fm_user_id ON aasbs.central_collab_ia USING btree (modified_by_fm_user_id);
CREATE INDEX ix_centcolab_funds_use_cd ON aasbs.central_collab_ia USING btree (funds_usage_cd);
CREATE INDEX ix_centcolab_ia_cbui ON aasbs.central_collab_ia USING btree (created_by_user_id);
CREATE INDEX ix_centcolab_ia_ccollabid ON aasbs.central_collab_ia USING btree (central_collab_id);
CREATE INDEX ix_centcolab_ia_ubui ON aasbs.central_collab_ia USING btree (updated_by_user_id);

CREATE TRIGGER central_collab_ia_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.central_collab_ia FOR EACH ROW EXECUTE FUNCTION aasbs.trg_central_collab_ia_hst();

COMMENT ON COLUMN "aasbs"."central_collab_ia"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.CENTRAL_COLLAB_IA_SEQ';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."central_collab_id" IS 'Foreign Key to table AASBS.CENTRAL_COLLAB.ID';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."fund_return_yn" IS 'Is this request returning funds?';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."funding_num" IS 'Client funding document number';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."amendment_num" IS 'Client funding document amendment number';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."acquisition" IS 'Supporting Acquisition';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."funds_usage_cd" IS 'Foreign Key to table AASBS.LU_FUNDS_USAGE';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."service_charge_info" IS 'This column is not used - always has one value - Service Charge';
COMMENT ON COLUMN "aasbs"."central_collab_ia"."modified_by_fm_user_id" IS 'Foreign key to ASSIST Users userid';

COMMENT ON TABLE "aasbs"."central_collab_ia" IS 'This table contains additional data captured for an IA Fund Request Collaboration';

-- ------------------------------------------------------------
-- Table: aasbs.central_collab_post_award
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."central_collab_post_award" (
    "id" bigint NOT NULL,
    "central_collab_id" bigint NOT NULL,
    "resume_emp_num" character varying(45),
    "resume_emp_name" character varying(75),
    "funds_intake_fund_doc_num" character varying(25),
    "funds_intake_amend_num" integer,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_central_collab_post_award" PRIMARY KEY (id),
    CONSTRAINT "fk_centcolab_pa_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_centcolab_pa_cent_collab_id" FOREIGN KEY (central_collab_id) REFERENCES aasbs.central_collab(id),
    CONSTRAINT "fk_centcolab_pa_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_centcolab_pa_cbui ON aasbs.central_collab_post_award USING btree (created_by_user_id);
CREATE INDEX ix_centcolab_pa_ccollabid ON aasbs.central_collab_post_award USING btree (central_collab_id);
CREATE INDEX ix_centcolab_pa_ubui ON aasbs.central_collab_post_award USING btree (updated_by_user_id);

CREATE TRIGGER central_collab_post_award_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.central_collab_post_award FOR EACH ROW EXECUTE FUNCTION aasbs.trg_central_collab_post_award_hst();

COMMENT ON COLUMN "aasbs"."central_collab_post_award"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.CENTRAL_COLLAB_POST_AWARD_SEQ';
COMMENT ON COLUMN "aasbs"."central_collab_post_award"."central_collab_id" IS 'Foreign Key to table AASBS.CENTRAL_COLLAB';
COMMENT ON COLUMN "aasbs"."central_collab_post_award"."resume_emp_num" IS 'Hold Employee Number only for Collab Type = RESUME';
COMMENT ON COLUMN "aasbs"."central_collab_post_award"."resume_emp_name" IS 'Employee Name only for Collab Type = RESUME';
COMMENT ON COLUMN "aasbs"."central_collab_post_award"."funds_intake_fund_doc_num" IS 'Contains the related Funding Document Number for FUNDS_INTAKE Collab Types';
COMMENT ON COLUMN "aasbs"."central_collab_post_award"."funds_intake_amend_num" IS 'Contains the related Funding Package Amendment Number for FUNDS_INTAKE Collab Types';
COMMENT ON COLUMN "aasbs"."central_collab_post_award"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."central_collab_post_award"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."central_collab_post_award"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."central_collab_post_award"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."central_collab_post_award" IS 'Central Collaboration details specific to only Post Award';

-- ------------------------------------------------------------
-- Table: aasbs.central_collab_responsible
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."central_collab_responsible" (
    "id" bigint NOT NULL,
    "central_collab_id" bigint NOT NULL,
    "responsible_user_id" character varying(16) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_cent_collab_resp" PRIMARY KEY (id),
    CONSTRAINT "fk_cent_collab_resp_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_cent_collab_resp_collab" FOREIGN KEY (central_collab_id) REFERENCES aasbs.central_collab(id),
    CONSTRAINT "fk_cent_collab_resp_emp" FOREIGN KEY (responsible_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_cent_collab_resp_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_cent_collab_resp_cbui ON aasbs.central_collab_responsible USING btree (created_by_user_id);
CREATE INDEX ix_cent_collab_resp_collab ON aasbs.central_collab_responsible USING btree (central_collab_id);
CREATE INDEX ix_cent_collab_resp_rui ON aasbs.central_collab_responsible USING btree (responsible_user_id);
CREATE INDEX ix_cent_collab_resp_ubui ON aasbs.central_collab_responsible USING btree (updated_by_user_id);

CREATE TRIGGER central_collab_responsible_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.central_collab_responsible FOR EACH ROW EXECUTE FUNCTION aasbs.trg_central_collab_responsible_hst();

COMMENT ON COLUMN "aasbs"."central_collab_responsible"."id" IS 'Primary key for this table.  Filled with sequence:  CENTRAL_COLLAB_RESPONSIBLE_SEQ';
COMMENT ON COLUMN "aasbs"."central_collab_responsible"."central_collab_id" IS 'Foreign Key to AASBS.CENTRAL_COLLAB table';
COMMENT ON COLUMN "aasbs"."central_collab_responsible"."responsible_user_id" IS 'Collaborating User - Foreign key to ASSIST.USERS(ID)';
COMMENT ON COLUMN "aasbs"."central_collab_responsible"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."central_collab_responsible"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."central_collab_responsible"."updated_by_user_id" IS 'Record last updated by this user - Foreign key to ASSIST.USERS(ID)';
COMMENT ON COLUMN "aasbs"."central_collab_responsible"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."central_collab_responsible" IS 'This table connects collaborating people to the forms on which they are working together';

-- ------------------------------------------------------------
-- Table: aasbs.central_comment
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."central_comment" (
    "id" bigint NOT NULL,
    "table_name_cd" character varying(30) NOT NULL,
    "table_record_id" bigint NOT NULL,
    "comment_type_cd" character varying(30) NOT NULL,
    "comment_text" character varying(4000) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_cent_cmnt" PRIMARY KEY (id),
    CONSTRAINT "fk_cent_cmnt_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_cent_cmnt_comment_type" FOREIGN KEY (comment_type_cd) REFERENCES aasbs.lu_central_comment_type(cd),
    CONSTRAINT "fk_cent_cmnt_table_name" FOREIGN KEY (table_name_cd) REFERENCES aasbs.lu_table_name(cd),
    CONSTRAINT "fk_cent_cmnt_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_cent_cmnt_cbui ON aasbs.central_comment USING btree (created_by_user_id);
CREATE INDEX ix_cent_cmnt_comment_type ON aasbs.central_comment USING btree (comment_type_cd);
CREATE INDEX ix_cent_cmnt_table_name ON aasbs.central_comment USING btree (table_name_cd);
CREATE INDEX ix_cent_cmnt_table_rec_id ON aasbs.central_comment USING btree (table_record_id);
CREATE INDEX ix_cent_cmnt_ubui ON aasbs.central_comment USING btree (updated_by_user_id);

CREATE TRIGGER central_comment_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.central_comment FOR EACH ROW EXECUTE FUNCTION aasbs.trg_central_comment_hst();
CREATE TRIGGER trg_ccom BEFORE INSERT OR UPDATE ON aasbs.central_comment FOR EACH ROW EXECUTE FUNCTION aasbs."trg_ccom$central_comment"();

COMMENT ON COLUMN "aasbs"."central_comment"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.CENTRAL_COMMENT_SEQ';
COMMENT ON COLUMN "aasbs"."central_comment"."table_name_cd" IS 'Foreign Key to table AASBS.LU_TABLE_NAME';
COMMENT ON COLUMN "aasbs"."central_comment"."table_record_id" IS 'Logically foreign key from the table in CENTRAL_COMMENT.TABLE_NAME_CD column (enforced via triggers)';
COMMENT ON COLUMN "aasbs"."central_comment"."comment_type_cd" IS 'Foreign Key to table AASBS.LU_COMMENT_TYPE';
COMMENT ON COLUMN "aasbs"."central_comment"."comment_text" IS 'Holds actual text of a comment';
COMMENT ON COLUMN "aasbs"."central_comment"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."central_comment"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."central_comment"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."central_comment"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."central_comment" IS 'Central Comment table that is used by all other tables that have comments associated with their records';

-- ------------------------------------------------------------
-- Table: aasbs.chronology
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."chronology" (
    "id" bigint NOT NULL,
    "source_table_name_cd" character varying(30) NOT NULL,
    "source_record_id" character varying(32) NOT NULL,
    "event_table_name_cd" character varying(30),
    "event_record_id" character varying(32),
    "event_type_cd" character varying(30) NOT NULL,
    "entry_description" character varying(4000) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk1_chron" PRIMARY KEY (id),
    CONSTRAINT "fk_chron_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_chron_event_tab" FOREIGN KEY (event_table_name_cd) REFERENCES aasbs.lu_table_name(cd),
    CONSTRAINT "fk_chron_event_type" FOREIGN KEY (event_type_cd) REFERENCES aasbs.lu_event_type(cd),
    CONSTRAINT "fk_chron_source_tab" FOREIGN KEY (source_table_name_cd) REFERENCES aasbs.lu_table_name(cd),
    CONSTRAINT "fk_chron_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_chron_cbui ON aasbs.chronology USING btree (created_by_user_id);
CREATE INDEX ix_chron_event_rec_id ON aasbs.chronology USING btree (event_record_id);
CREATE INDEX ix_chron_event_tab ON aasbs.chronology USING btree (event_table_name_cd);
CREATE INDEX ix_chron_event_type ON aasbs.chronology USING btree (event_type_cd);
CREATE INDEX ix_chron_source_rec_id ON aasbs.chronology USING btree (source_record_id);
CREATE INDEX ix_chron_source_tab ON aasbs.chronology USING btree (source_table_name_cd);
CREATE INDEX ix_chron_ubui ON aasbs.chronology USING btree (updated_by_user_id);

CREATE TRIGGER chronology_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.chronology FOR EACH ROW EXECUTE FUNCTION aasbs.trg_chronology_hst();

COMMENT ON COLUMN "aasbs"."chronology"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.CHRONOLOGY_SEQ';
COMMENT ON COLUMN "aasbs"."chronology"."source_table_name_cd" IS 'Name of table containing the record that acted as the cause of this Chronology Event';
COMMENT ON COLUMN "aasbs"."chronology"."source_record_id" IS 'ID of the record that acted as the cause of this Chronology Event';
COMMENT ON COLUMN "aasbs"."chronology"."event_table_name_cd" IS 'Name of the table in which a record was created as a result of this Chronology Event';
COMMENT ON COLUMN "aasbs"."chronology"."event_record_id" IS 'ID of the record created as a result of this Chronology Event';
COMMENT ON COLUMN "aasbs"."chronology"."event_type_cd" IS 'Foreign Key to table AASBS.LU_EVENT_TYPE';
COMMENT ON COLUMN "aasbs"."chronology"."entry_description" IS 'Description of Chronology Event';
COMMENT ON COLUMN "aasbs"."chronology"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."chronology"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."chronology"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."chronology"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."chronology" IS 'Events captured at key points in each Order''s lifecycle';

-- ------------------------------------------------------------
-- Table: aasbs.email_log
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."email_log" (
    "id" bigint NOT NULL,
    "email_status_cd" character varying(20) NOT NULL,
    "from_email" character varying(200) NOT NULL,
    "subject" character varying(300) NOT NULL,
    "body" character varying(4000),
    "attachments_yn" character(1) NOT NULL,
    "body_large_data_id" bigint,
    "sent_dt" timestamp(6) without time zone,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "retry_count" integer DEFAULT 0 NOT NULL,
    CONSTRAINT "pk_email_log" PRIMARY KEY (id),
    CONSTRAINT "ck_email_log_attach_yn" CHECK (attachments_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_email_log_body_lrge_data_id" FOREIGN KEY (body_large_data_id) REFERENCES aasbs.large_data(id),
    CONSTRAINT "fk_email_log_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_email_log_email_status" FOREIGN KEY (email_status_cd) REFERENCES aasbs.lu_email_status(cd),
    CONSTRAINT "fk_email_log_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_email_log_body_lrge_data_id ON aasbs.email_log USING btree (body_large_data_id);
CREATE INDEX ix_email_log_cbui ON aasbs.email_log USING btree (created_by_user_id);
CREATE INDEX ix_email_log_email_status ON aasbs.email_log USING btree (email_status_cd);
CREATE INDEX ix_email_log_ubui ON aasbs.email_log USING btree (updated_by_user_id);

CREATE TRIGGER email_log_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.email_log FOR EACH ROW EXECUTE FUNCTION aasbs.trg_email_log_hst();

COMMENT ON COLUMN "aasbs"."email_log"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.EMAIL_LOG_SEQ';
COMMENT ON COLUMN "aasbs"."email_log"."email_status_cd" IS 'Foreign Key to table AASBS.LU_EMAIL_STATUS';
COMMENT ON COLUMN "aasbs"."email_log"."from_email" IS 'Contains the email address of the email sender';
COMMENT ON COLUMN "aasbs"."email_log"."subject" IS 'Email Subject';
COMMENT ON COLUMN "aasbs"."email_log"."body" IS 'Body of the email';
COMMENT ON COLUMN "aasbs"."email_log"."attachments_yn" IS 'Flag indicating whether an email had attachments (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."email_log"."body_large_data_id" IS 'Foreign Key to table AASBS.LU_BODY_LARGE_DATA only used for emails with BODY text length > 4000 chars';
COMMENT ON COLUMN "aasbs"."email_log"."sent_dt" IS 'Date email was sent to server';
COMMENT ON COLUMN "aasbs"."email_log"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."email_log"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."email_log"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."email_log"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."email_log"."retry_count" IS 'The number of times the email entry has attempted to be sent.';

COMMENT ON TABLE "aasbs"."email_log" IS 'Contains details of emails generated in ASSIST';

-- ------------------------------------------------------------
-- Table: aasbs.financial_services
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."financial_services" (
    "id" bigint NOT NULL,
    "financial_services_action_cd" character varying(20) NOT NULL,
    "transmit_yn" character(1) NOT NULL,
    "comments" character varying(2000),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "labor_correction_action_cd" character varying(20),
    "approved_by_id" character varying(16),
    CONSTRAINT "pk_financial_services" PRIMARY KEY (id),
    CONSTRAINT "ck_finsrv_transmit_yn" CHECK (transmit_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_finsrv_appby_id" FOREIGN KEY (approved_by_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_finsrv_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_finsrv_fsacd" FOREIGN KEY (financial_services_action_cd) REFERENCES aasbs.lu_financial_services_action(cd),
    CONSTRAINT "fk_finsrv_lca_cd" FOREIGN KEY (labor_correction_action_cd) REFERENCES aasbs.lu_labor_correction_action(cd),
    CONSTRAINT "fk_finsrv_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_finsrv_cbui ON aasbs.financial_services USING btree (created_by_user_id);
CREATE INDEX ix_finsrv_fsid ON aasbs.financial_services USING btree (financial_services_action_cd);
CREATE INDEX ix_finsrv_ubui ON aasbs.financial_services USING btree (updated_by_user_id);
CREATE INDEX ix_fs_approved_by_id ON aasbs.financial_services USING btree (approved_by_id);
CREATE INDEX ix_fs_labor_correction_action_cd ON aasbs.financial_services USING btree (labor_correction_action_cd);

CREATE TRIGGER financial_services_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.financial_services FOR EACH ROW EXECUTE FUNCTION aasbs.trg_financial_services_hst();

COMMENT ON COLUMN "aasbs"."financial_services"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.FINANCIAL_SERVICES_SEQ';
COMMENT ON COLUMN "aasbs"."financial_services"."financial_services_action_cd" IS 'Foreign Key to table AASBS.LU_FINANCIAL_SERVICES_ACTION';
COMMENT ON COLUMN "aasbs"."financial_services"."transmit_yn" IS 'Flag indicating whether transaction should be transmitted on to Pegasys (via AASBS_TRANSMIT tables)';
COMMENT ON COLUMN "aasbs"."financial_services"."comments" IS 'Comments for this transaction';
COMMENT ON COLUMN "aasbs"."financial_services"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."financial_services"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."financial_services"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."financial_services"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."financial_services"."labor_correction_action_cd" IS 'Foreign Key to table AASBS.LU_LABOR_CORRECTION_ACTION';
COMMENT ON COLUMN "aasbs"."financial_services"."approved_by_id" IS 'Foreign Key to table ASSIST.USERS - BM/SM who has authorized the Labor Correction action.';

COMMENT ON TABLE "aasbs"."financial_services" IS 'Captures user entered transaction details for a financial correction';

-- ------------------------------------------------------------
-- Table: aasbs.funding
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."funding" (
    "id" bigint NOT NULL,
    "ia_id" bigint,
    "fund_status_cd" character varying(30) NOT NULL,
    "funding_num" character varying(20) NOT NULL,
    "fund_category_cd" character varying(30) NOT NULL,
    "billing_type_cd" character varying(2) NOT NULL,
    "latest_funding_amendment_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_funding" PRIMARY KEY (id),
    CONSTRAINT "fk_funding_billing_type" FOREIGN KEY (billing_type_cd) REFERENCES aasbs.lu_billing_type(cd),
    CONSTRAINT "fk_funding_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_funding_fund_cat" FOREIGN KEY (fund_category_cd) REFERENCES aasbs.lu_fund_category(cd),
    CONSTRAINT "fk_funding_ia" FOREIGN KEY (ia_id) REFERENCES aasbs.ia(id),
    CONSTRAINT "fk_funding_latest_famd" FOREIGN KEY (latest_funding_amendment_id) REFERENCES aasbs.funding_amendment(id),
    CONSTRAINT "fk_funding_status" FOREIGN KEY (fund_status_cd) REFERENCES aasbs.lu_fund_status(cd),
    CONSTRAINT "fk_funding_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX fx_funding_iaid_fund_num2 ON aasbs.funding USING btree (upper((funding_num)::text));
CREATE INDEX ix_created_dt ON aasbs.funding USING btree (created_dt);
CREATE INDEX ix_funding_billing_type ON aasbs.funding USING btree (billing_type_cd);
CREATE INDEX ix_funding_cbui ON aasbs.funding USING btree (created_by_user_id);
CREATE INDEX ix_funding_fund_cat ON aasbs.funding USING btree (fund_category_cd);
CREATE INDEX ix_funding_fund_ia_id ON aasbs.funding USING btree (ia_id);
CREATE INDEX ix_funding_latest_famd ON aasbs.funding USING btree (latest_funding_amendment_id);
CREATE INDEX ix_funding_status ON aasbs.funding USING btree (fund_status_cd);
CREATE INDEX ix_funding_ubui ON aasbs.funding USING btree (updated_by_user_id);
CREATE UNIQUE INDEX ux_funding_iaid_fund_num ON aasbs.funding USING btree (ia_id, funding_num);

CREATE TRIGGER funding_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.funding FOR EACH ROW EXECUTE FUNCTION aasbs.trg_funding_hst();
CREATE TRIGGER trg_cclb_funding BEFORE DELETE ON aasbs.funding FOR EACH ROW EXECUTE FUNCTION aasbs."trg_cclb_funding$funding"();

COMMENT ON COLUMN "aasbs"."funding"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.FUNDING_SEQ';
COMMENT ON COLUMN "aasbs"."funding"."ia_id" IS 'Foreign Key to table AASBS.IA';
COMMENT ON COLUMN "aasbs"."funding"."fund_status_cd" IS 'Foreign Key to table AASBS.LU_FUND_STATUS';
COMMENT ON COLUMN "aasbs"."funding"."funding_num" IS 'Client funding document number';
COMMENT ON COLUMN "aasbs"."funding"."fund_category_cd" IS 'Foreign Key to table AASBS.LU_FUND_CATEGORY';
COMMENT ON COLUMN "aasbs"."funding"."billing_type_cd" IS 'Billing method (e.g., Standard, Advanced, Pre-Paid, etc)';
COMMENT ON COLUMN "aasbs"."funding"."latest_funding_amendment_id" IS 'Foreign Key to table AASBS.FUNDING_AMENDMENT to the last amendment or adjustment submitted - column is regularly updated';
COMMENT ON COLUMN "aasbs"."funding"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."funding"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."funding"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."funding"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."funding" IS 'Funding Package details - acts as the container for the LOA Amendments and LOAs';

-- ------------------------------------------------------------
-- Table: aasbs.funding_amendment
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."funding_amendment" (
    "id" bigint NOT NULL,
    "funding_id" bigint NOT NULL,
    "amend_num" integer NOT NULL,
    "adjustment_num" integer NOT NULL,
    "fund_amend_action_cd" character varying(10) NOT NULL,
    "fund_amend_status_cd" character varying(30) NOT NULL,
    "action_description" character varying(2000) NOT NULL,
    "requirement_description" character varying(2000) NOT NULL,
    "customer_signature_dt" timestamp(6) without time zone,
    "acceptance_dt" timestamp(6) without time zone NOT NULL,
    "refd_prev_funding_amendment_id" bigint,
    "attach_hash_id" character varying(64),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "ginv_pop_start_dt" timestamp(6) without time zone,
    "ginv_pop_end_dt" timestamp(6) without time zone,
    CONSTRAINT "pk_funding_amnd" PRIMARY KEY (id),
    CONSTRAINT "fk_fund_amnd_ref_fund_amend_id" FOREIGN KEY (refd_prev_funding_amendment_id) REFERENCES aasbs.funding_amendment(id),
    CONSTRAINT "fk_funding_amnd_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_funding_amnd_faac" FOREIGN KEY (fund_amend_action_cd) REFERENCES aasbs.lu_fund_amend_action(cd),
    CONSTRAINT "fk_funding_amnd_fasc" FOREIGN KEY (fund_amend_status_cd) REFERENCES aasbs.lu_fund_amend_status(cd),
    CONSTRAINT "fk_funding_amnd_funding" FOREIGN KEY (funding_id) REFERENCES aasbs.funding(id),
    CONSTRAINT "fk_funding_amnd_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_fund_amnd_ref_fund_amend_id ON aasbs.funding_amendment USING btree (refd_prev_funding_amendment_id);
CREATE INDEX ix_funding_amnd_cbui ON aasbs.funding_amendment USING btree (created_by_user_id);
CREATE INDEX ix_funding_amnd_fasc ON aasbs.funding_amendment USING btree (fund_amend_status_cd);
CREATE INDEX ix_funding_amnd_fid_amdn_adjn ON aasbs.funding_amendment USING btree (funding_id, amend_num, adjustment_num);
CREATE INDEX ix_funding_amnd_funding ON aasbs.funding_amendment USING btree (funding_id);
CREATE INDEX ix_funding_amnd_status_id ON aasbs.funding_amendment USING btree (fund_amend_status_cd, id);
CREATE INDEX ix_funding_amnd_support_aac ON aasbs.funding_amendment USING btree (fund_amend_action_cd);
CREATE INDEX ix_funding_amnd_ubui ON aasbs.funding_amendment USING btree (updated_by_user_id);

CREATE TRIGGER funding_amendment_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.funding_amendment FOR EACH ROW EXECUTE FUNCTION aasbs.trg_funding_amendment_hst();

COMMENT ON COLUMN "aasbs"."funding_amendment"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.FUNDING_AMENDMENT_SEQ';
COMMENT ON COLUMN "aasbs"."funding_amendment"."funding_id" IS 'Foreign Key to table AASBS.FUNDING';
COMMENT ON COLUMN "aasbs"."funding_amendment"."amend_num" IS 'Funding Amendment Number';
COMMENT ON COLUMN "aasbs"."funding_amendment"."adjustment_num" IS 'Funding Adjustment Number';
COMMENT ON COLUMN "aasbs"."funding_amendment"."fund_amend_action_cd" IS 'Foreign Key to table AASBS.LU_FUND_AMEND_ACTION';
COMMENT ON COLUMN "aasbs"."funding_amendment"."fund_amend_status_cd" IS 'Foreign Key to table AASBS.LU_FUND_AMEND_STATUS';
COMMENT ON COLUMN "aasbs"."funding_amendment"."action_description" IS 'User-entered Action Description of the event causing the amendment/adjustment';
COMMENT ON COLUMN "aasbs"."funding_amendment"."requirement_description" IS 'User-entered Requirement Description of the event causing the amendment/adjustment';
COMMENT ON COLUMN "aasbs"."funding_amendment"."customer_signature_dt" IS 'Date/time the Funding Amendment was received';
COMMENT ON COLUMN "aasbs"."funding_amendment"."acceptance_dt" IS 'Acceptance date of the Funding Amendment';
COMMENT ON COLUMN "aasbs"."funding_amendment"."refd_prev_funding_amendment_id" IS 'Foreign Key to table AASBS.FUNDING_AMENDMENT';
COMMENT ON COLUMN "aasbs"."funding_amendment"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."funding_amendment"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."funding_amendment"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."funding_amendment"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."funding_amendment"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."funding_amendment"."ginv_pop_start_dt" IS 'Period of Performance start date sourced from GInvoice';
COMMENT ON COLUMN "aasbs"."funding_amendment"."ginv_pop_end_dt" IS 'Period of Performance end date from GInvoice';

COMMENT ON TABLE "aasbs"."funding_amendment" IS 'Funding Amendment details';

-- ------------------------------------------------------------
-- Table: aasbs.funding_amendment_loa
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."funding_amendment_loa" (
    "id" bigint NOT NULL,
    "funding_amendment_id" bigint NOT NULL,
    "loa_id" bigint NOT NULL,
    "loa_change_amt" numeric(15,2) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_funding_amnd_loa" PRIMARY KEY (id),
    CONSTRAINT "fk_funding_amnd_loa_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_funding_amnd_loa_fund_amend" FOREIGN KEY (funding_amendment_id) REFERENCES aasbs.funding_amendment(id),
    CONSTRAINT "fk_funding_amnd_loa_loa" FOREIGN KEY (loa_id) REFERENCES aasbs.loa(id),
    CONSTRAINT "fk_funding_amnd_loa_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_funding_amnd_loa_cbui ON aasbs.funding_amendment_loa USING btree (created_by_user_id);
CREATE INDEX ix_funding_amnd_loa_fund_amend ON aasbs.funding_amendment_loa USING btree (funding_amendment_id);
CREATE INDEX ix_funding_amnd_loa_loa ON aasbs.funding_amendment_loa USING btree (loa_id);
CREATE INDEX ix_funding_amnd_loa_ubui ON aasbs.funding_amendment_loa USING btree (updated_by_user_id);

CREATE TRIGGER funding_amendment_loa_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.funding_amendment_loa FOR EACH ROW EXECUTE FUNCTION aasbs.trg_funding_amendment_loa_hst();

COMMENT ON COLUMN "aasbs"."funding_amendment_loa"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.FUNDING_AMENDMENT_LOA_SEQ';
COMMENT ON COLUMN "aasbs"."funding_amendment_loa"."funding_amendment_id" IS 'Foreign Key to table AASBS.FUNDING_AMENDMENT';
COMMENT ON COLUMN "aasbs"."funding_amendment_loa"."loa_id" IS 'Foreign Key to table AASBS.LOA';
COMMENT ON COLUMN "aasbs"."funding_amendment_loa"."loa_change_amt" IS 'The dollar amount of change to the referenced AASBS.LOA record';
COMMENT ON COLUMN "aasbs"."funding_amendment_loa"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."funding_amendment_loa"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."funding_amendment_loa"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."funding_amendment_loa"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."funding_amendment_loa" IS 'Join table allowing many to many relationship between the Funding_Amendment and LOA tables';

-- ------------------------------------------------------------
-- Table: aasbs.funding_responsible
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."funding_responsible" (
    "id" bigint NOT NULL,
    "funding_id" bigint NOT NULL,
    "user_id" character varying(16),
    "role_cd" character varying(30) NOT NULL,
    "is_primary_yn" character(1) NOT NULL,
    "full_name" character varying(100),
    "email" character varying(100),
    "phone" character varying(30),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_funding_resp" PRIMARY KEY (id),
    CONSTRAINT "ck_fund_resp_uid_name_email_ph" CHECK (user_id IS NOT NULL OR length(concat_ws(''::text, COALESCE(full_name, NULL::character varying), COALESCE(email, NULL::character varying), COALESCE(phone, NULL::character varying))) > 10) NOT VALID,
    CONSTRAINT "fk_funding_resp_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_funding_resp_fid" FOREIGN KEY (funding_id) REFERENCES aasbs.funding(id),
    CONSTRAINT "fk_funding_resp_rc" FOREIGN KEY (role_cd) REFERENCES aasbs.lu_responsible_role(cd),
    CONSTRAINT "fk_funding_resp_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_funding_resp_ui" FOREIGN KEY (user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_funding_resp_cbui ON aasbs.funding_responsible USING btree (created_by_user_id);
CREATE INDEX ix_funding_resp_fid ON aasbs.funding_responsible USING btree (funding_id);
CREATE INDEX ix_funding_resp_fid_role ON aasbs.funding_responsible USING btree (funding_id, role_cd);
CREATE INDEX ix_funding_resp_full_name_func ON aasbs.funding_responsible USING btree (lower(TRIM(BOTH FROM full_name)));
CREATE INDEX ix_funding_resp_rc ON aasbs.funding_responsible USING btree (role_cd);
CREATE INDEX ix_funding_resp_ubui ON aasbs.funding_responsible USING btree (updated_by_user_id);
CREATE INDEX ix_funding_resp_ui ON aasbs.funding_responsible USING btree (user_id);

CREATE TRIGGER funding_responsible_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.funding_responsible FOR EACH ROW EXECUTE FUNCTION aasbs.trg_funding_responsible_hst();

COMMENT ON COLUMN "aasbs"."funding_responsible"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.FUNDING_RESPONSIBLE_SEQ';
COMMENT ON COLUMN "aasbs"."funding_responsible"."funding_id" IS 'Foreign Key to table AASBS.FUNDING';
COMMENT ON COLUMN "aasbs"."funding_responsible"."user_id" IS 'Foreign Key to table ASSIST.USERS';
COMMENT ON COLUMN "aasbs"."funding_responsible"."role_cd" IS 'Foreign Key to table AASBS.LU_ROLE';
COMMENT ON COLUMN "aasbs"."funding_responsible"."is_primary_yn" IS 'Flag indicating whether a person associated with a Funding Package is the primary for their role (=Y)  or not (=N)';
COMMENT ON COLUMN "aasbs"."funding_responsible"."full_name" IS 'Free-text Full Name of Responsible User';
COMMENT ON COLUMN "aasbs"."funding_responsible"."email" IS 'Email of the Responsible person';
COMMENT ON COLUMN "aasbs"."funding_responsible"."phone" IS 'Phone Number';
COMMENT ON COLUMN "aasbs"."funding_responsible"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."funding_responsible"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."funding_responsible"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."funding_responsible"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."funding_responsible" IS 'Contains list of personnel associated with a Funding Package';

-- ------------------------------------------------------------
-- Table: aasbs.ia
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."ia" (
    "id" bigint NOT NULL,
    "piid" character varying(50) NOT NULL,
    "ia_status_cd" character varying(30) NOT NULL,
    "activity_address_cd" character varying(6) NOT NULL,
    "fund_cd" character varying(30) NOT NULL,
    "instrument_type_cd" character varying(3) NOT NULL,
    "title" character varying(200) NOT NULL,
    "description" character varying(4000) NOT NULL,
    "section_emp_org_id" bigint,
    "fiscal_year" integer NOT NULL,
    "ia_began_dt" timestamp(6) without time zone NOT NULL,
    "serv_agency_cd" character varying(30),
    "serv_bureau_cd" character varying(30),
    "piid_req_agency" character varying(50),
    "client_id" character varying(16),
    "legacy_ia_num" character varying(45),
    "omis_bas_seq_no" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "latest_ia_amendment_id" bigint,
    "gtc_num" character varying(20),
    "svcg_agmt_trkg_num" character varying(30),
    "emp_org_id" bigint,
    CONSTRAINT "pk_ia" PRIMARY KEY (id),
    CONSTRAINT "nn_ia_latest_ia_amendment_id" CHECK (latest_ia_amendment_id IS NOT NULL OR (ia_status_cd::text = ANY (ARRAY['DRAFT'::character varying::text, 'IN_PROCESS'::character varying::text]))) NOT VALID,
    CONSTRAINT "fk_ia_aac" FOREIGN KEY (activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd),
    CONSTRAINT "fk_ia_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_ia_cli" FOREIGN KEY (client_id) REFERENCES table_master.clients(client_id),
    CONSTRAINT "fk_ia_eoi" FOREIGN KEY (emp_org_id) REFERENCES assist.lu_emp_orgs(emp_org_id),
    CONSTRAINT "fk_ia_fund" FOREIGN KEY (fund_cd) REFERENCES aasbs.lu_fund(cd),
    CONSTRAINT "fk_ia_ia_amend" FOREIGN KEY (latest_ia_amendment_id) REFERENCES aasbs.ia_amendment(id),
    CONSTRAINT "fk_ia_ia_stat" FOREIGN KEY (ia_status_cd) REFERENCES aasbs.lu_ia_status(cd),
    CONSTRAINT "fk_ia_seoi" FOREIGN KEY (section_emp_org_id) REFERENCES assist.lu_emp_orgs(emp_org_id),
    CONSTRAINT "fk_ia_serv_agy" FOREIGN KEY (serv_agency_cd) REFERENCES aasbs.lu_agency(cd),
    CONSTRAINT "fk_ia_serv_bur" FOREIGN KEY (serv_bureau_cd, serv_agency_cd) REFERENCES aasbs.lu_bureau(cd, agency_cd),
    CONSTRAINT "fk_ia_type" FOREIGN KEY (instrument_type_cd) REFERENCES aasbs.lu_instrument_type(cd),
    CONSTRAINT "fk_ia_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX idx_ia_latest_ia_amend ON aasbs.ia USING btree (latest_ia_amendment_id);
CREATE INDEX ix_i_emp_org_id ON aasbs.ia USING btree (emp_org_id);
CREATE INDEX ix_ia_aac ON aasbs.ia USING btree (activity_address_cd);
CREATE INDEX ix_ia_cbui ON aasbs.ia USING btree (created_by_user_id);
CREATE INDEX ix_ia_cli ON aasbs.ia USING btree (client_id);
CREATE INDEX ix_ia_fund ON aasbs.ia USING btree (fund_cd);
CREATE UNIQUE INDEX ix_ia_gtcnum_aac ON aasbs.ia USING btree ((
CASE
    WHEN (((ia_status_cd)::text <> 'CLOSED'::text) AND (gtc_num IS NOT NULL)) THEN ((activity_address_cd)::text || (gtc_num)::text)
    ELSE NULL::text
END));
CREATE INDEX ix_ia_ia_stat ON aasbs.ia USING btree (ia_status_cd);
CREATE INDEX ix_ia_instr_type ON aasbs.ia USING btree (instrument_type_cd);
CREATE UNIQUE INDEX ix_ia_lian_aac ON aasbs.ia USING btree ((
CASE
    WHEN (legacy_ia_num IS NULL) THEN id
    ELSE NULL::bigint
END), (
CASE
    WHEN (legacy_ia_num IS NOT NULL) THEN ((activity_address_cd)::text || (legacy_ia_num)::text)
    ELSE NULL::text
END));
CREATE INDEX ix_ia_piid ON aasbs.ia USING btree (piid);
CREATE INDEX ix_ia_seoi ON aasbs.ia USING btree (section_emp_org_id);
CREATE INDEX ix_ia_serv_agy ON aasbs.ia USING btree (serv_agency_cd);
CREATE INDEX ix_ia_serv_bur ON aasbs.ia USING btree (serv_bureau_cd, serv_agency_cd);
CREATE INDEX ix_ia_ubui ON aasbs.ia USING btree (updated_by_user_id);
CREATE UNIQUE INDEX uk_ia_obsn ON aasbs.ia USING btree (omis_bas_seq_no);

CREATE TRIGGER ia_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.ia FOR EACH ROW EXECUTE FUNCTION aasbs.trg_ia_hst();
CREATE TRIGGER trg_cclb_ia BEFORE DELETE ON aasbs.ia FOR EACH ROW EXECUTE FUNCTION aasbs."trg_cclb_ia$ia"();

COMMENT ON COLUMN "aasbs"."ia"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.IA_SEQ';
COMMENT ON COLUMN "aasbs"."ia"."piid" IS 'IA procurement instrument identifier';
COMMENT ON COLUMN "aasbs"."ia"."ia_status_cd" IS 'IA Status - Foreign key to AASBS.LU_IA_STATUS.CD';
COMMENT ON COLUMN "aasbs"."ia"."activity_address_cd" IS 'Lead Servicing IA - Foreign key to AASBS.LU_ACTIVITY_ADDRESS_CODE.CD';
COMMENT ON COLUMN "aasbs"."ia"."fund_cd" IS 'Fund - Foreign key to AASBS.LU_FUND.CD';
COMMENT ON COLUMN "aasbs"."ia"."instrument_type_cd" IS 'IA Instrument Type - Foreign key to AASBS.LU_INSTRUMENT_TYPE.CD';
COMMENT ON COLUMN "aasbs"."ia"."title" IS 'Interagency Agreement Title';
COMMENT ON COLUMN "aasbs"."ia"."description" IS 'Description of the Interagecy Agreement';
COMMENT ON COLUMN "aasbs"."ia"."section_emp_org_id" IS 'This column is now used for all EMP ORGs, not just SECTIONs.  Foreign key to ASSIST.LU_EMP_ORGS.EMP_ORG_ID';
COMMENT ON COLUMN "aasbs"."ia"."fiscal_year" IS 'Fiscal Year of IA';
COMMENT ON COLUMN "aasbs"."ia"."ia_began_dt" IS 'Date IA process began';
COMMENT ON COLUMN "aasbs"."ia"."serv_agency_cd" IS 'Servicing Agency - Foreign key to AASBS.LU_AGENCY.CD';
COMMENT ON COLUMN "aasbs"."ia"."serv_bureau_cd" IS 'Servicing Bureau - Foreign key to AASBS.LU_BUREAU.CD and .AGENCY_CD';
COMMENT ON COLUMN "aasbs"."ia"."piid_req_agency" IS 'Requesting Agency IA Number';
COMMENT ON COLUMN "aasbs"."ia"."client_id" IS 'Requesting Agency Client Organization - Foreign Key to table TABLE_MASTER.CLIENTS.CLIENT_ID';
COMMENT ON COLUMN "aasbs"."ia"."legacy_ia_num" IS 'Legacy IA number.  Either migrated from OMIS.BAS.BA_ID or imported by RBA data call.';
COMMENT ON COLUMN "aasbs"."ia"."omis_bas_seq_no" IS 'For IAs which also exist in OMIS, foreign key to OMIS.BAS.SEQ_NO';
COMMENT ON COLUMN "aasbs"."ia"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."ia"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."ia"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."ia"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."ia"."latest_ia_amendment_id" IS 'Foreign Key to table AASBS.LATEST_IA_AMENDMENT';
COMMENT ON COLUMN "aasbs"."ia"."gtc_num" IS 'General Terms and Conditions GTC Number From G-INVOICING System';
COMMENT ON COLUMN "aasbs"."ia"."svcg_agmt_trkg_num" IS 'Servicing Agreement Tracking Number';
COMMENT ON COLUMN "aasbs"."ia"."emp_org_id" IS 'References ASSIST.LU_EMP_ORGS.EMP_ORG_ID';

COMMENT ON TABLE "aasbs"."ia" IS 'Interagency Agreements';

-- ------------------------------------------------------------
-- Table: aasbs.ia_amendment
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."ia_amendment" (
    "id" bigint NOT NULL,
    "ia_id" bigint NOT NULL,
    "piid_ext" character varying(6) NOT NULL,
    "ia_amend_action_cd" character varying(30) NOT NULL,
    "action_description" character varying(4000) NOT NULL,
    "start_dt" timestamp(6) without time zone NOT NULL,
    "end_dt" timestamp(6) without time zone NOT NULL,
    "direct_cost_est_amt" numeric(23,2) NOT NULL,
    "charges_est_amt" numeric(23,2) NOT NULL,
    "request_agency_scope" character varying(4000) NOT NULL,
    "service_agency_signer_id" character varying(16),
    "service_agency_signed_dt" timestamp(6) without time zone NOT NULL,
    "request_agency_signer" character varying(70) NOT NULL,
    "request_agency_signer_email" character varying(100) NOT NULL,
    "request_agency_signer_phone" character varying(40) NOT NULL,
    "request_agency_signed_dt" timestamp(6) without time zone NOT NULL,
    "attach_hash_id" character varying(64),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "gtc_service_agency_signer" character varying(100),
    "gtc_service_agency_signer_email" character varying(100),
    "gtc_service_agency_signer_phone" character varying(20),
    CONSTRAINT "pk_ia_amendment" PRIMARY KEY (id),
    CONSTRAINT "fk_ia_amd_act" FOREIGN KEY (ia_amend_action_cd) REFERENCES aasbs.lu_ia_amend_action(cd),
    CONSTRAINT "fk_ia_amd_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_ia_amd_ia" FOREIGN KEY (ia_id) REFERENCES aasbs.ia(id),
    CONSTRAINT "fk_ia_amd_sasi" FOREIGN KEY (service_agency_signer_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_ia_amd_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_ia_amd_act ON aasbs.ia_amendment USING btree (ia_amend_action_cd);
CREATE INDEX ix_ia_amd_cbui ON aasbs.ia_amendment USING btree (created_by_user_id);
CREATE INDEX ix_ia_amd_ia ON aasbs.ia_amendment USING btree (ia_id);
CREATE INDEX ix_ia_amd_sasi ON aasbs.ia_amendment USING btree (service_agency_signer_id);
CREATE INDEX ix_ia_amd_ubui ON aasbs.ia_amendment USING btree (updated_by_user_id);

CREATE TRIGGER ia_amendment_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.ia_amendment FOR EACH ROW EXECUTE FUNCTION aasbs.trg_ia_amendment_hst();

COMMENT ON COLUMN "aasbs"."ia_amendment"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.IA_AMENDMENT_SEQ';
COMMENT ON COLUMN "aasbs"."ia_amendment"."ia_id" IS 'Foreign Key to table AASBS.IA.ID';
COMMENT ON COLUMN "aasbs"."ia_amendment"."piid_ext" IS 'Amendment Number/PIID extension';
COMMENT ON COLUMN "aasbs"."ia_amendment"."ia_amend_action_cd" IS 'Amendment action - Foreign key to AASBS.LU_IA_AMEND_ACTION.CD';
COMMENT ON COLUMN "aasbs"."ia_amendment"."action_description" IS 'Description of the Amendment action';
COMMENT ON COLUMN "aasbs"."ia_amendment"."start_dt" IS 'Start Date for IA';
COMMENT ON COLUMN "aasbs"."ia_amendment"."end_dt" IS 'End Date for IA';
COMMENT ON COLUMN "aasbs"."ia_amendment"."direct_cost_est_amt" IS 'Estimated Direct Costs';
COMMENT ON COLUMN "aasbs"."ia_amendment"."charges_est_amt" IS 'Estimated Charges';
COMMENT ON COLUMN "aasbs"."ia_amendment"."request_agency_scope" IS 'Requesting Agency scope of work to be performed by the Servicing Agency';
COMMENT ON COLUMN "aasbs"."ia_amendment"."service_agency_signer_id" IS 'Servicing Agency Signer - Foreign Key to table AASBS.USER.ID';
COMMENT ON COLUMN "aasbs"."ia_amendment"."service_agency_signed_dt" IS 'Servicing Agency Date Signed';
COMMENT ON COLUMN "aasbs"."ia_amendment"."request_agency_signer" IS 'Requesting Agency Signer name';
COMMENT ON COLUMN "aasbs"."ia_amendment"."request_agency_signer_email" IS 'Requesting Agency Signer email';
COMMENT ON COLUMN "aasbs"."ia_amendment"."request_agency_signer_phone" IS 'Requesting Agency Signer phone number';
COMMENT ON COLUMN "aasbs"."ia_amendment"."request_agency_signed_dt" IS 'Requesting Agency Date Signed';
COMMENT ON COLUMN "aasbs"."ia_amendment"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."ia_amendment"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."ia_amendment"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."ia_amendment"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."ia_amendment"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."ia_amendment"."gtc_service_agency_signer" IS 'GTC Servicing Final Approval Signed Name From G-INVOICING System';
COMMENT ON COLUMN "aasbs"."ia_amendment"."gtc_service_agency_signer_email" IS 'GTC Servicing Final Approval Signed Email From G-INVOICING System';
COMMENT ON COLUMN "aasbs"."ia_amendment"."gtc_service_agency_signer_phone" IS 'GTC Servicing Final Approval Signed Phone From G-INVOICING System';

COMMENT ON TABLE "aasbs"."ia_amendment" IS 'Contains Amendments for an Interagency Agreement';

-- ------------------------------------------------------------
-- Table: aasbs.ia_responsible
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."ia_responsible" (
    "id" bigint NOT NULL,
    "ia_id" bigint NOT NULL,
    "user_id" character varying(16) NOT NULL,
    "responsible_role_cd" character varying(30) NOT NULL,
    "is_primary_yn" character(1) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_ia_responsible" PRIMARY KEY (id),
    CONSTRAINT "ck_ia_resp_is_primeyn" CHECK (is_primary_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_ia_resp_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_ia_resp_ia" FOREIGN KEY (ia_id) REFERENCES aasbs.ia(id),
    CONSTRAINT "fk_ia_resp_role_cd" FOREIGN KEY (responsible_role_cd) REFERENCES aasbs.lu_responsible_role(cd),
    CONSTRAINT "fk_ia_resp_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_ia_resp_user_id" FOREIGN KEY (user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_ia_resp_cbui ON aasbs.ia_responsible USING btree (created_by_user_id);
CREATE INDEX ix_ia_resp_ia ON aasbs.ia_responsible USING btree (ia_id);
CREATE INDEX ix_ia_resp_ipyn_rrc_ii ON aasbs.ia_responsible USING btree (is_primary_yn, responsible_role_cd, ia_id);
CREATE INDEX ix_ia_resp_role_cd ON aasbs.ia_responsible USING btree (responsible_role_cd);
CREATE INDEX ix_ia_resp_ubui ON aasbs.ia_responsible USING btree (updated_by_user_id);
CREATE INDEX ix_ia_resp_user_id ON aasbs.ia_responsible USING btree (user_id);

CREATE TRIGGER ia_responsible_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.ia_responsible FOR EACH ROW EXECUTE FUNCTION aasbs.trg_ia_responsible_hst();

COMMENT ON COLUMN "aasbs"."ia_responsible"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.IA_RESPONSIBLE_SEQ';
COMMENT ON COLUMN "aasbs"."ia_responsible"."ia_id" IS 'Foreign Key to table AASBS.IA.ID';
COMMENT ON COLUMN "aasbs"."ia_responsible"."user_id" IS 'Foreign Key to table AASBS.USER.ID';
COMMENT ON COLUMN "aasbs"."ia_responsible"."responsible_role_cd" IS 'Foreign Key to table AASBS.LU_RESPONSIBLE_ROLE.CD';
COMMENT ON COLUMN "aasbs"."ia_responsible"."is_primary_yn" IS 'Flag indicating whether a person associated with the IA is the primary for their role (=Y) or alternate (=N)';
COMMENT ON COLUMN "aasbs"."ia_responsible"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."ia_responsible"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."ia_responsible"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."ia_responsible"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."ia_responsible" IS 'Contains list of personnel associated with an Interagency Agreement';

-- ------------------------------------------------------------
-- Table: aasbs.ia_review
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."ia_review" (
    "id" bigint NOT NULL,
    "ia_id" bigint NOT NULL,
    "review_num" character varying(6) NOT NULL,
    "review_dt" timestamp(6) without time zone NOT NULL,
    "service_agency_reviewer_id" character varying(16) NOT NULL,
    "request_agency_reviewer" character varying(70) NOT NULL,
    "attach_hash_id" character varying(64),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_ia_review" PRIMARY KEY (id),
    CONSTRAINT "fk_ia_rev_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_ia_rev_ii" FOREIGN KEY (ia_id) REFERENCES aasbs.ia(id),
    CONSTRAINT "fk_ia_rev_sari" FOREIGN KEY (service_agency_reviewer_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_ia_rev_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_ia_rev_cbui ON aasbs.ia_review USING btree (created_by_user_id);
CREATE INDEX ix_ia_rev_ia ON aasbs.ia_review USING btree (ia_id);
CREATE INDEX ix_ia_rev_sasi ON aasbs.ia_review USING btree (service_agency_reviewer_id);
CREATE INDEX ix_ia_rev_ubui ON aasbs.ia_review USING btree (updated_by_user_id);

CREATE TRIGGER ia_review_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.ia_review FOR EACH ROW EXECUTE FUNCTION aasbs.trg_ia_review_hst();
CREATE TRIGGER trg_ccom_ia_review BEFORE DELETE ON aasbs.ia_review FOR EACH ROW EXECUTE FUNCTION aasbs."trg_ccom_ia_review$ia_review"();

COMMENT ON COLUMN "aasbs"."ia_review"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.IA_REVIEW_SEQ';
COMMENT ON COLUMN "aasbs"."ia_review"."ia_id" IS 'Foreign Key to table AASBS.IA.ID';
COMMENT ON COLUMN "aasbs"."ia_review"."review_num" IS 'Review Number';
COMMENT ON COLUMN "aasbs"."ia_review"."review_dt" IS 'Review Date for IA';
COMMENT ON COLUMN "aasbs"."ia_review"."service_agency_reviewer_id" IS 'Servicing Agency Reviewer - Foreign Key to table AASBS.USER.ID';
COMMENT ON COLUMN "aasbs"."ia_review"."request_agency_reviewer" IS 'Requesting Agency Reviewer name';
COMMENT ON COLUMN "aasbs"."ia_review"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."ia_review"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."ia_review"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."ia_review"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."ia_review"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."ia_review" IS 'Contains annual client reviews for an Interagency Agreement';

-- ------------------------------------------------------------
-- Table: aasbs.invoice
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."invoice" (
    "id" bigint NOT NULL,
    "invoice_number" character varying(30) NOT NULL,
    "award_id" bigint NOT NULL,
    "award_mod_id" bigint NOT NULL,
    "award_mod_company_id" bigint NOT NULL,
    "invoice_status_cd" character varying(35) NOT NULL,
    "total_invoice_amt" numeric(15,2) NOT NULL,
    "invoice_dt" timestamp(6) without time zone NOT NULL,
    "invoice_processed_dt" timestamp(6) without time zone,
    "invoice_net_days_cnt" numeric(19,2) NOT NULL,
    "invoice_discount_pct" numeric(6,3) NOT NULL,
    "invoice_discount_days_cnt" numeric(19,2) NOT NULL,
    "invoice_contractor_comments" character varying(2000),
    "vitap_payment_status_cd" character varying(20),
    "vitap_processed_dt" timestamp(6) without time zone,
    "vitap_payment_amt" numeric(15,2),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_invoice" PRIMARY KEY (id),
    CONSTRAINT "fk_invoice_awd_id" FOREIGN KEY (award_id) REFERENCES aasbs.award(id),
    CONSTRAINT "fk_invoice_awd_mod_company_id" FOREIGN KEY (award_mod_company_id) REFERENCES aasbs.award_mod_company(id),
    CONSTRAINT "fk_invoice_awd_mod_id" FOREIGN KEY (award_mod_id) REFERENCES aasbs.award_mod(id),
    CONSTRAINT "fk_invoice_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_invoice_invoice_status" FOREIGN KEY (invoice_status_cd) REFERENCES aasbs.lu_invoice_status(cd),
    CONSTRAINT "fk_invoice_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_invoice_vitap_pay_stat" FOREIGN KEY (vitap_payment_status_cd) REFERENCES aasbs.lu_vitap_payment_status(cd)
);

CREATE INDEX ix_invoice_awd_id ON aasbs.invoice USING btree (award_id);
CREATE INDEX ix_invoice_awd_mod_company_id ON aasbs.invoice USING btree (award_mod_company_id);
CREATE INDEX ix_invoice_awd_mod_id ON aasbs.invoice USING btree (award_mod_id);
CREATE INDEX ix_invoice_cbui ON aasbs.invoice USING btree (created_by_user_id);
CREATE INDEX ix_invoice_invoice_dt ON aasbs.invoice USING btree (invoice_dt);
CREATE INDEX ix_invoice_invoice_status ON aasbs.invoice USING btree (invoice_status_cd);
CREATE INDEX ix_invoice_ubui ON aasbs.invoice USING btree (updated_by_user_id);
CREATE INDEX ix_invoice_vitap_pay_stat ON aasbs.invoice USING btree (vitap_payment_status_cd);

CREATE TRIGGER invoice_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.invoice FOR EACH ROW EXECUTE FUNCTION aasbs.trg_invoice_hst();
CREATE TRIGGER trg_restrict_invoice BEFORE DELETE OR UPDATE ON aasbs.invoice FOR EACH ROW EXECUTE FUNCTION aasbs."trg_restrict_invoice$invoice"();

COMMENT ON COLUMN "aasbs"."invoice"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.INVOICE_SEQ';
COMMENT ON COLUMN "aasbs"."invoice"."invoice_number" IS 'Invoice Number';
COMMENT ON COLUMN "aasbs"."invoice"."award_id" IS 'Foreign Key to table AASBS.AWARD';
COMMENT ON COLUMN "aasbs"."invoice"."award_mod_id" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs"."invoice"."award_mod_company_id" IS 'Foreign Key to table AASBS.AWARD_MOD_COMPANY';
COMMENT ON COLUMN "aasbs"."invoice"."invoice_status_cd" IS 'Foreign key to AASBS.LU_INVOICE_STATUS';
COMMENT ON COLUMN "aasbs"."invoice"."total_invoice_amt" IS 'Combined total amount of all Invoice_Items under this Invoice';
COMMENT ON COLUMN "aasbs"."invoice"."invoice_dt" IS 'Date of Invoice submission';
COMMENT ON COLUMN "aasbs"."invoice"."invoice_processed_dt" IS 'Date the Invoice was processed';
COMMENT ON COLUMN "aasbs"."invoice"."invoice_net_days_cnt" IS 'Contractor-specified discount Net-Days';
COMMENT ON COLUMN "aasbs"."invoice"."invoice_discount_pct" IS 'Contractor-specified discount percentage if paid within specified number of days (INVOICE_DISCOUNT_DAYS_CNT column)';
COMMENT ON COLUMN "aasbs"."invoice"."invoice_discount_days_cnt" IS 'Contractor-specified number of days to achieve the discount percentage (INVOICE_DISCOUNT_PCT column)';
COMMENT ON COLUMN "aasbs"."invoice"."invoice_contractor_comments" IS 'Contractor-entered comments';
COMMENT ON COLUMN "aasbs"."invoice"."vitap_payment_status_cd" IS 'Foreign Key to table AASBS.LU_VITAP_PAYMENT_STATUS';
COMMENT ON COLUMN "aasbs"."invoice"."vitap_processed_dt" IS 'Date the VITAP payment was processed';
COMMENT ON COLUMN "aasbs"."invoice"."vitap_payment_amt" IS 'Amount of the VITAP payment';
COMMENT ON COLUMN "aasbs"."invoice"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."invoice"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."invoice"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."invoice"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."invoice" IS 'Contains high-level invoice details for all invoices within the ASSIST application';

-- ------------------------------------------------------------
-- Table: aasbs.invoice_connector
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."invoice_connector" (
    "id" bigint NOT NULL,
    "original_invoice_id" bigint NOT NULL,
    "correction_invoice_id" bigint NOT NULL,
    "invoice_correction_type_cd" character varying(20) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_invoice_connector" PRIMARY KEY (id),
    CONSTRAINT "fk_inv_crct_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_inv_crct_crct_invoice_id" FOREIGN KEY (correction_invoice_id) REFERENCES aasbs.invoice(id),
    CONSTRAINT "fk_inv_crct_inv_crct_type" FOREIGN KEY (invoice_correction_type_cd) REFERENCES aasbs.lu_invoice_correction_type(cd),
    CONSTRAINT "fk_inv_crct_orig_invoice_id" FOREIGN KEY (original_invoice_id) REFERENCES aasbs.invoice(id),
    CONSTRAINT "fk_inv_crct_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_inv_crct_cbui ON aasbs.invoice_connector USING btree (created_by_user_id);
CREATE INDEX ix_inv_crct_crct_invoice_id ON aasbs.invoice_connector USING btree (correction_invoice_id);
CREATE INDEX ix_inv_crct_inv_crct_type ON aasbs.invoice_connector USING btree (invoice_correction_type_cd);
CREATE INDEX ix_inv_crct_orig_invoice_id ON aasbs.invoice_connector USING btree (original_invoice_id);
CREATE INDEX ix_inv_crct_ubui ON aasbs.invoice_connector USING btree (updated_by_user_id);

CREATE TRIGGER invoice_connector_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.invoice_connector FOR EACH ROW EXECUTE FUNCTION aasbs.trg_invoice_connector_hst();

COMMENT ON COLUMN "aasbs"."invoice_connector"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.INVOICE_CONNECTOR_SEQ';
COMMENT ON COLUMN "aasbs"."invoice_connector"."original_invoice_id" IS 'Foreign Key to table AASBS.INVOICE';
COMMENT ON COLUMN "aasbs"."invoice_connector"."correction_invoice_id" IS 'Foreign Key to table AASBS.INVOICE';
COMMENT ON COLUMN "aasbs"."invoice_connector"."invoice_correction_type_cd" IS 'Foreign Key to table AASBS.LU_INVOICE_CORRECTION_TYPE';
COMMENT ON COLUMN "aasbs"."invoice_connector"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."invoice_connector"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."invoice_connector"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."invoice_connector"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."invoice_connector" IS 'For an Invoice corrected via Financial Services, this table links the correcting Invoice with the original Invoice';

-- ------------------------------------------------------------
-- Table: aasbs.invoice_item
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."invoice_item" (
    "id" bigint NOT NULL,
    "invoice_id" bigint NOT NULL,
    "cost_line_item_accepted_id" bigint NOT NULL,
    "invoice_item_begin_dt" timestamp(6) without time zone,
    "invoice_item_end_dt" timestamp(6) without time zone,
    "invoice_item_amt" numeric(15,2) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_invoice_item" PRIMARY KEY (id),
    CONSTRAINT "fk_invoice_cost_item_li_acpted" FOREIGN KEY (cost_line_item_accepted_id) REFERENCES aasbs.line_item_accepted(id),
    CONSTRAINT "fk_invoice_item_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_invoice_item_invoice_id" FOREIGN KEY (invoice_id) REFERENCES aasbs.invoice(id),
    CONSTRAINT "fk_invoice_item_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_invoice_item_cbui ON aasbs.invoice_item USING btree (created_by_user_id);
CREATE INDEX ix_invoice_item_invoice_id ON aasbs.invoice_item USING btree (invoice_id);
CREATE INDEX ix_invoice_item_li_accepted ON aasbs.invoice_item USING btree (cost_line_item_accepted_id);
CREATE INDEX ix_invoice_item_ubui ON aasbs.invoice_item USING btree (updated_by_user_id);

CREATE TRIGGER invoice_item_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.invoice_item FOR EACH ROW EXECUTE FUNCTION aasbs.trg_invoice_item_hst();

COMMENT ON COLUMN "aasbs"."invoice_item"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.INVOICE_ITEM_SEQ';
COMMENT ON COLUMN "aasbs"."invoice_item"."invoice_id" IS 'Foreign Key to table AASBS.INVOICE';
COMMENT ON COLUMN "aasbs"."invoice_item"."cost_line_item_accepted_id" IS 'Foreign Key to table AASBS.LINE_ITEM_ACCEPTED';
COMMENT ON COLUMN "aasbs"."invoice_item"."invoice_item_begin_dt" IS 'Invoiced Item "Begin Date"';
COMMENT ON COLUMN "aasbs"."invoice_item"."invoice_item_end_dt" IS 'Invoiced Item "End/Ship Date"';
COMMENT ON COLUMN "aasbs"."invoice_item"."invoice_item_amt" IS 'Amount billed against LOA_Ledger "Line_Item_Obligated_Amt" or "Service_Charge_Amt" column';
COMMENT ON COLUMN "aasbs"."invoice_item"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."invoice_item"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."invoice_item"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."invoice_item"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."invoice_item" IS 'Contains the one or more Line Items under a single Invoice';

-- ------------------------------------------------------------
-- Table: aasbs.li_award_fee
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."li_award_fee" (
    "id" bigint NOT NULL,
    "surcharge_rate_pct" numeric(19,16) NOT NULL,
    "fee_amt" numeric(15,2) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "fee_adj" numeric(15,2),
    CONSTRAINT "pk_li_award_fee" PRIMARY KEY (id),
    CONSTRAINT "ck_li_award_fee_mutual_exclusv" CHECK (fee_amt > 0::numeric AND surcharge_rate_pct = 0::numeric OR fee_amt = 0::numeric AND surcharge_rate_pct >= 0::numeric) NOT VALID,
    CONSTRAINT "fk_li_awd_fee_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_li_awd_fee_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_li_awd_fee_cbui ON aasbs.li_award_fee USING btree (created_by_user_id);
CREATE INDEX ix_li_awd_fee_ubui ON aasbs.li_award_fee USING btree (updated_by_user_id);

CREATE TRIGGER li_award_fee_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.li_award_fee FOR EACH ROW EXECUTE FUNCTION aasbs.trg_li_award_fee_hst();

COMMENT ON COLUMN "aasbs"."li_award_fee"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LI_AWARD_FEE_SEQ';
COMMENT ON COLUMN "aasbs"."li_award_fee"."surcharge_rate_pct" IS 'Percentage of Award to be charged for this Award Fee';
COMMENT ON COLUMN "aasbs"."li_award_fee"."fee_amt" IS 'Fixed Amount for this Award Fee';
COMMENT ON COLUMN "aasbs"."li_award_fee"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."li_award_fee"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."li_award_fee"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."li_award_fee"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."li_award_fee"."fee_adj" IS 'Fee adjustment amount used by business line to correct rounding discrepancies from fee surcharge line item calculation';

COMMENT ON TABLE "aasbs"."li_award_fee" IS 'Line Item extension table for Travel and Travel Tracking Details';

-- ------------------------------------------------------------
-- Table: aasbs.li_deliverable
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."li_deliverable" (
    "id" bigint NOT NULL,
    "psc_cd" character varying(20),
    "contract_type_cd" character varying(20),
    "acquisition_type_cd" character varying(30) NOT NULL,
    "severability_type_cd" character varying(20) NOT NULL,
    "unit_of_measure_cd" character varying(30) NOT NULL,
    "li_pop_address_id" bigint,
    "li_accepting_address_id" bigint,
    "required_yn" character(1) NOT NULL,
    "bona_fide_need_fy" integer,
    "draft_li_frm_yn" character(1) NOT NULL,
    "is_migrated_single_clin_yn" character(1),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_deliv" PRIMARY KEY (id),
    CONSTRAINT "ck_deliv_draft_li_frm_yn" CHECK (draft_li_frm_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_deliv_mig_single_clin_yn" CHECK (is_migrated_single_clin_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_deliv_reqd_yn" CHECK (required_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_deliv_acqtype" FOREIGN KEY (acquisition_type_cd) REFERENCES aasbs.lu_acquisition_type(cd),
    CONSTRAINT "fk_deliv_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_deliv_contrtyp" FOREIGN KEY (contract_type_cd) REFERENCES aasbs.lu_contract_type(cd),
    CONSTRAINT "fk_deliv_li_aaddr" FOREIGN KEY (li_accepting_address_id) REFERENCES aasbs.address(id),
    CONSTRAINT "fk_deliv_li_paddr" FOREIGN KEY (li_pop_address_id) REFERENCES aasbs.address(id),
    CONSTRAINT "fk_deliv_psc_code" FOREIGN KEY (psc_cd) REFERENCES aasbs.lus_psc(psc_code),
    CONSTRAINT "fk_deliv_sevtype" FOREIGN KEY (severability_type_cd) REFERENCES aasbs.lu_severability_type(cd),
    CONSTRAINT "fk_deliv_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_deliv_unit" FOREIGN KEY (unit_of_measure_cd) REFERENCES aasbs.lu_unit_of_measure(cd)
);

CREATE INDEX idx_lid_required_yn ON aasbs.li_deliverable USING btree (required_yn);
CREATE INDEX ix_deliv_acqtype ON aasbs.li_deliverable USING btree (acquisition_type_cd);
CREATE INDEX ix_deliv_cbui ON aasbs.li_deliverable USING btree (created_by_user_id);
CREATE INDEX ix_deliv_contrtyp ON aasbs.li_deliverable USING btree (contract_type_cd);
CREATE INDEX ix_deliv_li_aaddr ON aasbs.li_deliverable USING btree (li_accepting_address_id);
CREATE INDEX ix_deliv_li_paddr ON aasbs.li_deliverable USING btree (li_pop_address_id);
CREATE INDEX ix_deliv_psc_code ON aasbs.li_deliverable USING btree (psc_cd);
CREATE INDEX ix_deliv_sevtype ON aasbs.li_deliverable USING btree (severability_type_cd);
CREATE INDEX ix_deliv_ubui ON aasbs.li_deliverable USING btree (updated_by_user_id);
CREATE INDEX ix_deliv_unit_msr ON aasbs.li_deliverable USING btree (unit_of_measure_cd);

CREATE TRIGGER li_deliverable_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.li_deliverable FOR EACH ROW EXECUTE FUNCTION aasbs.trg_li_deliverable_hst();

COMMENT ON COLUMN "aasbs"."li_deliverable"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.DELIVERABLE_SEQ';
COMMENT ON COLUMN "aasbs"."li_deliverable"."psc_cd" IS 'Foreign Key to table AASBS.LUS_PSC';
COMMENT ON COLUMN "aasbs"."li_deliverable"."contract_type_cd" IS 'Foreign Key to table AASBS.LU_CONTRACT_TYPE';
COMMENT ON COLUMN "aasbs"."li_deliverable"."acquisition_type_cd" IS 'Foreign Key to table AASBS.LU_ACQUISITION_TYPE';
COMMENT ON COLUMN "aasbs"."li_deliverable"."severability_type_cd" IS 'Foreign Key to table AASBS.LU_SEVERABILITY_TYPE';
COMMENT ON COLUMN "aasbs"."li_deliverable"."unit_of_measure_cd" IS 'Foreign Key to table AASBS.LU_UNIT_OF_MEASURE';
COMMENT ON COLUMN "aasbs"."li_deliverable"."li_pop_address_id" IS 'Foreign Key to table AASBS.ADDRESS';
COMMENT ON COLUMN "aasbs"."li_deliverable"."li_accepting_address_id" IS 'Foreign Key to table AASBS.ADDRESS';
COMMENT ON COLUMN "aasbs"."li_deliverable"."required_yn" IS 'Flag indicating whether the Delivarble Line Item is required (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."li_deliverable"."bona_fide_need_fy" IS 'Fiscal year of the bona fide need';
COMMENT ON COLUMN "aasbs"."li_deliverable"."draft_li_frm_yn" IS 'Flag indicating whether Line Item Entry form is (=Y) or is not (=N) in DRAFT state';
COMMENT ON COLUMN "aasbs"."li_deliverable"."is_migrated_single_clin_yn" IS 'Indicates if this is was migrated as part of a single CLIN award (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."li_deliverable"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."li_deliverable"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."li_deliverable"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."li_deliverable"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."li_deliverable" IS 'Deliverables - an extension of the LINE_ITEM table for records of LI_DELIVERABLE line item type';

-- ------------------------------------------------------------
-- Table: aasbs.li_sc_fixed_fee
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."li_sc_fixed_fee" (
    "id" bigint NOT NULL,
    "manager_user_id" character varying(16) NOT NULL,
    "proc_phase_cd" character varying(15) NOT NULL,
    "project_num" character varying(15),
    "planned_amt" numeric(15,2) NOT NULL,
    "severability_type_cd" character varying(30) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_li_sc_fixed_fee" PRIMARY KEY (id),
    CONSTRAINT "ck_li_sc_ff_sev_type" CHECK (severability_type_cd::text = ANY (ARRAY['SEVERABLE'::character varying::text, 'NONSEVERABLE'::character varying::text])),
    CONSTRAINT "fk_li_sc_ff_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_li_sc_ff_mgr_userid" FOREIGN KEY (manager_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_li_sc_ff_proc_phase" FOREIGN KEY (proc_phase_cd) REFERENCES aasbs.lu_proc_phase(cd),
    CONSTRAINT "fk_li_sc_ff_sev_type" FOREIGN KEY (severability_type_cd) REFERENCES aasbs.lu_severability_type(cd),
    CONSTRAINT "fk_li_sc_ff_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_li_sc_ff_cbui ON aasbs.li_sc_fixed_fee USING btree (created_by_user_id);
CREATE INDEX ix_li_sc_ff_mgr_userid ON aasbs.li_sc_fixed_fee USING btree (manager_user_id);
CREATE INDEX ix_li_sc_ff_proc_phase ON aasbs.li_sc_fixed_fee USING btree (proc_phase_cd);
CREATE INDEX ix_li_sc_ff_proj_num ON aasbs.li_sc_fixed_fee USING btree (project_num);
CREATE INDEX ix_li_sc_ff_sev_type ON aasbs.li_sc_fixed_fee USING btree (severability_type_cd);
CREATE INDEX ix_li_sc_ff_ubui ON aasbs.li_sc_fixed_fee USING btree (updated_by_user_id);

CREATE TRIGGER li_sc_fixed_fee_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.li_sc_fixed_fee FOR EACH ROW EXECUTE FUNCTION aasbs.trg_li_sc_fixed_fee_hst();

COMMENT ON COLUMN "aasbs"."li_sc_fixed_fee"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LI_SC_FIXED_FEE_SEQ';
COMMENT ON COLUMN "aasbs"."li_sc_fixed_fee"."manager_user_id" IS 'Foreign Key to table ASSIST.USERS - Manager of the Fixed Fee Line Item';
COMMENT ON COLUMN "aasbs"."li_sc_fixed_fee"."proc_phase_cd" IS 'Foreign Key to table AASBS.LU_PROC_PHASE';
COMMENT ON COLUMN "aasbs"."li_sc_fixed_fee"."project_num" IS 'Account or PEP number, required for FEDSIM IAs';
COMMENT ON COLUMN "aasbs"."li_sc_fixed_fee"."planned_amt" IS 'Planned dollar amount for the Fixed Fee Line Item';
COMMENT ON COLUMN "aasbs"."li_sc_fixed_fee"."severability_type_cd" IS 'Foreign Key to table AASBS.LU_SEVERABILITY_TYPE.';
COMMENT ON COLUMN "aasbs"."li_sc_fixed_fee"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."li_sc_fixed_fee"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."li_sc_fixed_fee"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."li_sc_fixed_fee"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."li_sc_fixed_fee" IS 'Line Item extension table for Fixed Fee Details';

-- ------------------------------------------------------------
-- Table: aasbs.li_sc_labor
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."li_sc_labor" (
    "id" bigint NOT NULL,
    "manager_user_id" character varying(16) NOT NULL,
    "proc_phase_cd" character varying(15) NOT NULL,
    "project_num" character varying(15),
    "planned_amt" numeric(15,2) NOT NULL,
    "cost_rate_type_cd" character varying(30) NOT NULL,
    "severability_type_cd" character varying(30) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_li_sc_labor" PRIMARY KEY (id),
    CONSTRAINT "ck_li_sc_lab_sev_type" CHECK (severability_type_cd::text = 'NA'::text),
    CONSTRAINT "fk_li_sc_lab_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_li_sc_lab_cost_rate_type" FOREIGN KEY (cost_rate_type_cd) REFERENCES aasbs.lu_cost_rate_type(cd),
    CONSTRAINT "fk_li_sc_lab_mgr_userid" FOREIGN KEY (manager_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_li_sc_lab_proc_phase" FOREIGN KEY (proc_phase_cd) REFERENCES aasbs.lu_proc_phase(cd),
    CONSTRAINT "fk_li_sc_lab_sev_type" FOREIGN KEY (severability_type_cd) REFERENCES aasbs.lu_severability_type(cd),
    CONSTRAINT "fk_li_sc_lab_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_li_sc_lab_cbui ON aasbs.li_sc_labor USING btree (created_by_user_id);
CREATE INDEX ix_li_sc_lab_cost_rate_type ON aasbs.li_sc_labor USING btree (cost_rate_type_cd);
CREATE INDEX ix_li_sc_lab_mgr_userid ON aasbs.li_sc_labor USING btree (manager_user_id);
CREATE INDEX ix_li_sc_lab_proc_phase ON aasbs.li_sc_labor USING btree (proc_phase_cd);
CREATE INDEX ix_li_sc_lab_proj_num ON aasbs.li_sc_labor USING btree (project_num);
CREATE INDEX ix_li_sc_lab_sev_type ON aasbs.li_sc_labor USING btree (severability_type_cd);
CREATE INDEX ix_li_sc_lab_ubui ON aasbs.li_sc_labor USING btree (updated_by_user_id);

CREATE TRIGGER li_sc_labor_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.li_sc_labor FOR EACH ROW EXECUTE FUNCTION aasbs.trg_li_sc_labor_hst();

COMMENT ON COLUMN "aasbs"."li_sc_labor"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LI_SC_LABOR_SEQ';
COMMENT ON COLUMN "aasbs"."li_sc_labor"."manager_user_id" IS 'Foreign Key to table ASSIST.USERS - Manager of the Labor Service Charge Line Item';
COMMENT ON COLUMN "aasbs"."li_sc_labor"."proc_phase_cd" IS 'Foreign Key to table AASBS.LU_PROC_PHASE';
COMMENT ON COLUMN "aasbs"."li_sc_labor"."project_num" IS 'User chosen Project Number';
COMMENT ON COLUMN "aasbs"."li_sc_labor"."planned_amt" IS 'The planned funding amount for this Labor Service Charge';
COMMENT ON COLUMN "aasbs"."li_sc_labor"."cost_rate_type_cd" IS 'Foreign Key to table AASBS.LU_COST_RATE_TYPE';
COMMENT ON COLUMN "aasbs"."li_sc_labor"."severability_type_cd" IS 'Foreign Key to table AASBS.LU_SEVERABILITY_TYPE. (Always NA for Labor and Labor Tracking service charges.)';
COMMENT ON COLUMN "aasbs"."li_sc_labor"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."li_sc_labor"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."li_sc_labor"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."li_sc_labor"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."li_sc_labor" IS 'Line Item extension table for Labor and Labor Tracking Details';

-- ------------------------------------------------------------
-- Table: aasbs.li_sc_travel
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."li_sc_travel" (
    "id" bigint NOT NULL,
    "manager_user_id" character varying(16) NOT NULL,
    "proc_phase_cd" character varying(15) NOT NULL,
    "project_num" character varying(15),
    "planned_amt" numeric(15,2) NOT NULL,
    "severability_type_cd" character varying(30) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_li_sc_travel" PRIMARY KEY (id),
    CONSTRAINT "ck_li_sc_trv_sev_type" CHECK (severability_type_cd::text = ANY (ARRAY['SEVERABLE'::character varying::text, 'NONSEVERABLE'::character varying::text, 'NA'::character varying::text])),
    CONSTRAINT "fk_li_sc_trv_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_li_sc_trv_mgr_userid" FOREIGN KEY (manager_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_li_sc_trv_proc_phase" FOREIGN KEY (proc_phase_cd) REFERENCES aasbs.lu_proc_phase(cd),
    CONSTRAINT "fk_li_sc_trv_sev_type" FOREIGN KEY (severability_type_cd) REFERENCES aasbs.lu_severability_type(cd),
    CONSTRAINT "fk_li_sc_trv_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_li_sc_trv_cbui ON aasbs.li_sc_travel USING btree (created_by_user_id);
CREATE INDEX ix_li_sc_trv_mgr_userid ON aasbs.li_sc_travel USING btree (manager_user_id);
CREATE INDEX ix_li_sc_trv_proc_phase ON aasbs.li_sc_travel USING btree (proc_phase_cd);
CREATE INDEX ix_li_sc_trv_proj_num ON aasbs.li_sc_travel USING btree (project_num);
CREATE INDEX ix_li_sc_trv_sev_type ON aasbs.li_sc_travel USING btree (severability_type_cd);
CREATE INDEX ix_li_sc_trv_ubui ON aasbs.li_sc_travel USING btree (updated_by_user_id);

CREATE TRIGGER li_sc_travel_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.li_sc_travel FOR EACH ROW EXECUTE FUNCTION aasbs.trg_li_sc_travel_hst();

COMMENT ON COLUMN "aasbs"."li_sc_travel"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LI_SC_TRAVEL_SEQ';
COMMENT ON COLUMN "aasbs"."li_sc_travel"."manager_user_id" IS 'Foreign Key to table ASSIST.USERS - Manager of the Fixed Fee Line Item';
COMMENT ON COLUMN "aasbs"."li_sc_travel"."proc_phase_cd" IS 'Foreign Key to table AASBS.LU_PROC_PHASE';
COMMENT ON COLUMN "aasbs"."li_sc_travel"."project_num" IS 'Account or PEP number, required for FEDSIM IAs';
COMMENT ON COLUMN "aasbs"."li_sc_travel"."planned_amt" IS 'Planned dollar amount for the Fixed Fee Line Item';
COMMENT ON COLUMN "aasbs"."li_sc_travel"."severability_type_cd" IS 'Foreign Key to table AASBS.LU_SEVERABILITY_TYPE. (Always NA for Travel Tracking service charges.)';
COMMENT ON COLUMN "aasbs"."li_sc_travel"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."li_sc_travel"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."li_sc_travel"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."li_sc_travel"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."li_sc_travel" IS 'Line Item extension table for Travel and Travel Tracking Details';

-- ------------------------------------------------------------
-- Table: aasbs.line_item
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."line_item" (
    "id" bigint NOT NULL,
    "line_item_type_cd" character varying(30) NOT NULL,
    "line_item_num" character varying(16) NOT NULL,
    "quantity" numeric(10,2) NOT NULL,
    "line_item_unit_amt" numeric(15,2) NOT NULL,
    "line_item_total_amt" numeric(15,2) NOT NULL,
    "line_item_start_dt" timestamp(6) without time zone NOT NULL,
    "line_item_end_dt" timestamp(6) without time zone NOT NULL,
    "service_charge_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "li_tracking_num" bigint,
    "title" character varying(500) NOT NULL,
    "description" character varying(2000),
    "line_item_status_cd" character varying(25),
    "project_id" bigint,
    "prev_line_item_id" bigint,
    "parent_ia_id" bigint,
    "parent_award_mod_id" bigint,
    "parent_solicit_amendment_id" bigint,
    "exercised_1st_in_award_mod_id" bigint,
    "is_base_deliverable_yn" character(1),
    "line_item_fy" integer,
    "li_deliverable_id" bigint,
    "li_sc_fixed_fee_id" bigint,
    "li_sc_labor_id" bigint,
    "li_sc_travel_id" bigint,
    "li_award_fee_id" bigint,
    "ceiling_change_amt" numeric(15,2) NOT NULL,
    "ceiling_prior_amt" numeric(15,2) NOT NULL,
    "planned_funded_change_amt" numeric(15,2) NOT NULL,
    "planned_funded_prior_amt" numeric(15,2) NOT NULL,
    "solicit_line_item_id" bigint,
    CONSTRAINT "pk_line_item" PRIMARY KEY (id),
    CONSTRAINT "ck_line_item_description_check" CHECK ((line_item_type_cd::text = ANY (ARRAY['LI_AWARD_OBLIGATION_PCT'::character varying::text, 'LI_AWARD_SC_FF_TYPE'::character varying::text, 'LI_AWARD_SURCHG_PCT'::character varying::text, 'LI_IA_SC_ON_DEMAND'::character varying::text])) AND description IS NOT NULL OR (line_item_type_cd::text <> ALL (ARRAY['LI_AWARD_OBLIGATION_PCT'::character varying::text, 'LI_AWARD_SC_FF_TYPE'::character varying::text, 'LI_AWARD_SURCHG_PCT'::character varying::text, 'LI_IA_SC_ON_DEMAND'::character varying::text]))),
    CONSTRAINT "ck_line_item_mutual_exclsv_fks" CHECK ((
CASE
    WHEN service_charge_id::character varying IS NULL THEN 0
    ELSE 1
END +
CASE
    WHEN li_sc_fixed_fee_id::character varying IS NULL THEN 0
    ELSE 1
END +
CASE
    WHEN li_sc_labor_id::character varying IS NULL THEN 0
    ELSE 1
END +
CASE
    WHEN li_sc_travel_id::character varying IS NULL THEN 0
    ELSE 1
END +
CASE
    WHEN li_deliverable_id::character varying IS NULL THEN 0
    ELSE 1
END +
CASE
    WHEN li_award_fee_id::character varying IS NULL THEN 0
    ELSE 1
END) = 1),
    CONSTRAINT "ck_line_item_sc_quantity" CHECK ((line_item_type_cd::text <> ALL (ARRAY['LI_AWARD_DELIV'::character varying::text, 'LI_SOL_DELIV_TYPE'::character varying::text])) AND quantity = 1::numeric OR (line_item_type_cd::text = ANY (ARRAY['LI_AWARD_DELIV'::character varying::text, 'LI_SOL_DELIV_TYPE'::character varying::text]))),
    CONSTRAINT "ck_line_item_type_2_fk_col_chk" CHECK (line_item_type_cd::text = 'LI_AWARD_DELIV'::text AND li_deliverable_id IS NOT NULL OR line_item_type_cd::text = 'LI_AWARD_OBLIGATION_PCT'::text AND li_award_fee_id IS NOT NULL OR line_item_type_cd::text = 'LI_AWARD_SC_FF_TYPE'::text AND li_award_fee_id IS NOT NULL OR line_item_type_cd::text = 'LI_AWARD_SURCHG_PCT'::text AND li_award_fee_id IS NOT NULL OR line_item_type_cd::text = 'LI_IA_SC_FIXED_FEE'::text AND li_sc_fixed_fee_id IS NOT NULL OR line_item_type_cd::text = 'LI_IA_SC_LABOR'::text AND li_sc_labor_id IS NOT NULL OR line_item_type_cd::text = 'LI_IA_SC_LABOR_TRACKING'::text AND li_sc_labor_id IS NOT NULL OR line_item_type_cd::text = 'LI_IA_SC_ON_DEMAND'::text AND service_charge_id IS NOT NULL OR line_item_type_cd::text = 'LI_IA_SC_TRAVEL'::text AND li_sc_travel_id IS NOT NULL OR line_item_type_cd::text = 'LI_IA_SC_TRAVEL_TRACKING'::text AND li_sc_travel_id IS NOT NULL OR line_item_type_cd::text = 'LI_SOL_DELIV_TYPE'::text AND li_deliverable_id IS NOT NULL),
    CONSTRAINT "ck_line_item_unit_amt_tot_amt" CHECK ((line_item_type_cd::text <> ALL (ARRAY['LI_SOL_DELIV_TYPE'::character varying::text, 'LI_AWARD_DELIV'::character varying::text])) AND line_item_unit_amt = line_item_total_amt OR (line_item_type_cd::text = ANY (ARRAY['LI_SOL_DELIV_TYPE'::character varying::text, 'LI_AWARD_DELIV'::character varying::text]))),
    CONSTRAINT "nn_is_base_deliv_yn" CHECK (is_base_deliverable_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_li_solicit_li_id" FOREIGN KEY (solicit_line_item_id) REFERENCES aasbs.line_item(id),
    CONSTRAINT "fk_line_item_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_line_item_li_award_fee_id" FOREIGN KEY (li_award_fee_id) REFERENCES aasbs.li_award_fee(id),
    CONSTRAINT "fk_line_item_li_deliverable" FOREIGN KEY (li_deliverable_id) REFERENCES aasbs.li_deliverable(id),
    CONSTRAINT "fk_line_item_li_sc_fixed_fee" FOREIGN KEY (li_sc_fixed_fee_id) REFERENCES aasbs.li_sc_fixed_fee(id),
    CONSTRAINT "fk_line_item_li_sc_labor_id" FOREIGN KEY (li_sc_labor_id) REFERENCES aasbs.li_sc_labor(id),
    CONSTRAINT "fk_line_item_li_sc_travel_id" FOREIGN KEY (li_sc_travel_id) REFERENCES aasbs.li_sc_travel(id),
    CONSTRAINT "fk_line_item_li_status" FOREIGN KEY (line_item_status_cd) REFERENCES aasbs.lu_line_item_status(cd),
    CONSTRAINT "fk_line_item_parent_ia" FOREIGN KEY (parent_ia_id) REFERENCES aasbs.ia(id),
    CONSTRAINT "fk_line_item_prev_li_id" FOREIGN KEY (prev_line_item_id) REFERENCES aasbs.line_item(id),
    CONSTRAINT "fk_line_item_project_id" FOREIGN KEY (project_id) REFERENCES aasbs.project(id),
    CONSTRAINT "fk_line_item_solamnd" FOREIGN KEY (parent_solicit_amendment_id) REFERENCES aasbs.solicit_amendment(id),
    CONSTRAINT "fk_line_item_svc_chg" FOREIGN KEY (service_charge_id) REFERENCES aasbs.service_charge(id),
    CONSTRAINT "fk_line_item_type" FOREIGN KEY (line_item_type_cd) REFERENCES aasbs.lu_line_item_type(cd),
    CONSTRAINT "fk_line_item_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX idx_line_item_id_parent_li_track_num ON aasbs.line_item USING btree (line_item_num);
CREATE INDEX idx_line_item_li_tracking_num ON aasbs.line_item USING btree (li_tracking_num);
CREATE INDEX ix_li_solicit_li_id ON aasbs.line_item USING btree (solicit_line_item_id);
CREATE INDEX ix_line_item_cbui ON aasbs.line_item USING btree (created_by_user_id);
CREATE INDEX ix_line_item_delivid ON aasbs.line_item USING btree (li_deliverable_id);
CREATE INDEX ix_line_item_exczd_awd_mod_id ON aasbs.line_item USING btree (exercised_1st_in_award_mod_id);
CREATE INDEX ix_line_item_li_award_fee_id ON aasbs.line_item USING btree (li_award_fee_id);
CREATE INDEX ix_line_item_li_status ON aasbs.line_item USING btree (line_item_status_cd);
CREATE INDEX ix_line_item_li_tracknum ON aasbs.line_item USING btree (li_tracking_num);
CREATE INDEX ix_line_item_multi_col_fy_cd_ia ON aasbs.line_item USING btree (line_item_fy, line_item_type_cd);
COMMENT ON INDEX "ix_line_item_multi_col_fy_cd_ia" IS 'Created for Postgres due to time keeping main page not loading fast';
CREATE INDEX ix_line_item_parent_awd_mod ON aasbs.line_item USING btree (parent_award_mod_id);
CREATE INDEX ix_line_item_parent_ia ON aasbs.line_item USING btree (parent_ia_id);
CREATE INDEX ix_line_item_parent_sol_amend ON aasbs.line_item USING btree (parent_solicit_amendment_id);
CREATE INDEX ix_line_item_prev_li_id ON aasbs.line_item USING btree (prev_line_item_id);
CREATE INDEX ix_line_item_project_id ON aasbs.line_item USING btree (project_id);
CREATE INDEX ix_line_item_scffid ON aasbs.line_item USING btree (li_sc_fixed_fee_id);
CREATE INDEX ix_line_item_sclabid ON aasbs.line_item USING btree (li_sc_labor_id);
CREATE INDEX ix_line_item_sctravid ON aasbs.line_item USING btree (li_sc_travel_id);
CREATE INDEX ix_line_item_svc_chg ON aasbs.line_item USING btree (service_charge_id);
CREATE INDEX ix_line_item_type ON aasbs.line_item USING btree (line_item_type_cd);
CREATE INDEX ix_line_item_ubui ON aasbs.line_item USING btree (updated_by_user_id);

CREATE TRIGGER line_item_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.line_item FOR EACH ROW EXECUTE FUNCTION aasbs.trg_line_item_hst();

COMMENT ON COLUMN "aasbs"."line_item"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LINE_ITEM_SEQ';
COMMENT ON COLUMN "aasbs"."line_item"."line_item_type_cd" IS 'Foreign Key to table AASBS.LU_LINE_ITEM_TYPE';
COMMENT ON COLUMN "aasbs"."line_item"."line_item_num" IS 'Line Item Number uniquely identifies a Line Item within a Solicitation / Award';
COMMENT ON COLUMN "aasbs"."line_item"."quantity" IS 'Quantity of items (units) on this Line Item';
COMMENT ON COLUMN "aasbs"."line_item"."line_item_unit_amt" IS 'Dollar amount per item (unit) on this Line Item';
COMMENT ON COLUMN "aasbs"."line_item"."line_item_total_amt" IS 'Total dollar amount for this Line Item = quantity x item (unit) amount';
COMMENT ON COLUMN "aasbs"."line_item"."line_item_start_dt" IS 'Date at which a Line Item's window of deliveries / services are to start';
COMMENT ON COLUMN "aasbs"."line_item"."line_item_end_dt" IS 'Date by which a Line Item's window of deliveries / services are to end';
COMMENT ON COLUMN "aasbs"."line_item"."service_charge_id" IS 'Foreign Key to table AASBS.SERVICE_CHARGE';
COMMENT ON COLUMN "aasbs"."line_item"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."line_item"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."line_item"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."line_item"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."line_item"."li_tracking_num" IS 'Identifier of a real-world "Line Item".  It is the ID of the first record (/ version) in LINE_ITEM table representing the real-world "Line Item"';
COMMENT ON COLUMN "aasbs"."line_item"."title" IS 'Title of Line Item';
COMMENT ON COLUMN "aasbs"."line_item"."description" IS 'Description of Line Item';
COMMENT ON COLUMN "aasbs"."line_item"."line_item_status_cd" IS 'Foreign Key to table AASBS.LU_LINE_ITEM_STATUS';
COMMENT ON COLUMN "aasbs"."line_item"."project_id" IS 'Foreign Key to table AASBS.PROJECT - The PROJECT to which this Line Item is associated';
COMMENT ON COLUMN "aasbs"."line_item"."prev_line_item_id" IS 'Foreign Key to table AASBS.PREV_LINE_ITEM';
COMMENT ON COLUMN "aasbs"."line_item"."parent_ia_id" IS 'Foreign Key to table AASBS.IA - The IA to which this Line Item is associated';
COMMENT ON COLUMN "aasbs"."line_item"."parent_award_mod_id" IS 'Foreign Key to table AASBS.AWARD_MOD - The Award Mod to which this Line Item is associated';
COMMENT ON COLUMN "aasbs"."line_item"."parent_solicit_amendment_id" IS 'Foreign Key to table AASBS.SOLICIT_AMENDMENT - The Solicitation Amendment to which this Line Item is associated';
COMMENT ON COLUMN "aasbs"."line_item"."exercised_1st_in_award_mod_id" IS 'Foreign Key to table AASBS.EXERCISED_1ST_IN_AWARD_MOD';
COMMENT ON COLUMN "aasbs"."line_item"."is_base_deliverable_yn" IS 'Flag indicating whether the Line Item Deliverable was/is in the Base Mod (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."line_item"."line_item_fy" IS 'Line Item Fiscal Year';
COMMENT ON COLUMN "aasbs"."line_item"."li_deliverable_id" IS 'Foreign Key to table AASBS.LI_DELIVERABLE';
COMMENT ON COLUMN "aasbs"."line_item"."li_sc_fixed_fee_id" IS 'Foreign Key to table AASBS.LI_SC_FIXED_FEE';
COMMENT ON COLUMN "aasbs"."line_item"."li_sc_labor_id" IS 'Foreign Key to table AASBS.LI_SC_LABOR';
COMMENT ON COLUMN "aasbs"."line_item"."li_sc_travel_id" IS 'Foreign Key to table AASBS.LI_SC_TRAVEL';
COMMENT ON COLUMN "aasbs"."line_item"."li_award_fee_id" IS 'Foreign Key to table AASBS.LI_AWARD_FEE';
COMMENT ON COLUMN "aasbs"."line_item"."ceiling_change_amt" IS 'Ceiling Amount change since previous Mod';
COMMENT ON COLUMN "aasbs"."line_item"."ceiling_prior_amt" IS 'Prior Maximum Funding Amount for this Line Item';
COMMENT ON COLUMN "aasbs"."line_item"."planned_funded_change_amt" IS 'The planned funding amount change for this Line Item';
COMMENT ON COLUMN "aasbs"."line_item"."planned_funded_prior_amt" IS 'The previous planned funding amount for the previous version of this Line Item';
COMMENT ON COLUMN "aasbs"."line_item"."solicit_line_item_id" IS 'Foreign Key to table AASBS_DOCUMENTS.LINE_ITEM representing the origin of line item for those that started in Solictiation';

COMMENT ON TABLE "aasbs"."line_item" IS 'Line Items main table - uses extension tables Service_Charge, etc.';

-- ------------------------------------------------------------
-- Table: aasbs.line_item_accepted
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."line_item_accepted" (
    "id" bigint NOT NULL,
    "line_item_id" bigint NOT NULL,
    "li_tracking_num" bigint,
    "award_mod_id" bigint,
    "sc_fin" character varying(8),
    "budget_fy" integer,
    "exercise_this_yn" character(1) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "commitment_note" character varying(2000),
    CONSTRAINT "pk_line_item_accepted" PRIMARY KEY (id),
    CONSTRAINT "ck_li_accepted_exercise_yn" CHECK (exercise_this_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_li_accepted_awd_mod_id" FOREIGN KEY (award_mod_id) REFERENCES aasbs.award_mod(id),
    CONSTRAINT "fk_li_accepted_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_li_accepted_li_id" FOREIGN KEY (line_item_id) REFERENCES aasbs.line_item(id),
    CONSTRAINT "fk_li_accepted_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_li_accepted_awd_mod_id ON aasbs.line_item_accepted USING btree (award_mod_id);
CREATE INDEX ix_li_accepted_cbui ON aasbs.line_item_accepted USING btree (created_by_user_id);
CREATE INDEX ix_li_accepted_li_track_num ON aasbs.line_item_accepted USING btree (li_tracking_num);
CREATE INDEX ix_li_accepted_sc_fin ON aasbs.line_item_accepted USING btree (sc_fin);
CREATE INDEX ix_li_accepted_ubui ON aasbs.line_item_accepted USING btree (updated_by_user_id);
CREATE UNIQUE INDEX uk_li_accept_line_item_id ON aasbs.line_item_accepted USING btree (line_item_id);

CREATE TRIGGER line_item_accepted_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.line_item_accepted FOR EACH ROW EXECUTE FUNCTION aasbs.trg_line_item_accepted_hst();

COMMENT ON COLUMN "aasbs"."line_item_accepted"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LINE_ITEM_SEQ';
COMMENT ON COLUMN "aasbs"."line_item_accepted"."line_item_id" IS 'Foreign Key to table AASBS.LINE_ITEM';
COMMENT ON COLUMN "aasbs"."line_item_accepted"."li_tracking_num" IS 'Identifier of a real-world "Line Item".  It is the ID of the first record (/ version) in LINE_ITEM table representing the real-world "Line Item"';
COMMENT ON COLUMN "aasbs"."line_item_accepted"."award_mod_id" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs"."line_item_accepted"."sc_fin" IS 'Financial Identification Number - for Service Charges Only (SC_FIN = Service Charge FIN) - see AWARD table for AWARD_FIN';
COMMENT ON COLUMN "aasbs"."line_item_accepted"."budget_fy" IS 'Budget fiscal year';
COMMENT ON COLUMN "aasbs"."line_item_accepted"."exercise_this_yn" IS 'Flag indicating whether the Line Item must be included in the Award and Funded (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."line_item_accepted"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."line_item_accepted"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."line_item_accepted"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."line_item_accepted"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."line_item_accepted"."commitment_note" IS 'User note created while "Committing" LOA funds to a Line Item';

COMMENT ON TABLE "aasbs"."line_item_accepted" IS 'Line Items ACCEPTED table - Line Items must be in this table to be accepted by an LOA in the LOA_LEDGER table.';

-- ------------------------------------------------------------
-- Table: aasbs.line_item_connector
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."line_item_connector" (
    "id" bigint NOT NULL,
    "from_line_item_id" bigint,
    "to_line_item_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_li_connect" PRIMARY KEY (id),
    CONSTRAINT "nn_li_connect_from_li_id" CHECK (from_line_item_id IS NOT NULL),
    CONSTRAINT "nn_li_connect_to_li_id" CHECK (to_line_item_id IS NOT NULL),
    CONSTRAINT "fk_li_connect_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_li_connect_from_li_id" FOREIGN KEY (from_line_item_id) REFERENCES aasbs.line_item(id),
    CONSTRAINT "fk_li_connect_to_li_id" FOREIGN KEY (to_line_item_id) REFERENCES aasbs.line_item(id),
    CONSTRAINT "fk_li_connect_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_li_connect_cbui ON aasbs.line_item_connector USING btree (created_by_user_id);
CREATE INDEX ix_li_connect_from_to ON aasbs.line_item_connector USING btree (from_line_item_id, to_line_item_id);
CREATE INDEX ix_li_connect_li_id ON aasbs.line_item_connector USING btree (from_line_item_id);
CREATE INDEX ix_li_connect_to_from ON aasbs.line_item_connector USING btree (to_line_item_id, from_line_item_id);
CREATE INDEX ix_li_connect_to_li_id ON aasbs.line_item_connector USING btree (to_line_item_id);
CREATE INDEX ix_li_connect_ubui ON aasbs.line_item_connector USING btree (updated_by_user_id);

CREATE TRIGGER line_item_connector_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.line_item_connector FOR EACH ROW EXECUTE FUNCTION aasbs.trg_line_item_connector_hst();

COMMENT ON COLUMN "aasbs"."line_item_connector"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LINE_ITEM_CONNECTOR_SEQ';
COMMENT ON COLUMN "aasbs"."line_item_connector"."from_line_item_id" IS 'Foreign Key to table AASBS.LINE_ITEM';
COMMENT ON COLUMN "aasbs"."line_item_connector"."to_line_item_id" IS 'Foreign Key to table AASBS.LINE_ITEM';
COMMENT ON COLUMN "aasbs"."line_item_connector"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."line_item_connector"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."line_item_connector"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."line_item_connector"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."line_item_connector" IS 'This table links Line Items to other Line Items.  E.g., Deliverable to it''s related "% of cost" Service Charge';

-- ------------------------------------------------------------
-- Table: aasbs.line_item_response
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."line_item_response" (
    "id" bigint NOT NULL,
    "line_item_id" bigint NOT NULL,
    "solicit_response_id" bigint NOT NULL,
    "response_quantity" numeric(10,2) NOT NULL,
    "response_line_item_unit_amt" numeric(15,2) NOT NULL,
    "response_line_item_total_amt" numeric(15,2) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_line_item_response" PRIMARY KEY (id),
    CONSTRAINT "fk_line_item_resp_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_line_item_resp_lineitem_id" FOREIGN KEY (line_item_id) REFERENCES aasbs.line_item(id),
    CONSTRAINT "fk_line_item_resp_solresp_id" FOREIGN KEY (solicit_response_id) REFERENCES aasbs.solicit_response(id),
    CONSTRAINT "fk_line_item_resp_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_line_item_resp_cbui ON aasbs.line_item_response USING btree (created_by_user_id);
CREATE INDEX ix_line_item_resp_lineitem_id ON aasbs.line_item_response USING btree (line_item_id);
CREATE INDEX ix_line_item_resp_solresp_id ON aasbs.line_item_response USING btree (solicit_response_id);
CREATE INDEX ix_line_item_resp_ubui ON aasbs.line_item_response USING btree (updated_by_user_id);

CREATE TRIGGER line_item_response_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.line_item_response FOR EACH ROW EXECUTE FUNCTION aasbs.trg_line_item_response_hst();

COMMENT ON COLUMN "aasbs"."line_item_response"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LINE_ITEM_RESPONSE_SEQ';
COMMENT ON COLUMN "aasbs"."line_item_response"."line_item_id" IS 'Foreign Key to table AASBS.LINE_ITEM';
COMMENT ON COLUMN "aasbs"."line_item_response"."solicit_response_id" IS 'Foreign Key to table AASBS.SOLICIT_RESPONSE';
COMMENT ON COLUMN "aasbs"."line_item_response"."response_quantity" IS 'Quantity of items (units) on this Line Item';
COMMENT ON COLUMN "aasbs"."line_item_response"."response_line_item_unit_amt" IS 'Dollar amount per item (unit) on this Line Item';
COMMENT ON COLUMN "aasbs"."line_item_response"."response_line_item_total_amt" IS 'Total dollar amount for this Line Item = quantity x item (unit) amount';
COMMENT ON COLUMN "aasbs"."line_item_response"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."line_item_response"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."line_item_response"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."line_item_response"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."line_item_response" IS 'Line Items Responses from each Contractor to the Line Items in aasbs.LINE_ITEM created during a Solicitation Response';

-- ------------------------------------------------------------
-- Table: aasbs.loa
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."loa" (
    "id" bigint NOT NULL,
    "loa_status_cd" character varying(30) NOT NULL,
    "creator_funding_amendment_id" bigint NOT NULL,
    "agreement_number" character varying(20),
    "agreement_end_dt" timestamp(6) without time zone,
    "transmitted_dt" timestamp(6) without time zone,
    "agreement_accepted_yn" character(1) NOT NULL,
    "agreement_converted_yn" character(1) NOT NULL,
    "legacy_sync_status_cd" character varying(30),
    "legacy_sync_dt" timestamp(6) without time zone,
    "tracking_num" character varying(20) NOT NULL,
    "line_of_accounting" character varying(115) NOT NULL,
    "program_cd" character varying(10) NOT NULL,
    "funds_usage_cd" character varying(20),
    "loa_unique_restrictions_yn" character(1) NOT NULL,
    "fund_restriction" character varying(2000),
    "client_duns_number" character varying(9),
    "client_duns_plus4" character varying(4),
    "appropriation" character varying(4),
    "treasury_sub_account" character varying(3),
    "tsym_alloc_trans_agency_cd" character varying(3),
    "tsym_customer_sublevel_prefix" character varying(3),
    "boac1" character varying(6),
    "boac2" character varying(6),
    "agency_location_code" character varying(10),
    "requesting_agency_cd" character varying(3),
    "loa_change_reason_cd" character varying(40),
    "loa_change_remarks" character varying(2000),
    "pop_start_dt" timestamp(6) without time zone,
    "pop_end_dt" timestamp(6) without time zone,
    "fund_type_cd" character varying(10) NOT NULL,
    "first_fy_available_year" integer,
    "last_fy_available_year" integer,
    "tsym_availability_type" character varying(1),
    "loa_expiration_dt" timestamp(6) without time zone,
    "subject_to_availability_yn" character(1) NOT NULL,
    "loa_dollar_amt" numeric(15,2) NOT NULL,
    "customer_act_num" character varying(20),
    "customer_mdl" character varying(2),
    "customer_fund" character varying(4),
    "fed_code" character varying(2),
    "cost_element" character varying(3),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "client_uei" character varying(16),
    "client_uei_plus4" character varying(8),
    "sub_allocation" character varying(4),
    "ginv_line_num" integer,
    "ginv_schedule_number" integer,
    "bona_fide_need" character varying(2000),
    "client_vuei" character varying(16) GENERATED ALWAYS AS (
CASE
    WHEN (client_uei IS NULL) THEN client_duns_number
    ELSE client_uei
END) STORED,
    "client_vuei_plus4" character varying(13) GENERATED ALWAYS AS (
CASE
    WHEN (client_uei_plus4 IS NULL) THEN client_duns_plus4
    ELSE client_uei_plus4
END) STORED,
    CONSTRAINT "pk_loa" PRIMARY KEY (id),
    CONSTRAINT "fk_legacy_sync_status" FOREIGN KEY (legacy_sync_status_cd) REFERENCES aasbs.lu_legacy_sync_status(cd),
    CONSTRAINT "fk_loa_boac1" FOREIGN KEY (boac1) REFERENCES table_master.boacs(boac_code) NOT VALID,
    CONSTRAINT "fk_loa_boac2" FOREIGN KEY (boac2) REFERENCES table_master.boacs(boac_code) NOT VALID,
    CONSTRAINT "fk_loa_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_loa_chng_rsn" FOREIGN KEY (loa_change_reason_cd) REFERENCES aasbs.lu_loa_change_reason(cd),
    CONSTRAINT "fk_loa_fund_type" FOREIGN KEY (fund_type_cd) REFERENCES aasbs.lu_fund_type(cd),
    CONSTRAINT "fk_loa_funding_amendment" FOREIGN KEY (creator_funding_amendment_id) REFERENCES aasbs.funding_amendment(id),
    CONSTRAINT "fk_loa_funds_usage_cd" FOREIGN KEY (funds_usage_cd) REFERENCES aasbs.lu_funds_usage(cd),
    CONSTRAINT "fk_loa_program" FOREIGN KEY (program_cd) REFERENCES aasbs.lu_program(cd),
    CONSTRAINT "fk_loa_reqst_agency" FOREIGN KEY (requesting_agency_cd) REFERENCES aasbs.lu_agency(cd) NOT VALID,
    CONSTRAINT "fk_loa_status" FOREIGN KEY (loa_status_cd) REFERENCES aasbs.lu_loa_status(cd),
    CONSTRAINT "fk_loa_tsym_alloc_trans_agency" FOREIGN KEY (tsym_alloc_trans_agency_cd) REFERENCES aasbs.lu_agency(cd),
    CONSTRAINT "fk_loa_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_id ON aasbs.loa USING btree (id);
CREATE INDEX ix_legacy_sync_status ON aasbs.loa USING btree (legacy_sync_status_cd);
CREATE INDEX ix_loa_agree_num ON aasbs.loa USING btree (agreement_number);
CREATE INDEX ix_loa_boac1 ON aasbs.loa USING btree (boac1);
CREATE INDEX ix_loa_boac2 ON aasbs.loa USING btree (boac2);
CREATE INDEX ix_loa_cbui ON aasbs.loa USING btree (created_by_user_id);
CREATE INDEX ix_loa_chng_rsn ON aasbs.loa USING btree (loa_change_reason_cd);
CREATE INDEX ix_loa_created_dt ON aasbs.loa USING btree (created_dt);
CREATE INDEX ix_loa_fund_type ON aasbs.loa USING btree (fund_type_cd);
CREATE INDEX ix_loa_funding_amendment ON aasbs.loa USING btree (creator_funding_amendment_id);
CREATE INDEX ix_loa_funds_usage_cd ON aasbs.loa USING btree (funds_usage_cd);
CREATE INDEX ix_loa_program ON aasbs.loa USING btree (program_cd);
CREATE INDEX ix_loa_reqst_agency ON aasbs.loa USING btree (requesting_agency_cd);
CREATE INDEX ix_loa_status ON aasbs.loa USING btree (loa_status_cd);
CREATE INDEX ix_loa_status_cd ON aasbs.loa USING btree (loa_status_cd);
CREATE INDEX ix_loa_subj2avail_yn ON aasbs.loa USING btree (subject_to_availability_yn);
CREATE INDEX ix_loa_tracking_num ON aasbs.loa USING btree (tracking_num);
CREATE INDEX ix_loa_trk_num_id ON aasbs.loa USING btree (tracking_num, id DESC);
CREATE INDEX ix_loa_tsym_alloc_trans_agency ON aasbs.loa USING btree (tsym_alloc_trans_agency_cd);
CREATE INDEX ix_loa_ubui ON aasbs.loa USING btree (updated_by_user_id);

CREATE TRIGGER loa_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.loa FOR EACH ROW EXECUTE FUNCTION aasbs.trg_loa_hst();
CREATE TRIGGER trg_ccom_loa BEFORE DELETE ON aasbs.loa FOR EACH ROW EXECUTE FUNCTION aasbs."trg_ccom_loa$loa"();

COMMENT ON COLUMN "aasbs"."loa"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LOA_SEQ';
COMMENT ON COLUMN "aasbs"."loa"."loa_status_cd" IS 'Foreign Key to table AASBS.LU_LOA_STATUS';
COMMENT ON COLUMN "aasbs"."loa"."creator_funding_amendment_id" IS 'Foreign Key to table AASBS.FUNDING_AMENDMENT';
COMMENT ON COLUMN "aasbs"."loa"."agreement_number" IS 'Agreement Number on a Citation';
COMMENT ON COLUMN "aasbs"."loa"."agreement_end_dt" IS 'Agreement date of the Line of Accounting (LOA)';
COMMENT ON COLUMN "aasbs"."loa"."transmitted_dt" IS 'Date/time the LOA was transmitted';
COMMENT ON COLUMN "aasbs"."loa"."agreement_accepted_yn" IS 'Flag indicating whether the LOA Agreement is Accepted (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."loa"."agreement_converted_yn" IS 'Flag indicating whether the LOA Agreement is Converted (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."loa"."legacy_sync_status_cd" IS 'Foreign Key to table AASBS.LU_LEGACY_SYNC_STATUS';
COMMENT ON COLUMN "aasbs"."loa"."legacy_sync_dt" IS 'Used during Convergence period in which ASSIST2.0 data is being synchronized into Legacy ASSIST tables';
COMMENT ON COLUMN "aasbs"."loa"."tracking_num" IS 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via it''s Tracking_Num';
COMMENT ON COLUMN "aasbs"."loa"."line_of_accounting" IS 'Complex Line of Accounting String with CLINS, DUNS, etc';
COMMENT ON COLUMN "aasbs"."loa"."program_cd" IS 'Foreign Key to table AASBS.LU_PROGRAM';
COMMENT ON COLUMN "aasbs"."loa"."funds_usage_cd" IS 'Foreign Key to table AASBS.LU_FUNDS_USAGE';
COMMENT ON COLUMN "aasbs"."loa"."loa_unique_restrictions_yn" IS 'Flag indicating whether there are unique restrictions on this LOA (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."loa"."fund_restriction" IS 'User-entered free-text Restrictions on LOA use';
COMMENT ON COLUMN "aasbs"."loa"."client_duns_number" IS 'Client DUNS. The requesting agency DUNS number';
COMMENT ON COLUMN "aasbs"."loa"."client_duns_plus4" IS 'DUNS number assigned to a Client';
COMMENT ON COLUMN "aasbs"."loa"."appropriation" IS 'The order appropriation code';
COMMENT ON COLUMN "aasbs"."loa"."treasury_sub_account" IS 'Treasury Sub Account. The Treasury Sub-account identifier';
COMMENT ON COLUMN "aasbs"."loa"."tsym_alloc_trans_agency_cd" IS 'Foreign Key to table AASBS.LU_AGENCY';
COMMENT ON COLUMN "aasbs"."loa"."tsym_customer_sublevel_prefix" IS 'TSYM specific data';
COMMENT ON COLUMN "aasbs"."loa"."boac1" IS 'The ordering agency BOAC code';
COMMENT ON COLUMN "aasbs"."loa"."boac2" IS 'The billing agency BOAC code';
COMMENT ON COLUMN "aasbs"."loa"."agency_location_code" IS 'Identifying code of the paying office for treasury when an agency is using OPAC billing';
COMMENT ON COLUMN "aasbs"."loa"."requesting_agency_cd" IS 'Foreign Key to table AASBS.LU_REQUESTING_AGENCY';
COMMENT ON COLUMN "aasbs"."loa"."loa_change_reason_cd" IS 'Foreign Key to table AASBS.LU_LOA_CHANGE_REASON';
COMMENT ON COLUMN "aasbs"."loa"."loa_change_remarks" IS 'Reason why the LOA was modified';
COMMENT ON COLUMN "aasbs"."loa"."pop_start_dt" IS 'Period of Performance start date';
COMMENT ON COLUMN "aasbs"."loa"."pop_end_dt" IS 'Period of Performance end date';
COMMENT ON COLUMN "aasbs"."loa"."fund_type_cd" IS 'Foreign Key to table AASBS.LU_FUND_TYPE';
COMMENT ON COLUMN "aasbs"."loa"."first_fy_available_year" IS 'First Fiscal Year the LOA money will be available';
COMMENT ON COLUMN "aasbs"."loa"."last_fy_available_year" IS 'Last Fiscal Year the LOA money will be available';
COMMENT ON COLUMN "aasbs"."loa"."tsym_availability_type" IS 'TSYM specific data';
COMMENT ON COLUMN "aasbs"."loa"."loa_expiration_dt" IS 'Expiration date of the LOA';
COMMENT ON COLUMN "aasbs"."loa"."subject_to_availability_yn" IS 'Flag indicating whether funds are Subject to Availability for this LOA (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."loa"."loa_dollar_amt" IS 'Total dollar amount remaining on the LOA (when it is the latest LOA record for a single Tracking_Num)';
COMMENT ON COLUMN "aasbs"."loa"."customer_act_num" IS 'IA Type S2 - Internal GSA Funding Document Number';
COMMENT ON COLUMN "aasbs"."loa"."customer_mdl" IS 'The client''s MDL';
COMMENT ON COLUMN "aasbs"."loa"."customer_fund" IS 'Customer Fund assigned from the funding doc, and used to determine Interfund Indicator and Activity Code. Different from Fund.';
COMMENT ON COLUMN "aasbs"."loa"."fed_code" IS 'The fed code associated with the funding citation';
COMMENT ON COLUMN "aasbs"."loa"."cost_element" IS 'Client Cost Element';
COMMENT ON COLUMN "aasbs"."loa"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."loa"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."loa"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."loa"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."loa"."client_uei" IS 'Unique Entity Identifier (UEI) - replaces DUNS';
COMMENT ON COLUMN "aasbs"."loa"."client_uei_plus4" IS 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4';
COMMENT ON COLUMN "aasbs"."loa"."sub_allocation" IS 'LOA embedded data that facilitates DoD monthly reconciliations';
COMMENT ON COLUMN "aasbs"."loa"."ginv_line_num" IS 'G-Invoicing Order Line Number (GINV_ORD_LNUM)';
COMMENT ON COLUMN "aasbs"."loa"."ginv_schedule_number" IS 'G-Invoicing Order Schedule Number (GINV_ORD_SCHL_NUM)';
COMMENT ON COLUMN "aasbs"."loa"."bona_fide_need" IS 'Description of (bona fide) need for funding. If gInvoicing, then sourced from PROD_NEED_DSCR.';

COMMENT ON TABLE "aasbs"."loa" IS 'Line of Accounting details';

-- ------------------------------------------------------------
-- Table: aasbs.loa_acquisition
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."loa_acquisition" (
    "id" bigint NOT NULL,
    "loa_id" bigint NOT NULL,
    "acquisition_id" bigint NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_loa_acquisition" PRIMARY KEY (id),
    CONSTRAINT "fk_loa_acq_acq_id" FOREIGN KEY (acquisition_id) REFERENCES aasbs.acquisition(id),
    CONSTRAINT "fk_loa_acq_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_loa_acq_loa_id" FOREIGN KEY (loa_id) REFERENCES aasbs.loa(id),
    CONSTRAINT "fk_loa_acq_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_loa_acq_acq_id ON aasbs.loa_acquisition USING btree (acquisition_id);
CREATE INDEX ix_loa_acq_cbui ON aasbs.loa_acquisition USING btree (created_by_user_id);
CREATE INDEX ix_loa_acq_loa_id ON aasbs.loa_acquisition USING btree (loa_id);
CREATE INDEX ix_loa_acq_ubui ON aasbs.loa_acquisition USING btree (updated_by_user_id);

CREATE TRIGGER loa_acquisition_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.loa_acquisition FOR EACH ROW EXECUTE FUNCTION aasbs.trg_loa_acquisition_hst();

COMMENT ON COLUMN "aasbs"."loa_acquisition"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LOA_ACQUISITION_SEQ';
COMMENT ON COLUMN "aasbs"."loa_acquisition"."loa_id" IS 'Foreign Key to table AASBS.LOA';
COMMENT ON COLUMN "aasbs"."loa_acquisition"."acquisition_id" IS 'Foreign Key to table AASBS.ACQUISITION';
COMMENT ON COLUMN "aasbs"."loa_acquisition"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."loa_acquisition"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."loa_acquisition"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."loa_acquisition"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."loa_acquisition" IS 'Join table associating LOAs with Acquisitions';

-- ------------------------------------------------------------
-- Table: aasbs.loa_ledger
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."loa_ledger" (
    "id" bigint NOT NULL,
    "loa_id" bigint NOT NULL,
    "transaction_type_cd" character varying(30) NOT NULL,
    "tracking_num" character varying(20) NOT NULL,
    "line_item_committed_amt" numeric(15,2) NOT NULL,
    "line_item_certified_amt" numeric(15,2) NOT NULL,
    "line_item_obligated_amt" numeric(15,2) NOT NULL,
    "service_charge_amt" numeric(15,2) NOT NULL,
    "loa_funded_amt" numeric(15,2) NOT NULL,
    "funding_amendment_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "line_item_accepted_id" bigint,
    "draft_obligation_adjust_amt" numeric(15,2),
    "li_tracking_num" bigint,
    "funding_id" bigint NOT NULL,
    "ia_id" bigint NOT NULL,
    CONSTRAINT "pk_loa_ledger" PRIMARY KEY (id),
    CONSTRAINT "ck_loa_ledger_fk_missing" CHECK (transaction_type_cd::text = 'LINE_ITEM'::text AND line_item_accepted_id IS NOT NULL OR transaction_type_cd::text = 'AMEND_ADJUST'::text AND funding_amendment_id IS NOT NULL OR transaction_type_cd::text = 'PREASSIST'::text),
    CONSTRAINT "fk_loa_ledger_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_loa_ledger_famend_id" FOREIGN KEY (funding_amendment_id) REFERENCES aasbs.funding_amendment(id),
    CONSTRAINT "fk_loa_ledger_funding_id" FOREIGN KEY (funding_id) REFERENCES aasbs.funding(id),
    CONSTRAINT "fk_loa_ledger_ia_id" FOREIGN KEY (ia_id) REFERENCES aasbs.ia(id),
    CONSTRAINT "fk_loa_ledger_li_accepted_id" FOREIGN KEY (line_item_accepted_id) REFERENCES aasbs.line_item_accepted(id),
    CONSTRAINT "fk_loa_ledger_loaid" FOREIGN KEY (loa_id) REFERENCES aasbs.loa(id),
    CONSTRAINT "fk_loa_ledger_trans_type" FOREIGN KEY (transaction_type_cd) REFERENCES aasbs.lu_transaction_type(cd),
    CONSTRAINT "fk_loa_ledger_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX idx_created_dt ON aasbs.loa_ledger USING btree (created_dt);
CREATE INDEX ix_loa_ledger_cbui ON aasbs.loa_ledger USING btree (created_by_user_id);
CREATE INDEX ix_loa_ledger_famendid ON aasbs.loa_ledger USING btree (funding_amendment_id);
CREATE INDEX ix_loa_ledger_funding_id ON aasbs.loa_ledger USING btree (funding_id);
CREATE INDEX ix_loa_ledger_ia_id ON aasbs.loa_ledger USING btree (ia_id);
CREATE INDEX ix_loa_ledger_li_accepted_id ON aasbs.loa_ledger USING btree (line_item_accepted_id);
CREATE INDEX ix_loa_ledger_li_tracking_num ON aasbs.loa_ledger USING btree (li_tracking_num);
CREATE INDEX ix_loa_ledger_loaid ON aasbs.loa_ledger USING btree (loa_id);
CREATE INDEX ix_loa_ledger_tracknum ON aasbs.loa_ledger USING btree (tracking_num);
CREATE INDEX ix_loa_ledger_transtype ON aasbs.loa_ledger USING btree (transaction_type_cd);
CREATE INDEX ix_loa_ledger_ubui ON aasbs.loa_ledger USING btree (updated_by_user_id);
CREATE INDEX " idx_loa_ledger_lia_id_tracking_amt" ON aasbs.loa_ledger USING btree (line_item_obligated_amt);

CREATE TRIGGER loa_ledger_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.loa_ledger FOR EACH ROW EXECUTE FUNCTION aasbs.trg_loa_ledger_hst();
CREATE TRIGGER trg_restrict_loa_ledger BEFORE UPDATE ON aasbs.loa_ledger FOR EACH ROW EXECUTE FUNCTION aasbs."trg_restrict_loa_ledger$loa_ledger"();

COMMENT ON COLUMN "aasbs"."loa_ledger"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LOA_LEDGER_SEQ';
COMMENT ON COLUMN "aasbs"."loa_ledger"."loa_id" IS 'Foreign Key to table AASBS.LOA';
COMMENT ON COLUMN "aasbs"."loa_ledger"."transaction_type_cd" IS 'Foreign Key to table AASBS.LU_TRANSACTION_TYPE';
COMMENT ON COLUMN "aasbs"."loa_ledger"."tracking_num" IS 'The Line Of Accounting (LOA) Tracking Number - uniquely identifies a single LOA - sometimes across multple aasbs.LOA records';
COMMENT ON COLUMN "aasbs"."loa_ledger"."line_item_committed_amt" IS 'Committed amount for any type of Line Item - Deliverable or Service Charge';
COMMENT ON COLUMN "aasbs"."loa_ledger"."line_item_certified_amt" IS 'Certified amount for any type of Line Item - Deliverable or Service Charge';
COMMENT ON COLUMN "aasbs"."loa_ledger"."line_item_obligated_amt" IS 'Obligated amount for Deliverables Line Items.  Note that Service Charges do not go in this column - they go in Service_Charge_Amt';
COMMENT ON COLUMN "aasbs"."loa_ledger"."service_charge_amt" IS 'The Service Charge amount that can be billed ';
COMMENT ON COLUMN "aasbs"."loa_ledger"."loa_funded_amt" IS 'The amounts added or subtracted from an LOA via Funding Amendments';
COMMENT ON COLUMN "aasbs"."loa_ledger"."funding_amendment_id" IS 'Foreign Key to table AASBS.FUNDING_AMENDMENT';
COMMENT ON COLUMN "aasbs"."loa_ledger"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."loa_ledger"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."loa_ledger"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."loa_ledger"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."loa_ledger"."line_item_accepted_id" IS 'Foreign Key to table AASBS.LINE_ITEM_ACCEPTED';
COMMENT ON COLUMN "aasbs"."loa_ledger"."draft_obligation_adjust_amt" IS 'Draft Obligation Adjustment Amount';
COMMENT ON COLUMN "aasbs"."loa_ledger"."li_tracking_num" IS 'Identifier of a real-world "Line Item".  It is the ID of the first record (/ version) in LINE_ITEM table representing the real-world "Line Item"';
COMMENT ON COLUMN "aasbs"."loa_ledger"."funding_id" IS 'Foreign Key to table AASBS.FUNDING';
COMMENT ON COLUMN "aasbs"."loa_ledger"."ia_id" IS 'Foreign Key to table AASBS.IA';

COMMENT ON TABLE "aasbs"."loa_ledger" IS 'Table Description:  Ledger tracking all LOA related monitary transfers.  All amounts in this table are relative to the LOA.  If an amount column is positive - money is being added to the LOA.  If an amount column is negative - money is being removed from the LOA.';

-- ------------------------------------------------------------
-- Table: aasbs.loa_ledger_burn_order
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."loa_ledger_burn_order" (
    "id" bigint NOT NULL,
    "line_item_accepted_id" bigint,
    "tracking_num" character varying(20) NOT NULL,
    "burn_order" bigint NOT NULL,
    "loa_subtask" character varying(4),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_loa_ledger_burn_order" PRIMARY KEY (id),
    CONSTRAINT "fk_loa_burn_order_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_loa_burn_order_liaccept_id" FOREIGN KEY (line_item_accepted_id) REFERENCES aasbs.line_item_accepted(id),
    CONSTRAINT "fk_loa_burn_order_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_loa_burn_order_burn_order ON aasbs.loa_ledger_burn_order USING btree (burn_order);
CREATE INDEX ix_loa_burn_order_cbui ON aasbs.loa_ledger_burn_order USING btree (created_by_user_id);
CREATE INDEX ix_loa_burn_order_liaccept_id ON aasbs.loa_ledger_burn_order USING btree (line_item_accepted_id);
CREATE INDEX ix_loa_burn_order_track_num ON aasbs.loa_ledger_burn_order USING btree (tracking_num);
CREATE INDEX ix_loa_burn_order_ubui ON aasbs.loa_ledger_burn_order USING btree (updated_by_user_id);
CREATE UNIQUE INDEX ux_loa_burn_order_li_id_trknum ON aasbs.loa_ledger_burn_order USING btree (line_item_accepted_id, tracking_num);
CREATE INDEX " idx_llbo_id_loa_subtask" ON aasbs.loa_ledger_burn_order USING btree (loa_subtask);

CREATE TRIGGER loa_ledger_burn_order_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.loa_ledger_burn_order FOR EACH ROW EXECUTE FUNCTION aasbs.trg_loa_ledger_burn_order_hst();

COMMENT ON COLUMN "aasbs"."loa_ledger_burn_order"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.LOA_LEDGER_BURN_ORDER_SEQ';
COMMENT ON COLUMN "aasbs"."loa_ledger_burn_order"."line_item_accepted_id" IS 'Foreign Key to table AASBS.LINE_ITEM_ACCEPTED';
COMMENT ON COLUMN "aasbs"."loa_ledger_burn_order"."tracking_num" IS 'The distinct identifier for a distinct LOA.  LOA Tracking_Num can be represented on multiple LOA records but they all represent a single LOA via it''s Tracking_Num';
COMMENT ON COLUMN "aasbs"."loa_ledger_burn_order"."burn_order" IS 'Defined order of depletion';
COMMENT ON COLUMN "aasbs"."loa_ledger_burn_order"."loa_subtask" IS 'Subtask used to identify Line Item / LOA combinations';
COMMENT ON COLUMN "aasbs"."loa_ledger_burn_order"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."loa_ledger_burn_order"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."loa_ledger_burn_order"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."loa_ledger_burn_order"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."loa_ledger_burn_order" IS 'The order in which LOAs are to be depleted.  Applies only to cases of multiple LOAs funding a single line item';

-- ------------------------------------------------------------
-- Table: aasbs.lu_accrual_income_doc_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_accrual_income_doc_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_accrual_income_doc_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_accrual_income_doc_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_accrual_income_doc_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_accrual_income_doc_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_accrual_income_doc_type_hst();

COMMENT ON COLUMN "aasbs"."lu_accrual_income_doc_type"."cd" IS 'alphanumeric primary key for this table';
COMMENT ON COLUMN "aasbs"."lu_accrual_income_doc_type"."description" IS 'description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_accrual_income_doc_type"."active_yn" IS 'flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_accrual_income_doc_type"."created_by_user_name" IS 'oracle login user id who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_accrual_income_doc_type"."created_dt" IS 'date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_accrual_income_doc_type"."updated_by_user_name" IS 'oracle login username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_accrual_income_doc_type"."updated_dt" IS 'most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_accrual_income_doc_type" IS 'lookup table - all accrual income document types';

-- ------------------------------------------------------------
-- Table: aasbs.lu_acquisition_mod_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_acquisition_mod_status" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_acq_mod_status" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_acq_mod_status_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_acquisition_mod_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_acquisition_mod_status FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_acquisition_mod_status_hst();

COMMENT ON COLUMN "aasbs"."lu_acquisition_mod_status"."cd" IS 'State or status code of acquisition mod';
COMMENT ON COLUMN "aasbs"."lu_acquisition_mod_status"."description" IS 'Display names for acquisition mod status';
COMMENT ON COLUMN "aasbs"."lu_acquisition_mod_status"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_acquisition_mod_status"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_acquisition_mod_status"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_acquisition_mod_status"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_acquisition_mod_status"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."lu_acquisition_mod_status" IS 'This table contains all possible statuses of an acquisition mod';

-- ------------------------------------------------------------
-- Table: aasbs.lu_acquisition_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_acquisition_status" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_acquisition_status" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_acquisition_status_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_acquisition_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_acquisition_status FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_acquisition_status_hst();

COMMENT ON COLUMN "aasbs"."lu_acquisition_status"."cd" IS 'State or status code on acquisition';
COMMENT ON COLUMN "aasbs"."lu_acquisition_status"."description" IS 'Display names for acquisition status';
COMMENT ON COLUMN "aasbs"."lu_acquisition_status"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_acquisition_status"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_acquisition_status"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_acquisition_status"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_acquisition_status"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."lu_acquisition_status" IS 'Lookup table - All possible statuses of an Acquisition';

-- ------------------------------------------------------------
-- Table: aasbs.lu_acquisition_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_acquisition_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_acquisition_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_acq_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_acquisition_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_acquisition_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_acquisition_type_hst();

COMMENT ON COLUMN "aasbs"."lu_acquisition_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_acquisition_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_acquisition_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_acquisition_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_acquisition_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_acquisition_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_acquisition_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_acquisition_type" IS 'Lookup table - Type of Acquistion, e.g., Service or Supply';

-- ------------------------------------------------------------
-- Table: aasbs.lu_activity_address_code
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_activity_address_code" (
    "cd" character varying(6) NOT NULL,
    "description" character varying(100),
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "region_cd" character varying(2),
    "program_cd" character varying(30),
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "emp_org_id" bigint,
    "regional_office_address_id" bigint,
    "fpds_ng_agency_identifier" character varying(6),
    CONSTRAINT "pk_lu_activity_address_code" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_aac_activeyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "nn_lu_aac_program" CHECK (program_cd IS NOT NULL),
    CONSTRAINT "nn_lu_aac_region" CHECK (region_cd IS NOT NULL),
    CONSTRAINT "fk_aac_emporgid" FOREIGN KEY (emp_org_id) REFERENCES assist.lu_emp_orgs(emp_org_id),
    CONSTRAINT "fk_lu_aac_program" FOREIGN KEY (program_cd) REFERENCES aasbs.lu_program(cd),
    CONSTRAINT "fk_lu_aac_region" FOREIGN KEY (region_cd) REFERENCES aasbs.lu_region(cd)
);

CREATE INDEX idx_lu_activity_address_code_cd ON aasbs.lu_activity_address_code USING btree (cd);
CREATE INDEX ix_laac_emp_org_id ON aasbs.lu_activity_address_code USING btree (emp_org_id);
CREATE INDEX ix_laac_program_cd ON aasbs.lu_activity_address_code USING btree (program_cd);
CREATE INDEX ix_laac_region_cd ON aasbs.lu_activity_address_code USING btree (region_cd);

CREATE TRIGGER lu_activity_address_code_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_activity_address_code FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_activity_address_code_hst();

COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."region_cd" IS 'Foreign Key to table AASBS.LU_REGION';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."program_cd" IS 'Foreign Key to table AASBS.LU_PROGRAM';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."emp_org_id" IS 'Foreign key to table ASSIST.LU_EMP_ORGS.EMP_ORG_ID (values should be "Office" from Registration LU_EMP_ORGS.TIER_ID = 3)';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."regional_office_address_id" IS 'Foreign Key to table AASBS.ADDRESS';
COMMENT ON COLUMN "aasbs"."lu_activity_address_code"."fpds_ng_agency_identifier" IS 'These "Agency Identifiers" are a concatenation ASSIST2 Agency and Bureau codes - specifically used by FPDS-NG (Federal Procurement Data System - Next Generation)';

COMMENT ON TABLE "aasbs"."lu_activity_address_code" IS 'Lookup table - Activity Address Codes - uniform way to identify organizations in federal agencies';

-- ------------------------------------------------------------
-- Table: aasbs.lu_address_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_address_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_address_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_address_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_address_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_address_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_address_type_hst();

COMMENT ON COLUMN "aasbs"."lu_address_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_address_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_address_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_address_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_address_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_address_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_address_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_address_type" IS 'Lookup table - Address type for a single Address record, e.g., Client, Shipping, Accepting';

-- ------------------------------------------------------------
-- Table: aasbs.lu_agency
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_agency" (
    "cd" character varying(30) NOT NULL,
    "cd_2char" character varying(2) NOT NULL,
    "description" character varying(100) NOT NULL,
    "is_dod_agency_yn" character(1) NOT NULL,
    "active_yn" character(1) NOT NULL,
    "comments" character varying(500),
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "sort_order" bigint,
    CONSTRAINT "pk_lu_agency" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_agency_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_lu_agency_dodyn" CHECK (is_dod_agency_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_agency_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_agency FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_agency_hst();

COMMENT ON COLUMN "aasbs"."lu_agency"."cd" IS 'Primary key for the table';
COMMENT ON COLUMN "aasbs"."lu_agency"."cd_2char" IS 'Legacy 2-char agency code';
COMMENT ON COLUMN "aasbs"."lu_agency"."description" IS 'Agency Name (sourced from FAST Book II - March 2017 https://www.fiscal.treasury.gov/fsreports/ref/fastBook/fastbook_home.htm )';
COMMENT ON COLUMN "aasbs"."lu_agency"."is_dod_agency_yn" IS 'Flag indicating whether an Agency is associated with DOD (=Y)  or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_agency"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_agency"."comments" IS 'Comments';
COMMENT ON COLUMN "aasbs"."lu_agency"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_agency"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_agency"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_agency"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."lu_agency" IS 'Lookup table - All Agencies referenced in Assist';

-- ------------------------------------------------------------
-- Table: aasbs.lu_agreement_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_agreement_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(200) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_agreement_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_agreement_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_agreement_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_agreement_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_agreement_type_hst();

COMMENT ON COLUMN "aasbs"."lu_agreement_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_agreement_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_agreement_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_agreement_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_agreement_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_agreement_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_agreement_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_agreement_type" IS 'Lookup table - GSA Agency Agreement types, e.g., Inter-Agency, Intra-Agency';

-- ------------------------------------------------------------
-- Table: aasbs.lu_award_mod_fpds_reason
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_award_mod_fpds_reason" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_award_mod_fpds_reason" PRIMARY KEY (cd),
    CONSTRAINT "ck_awd_mod_fpds_reason_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_award_mod_fpds_reason_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_award_mod_fpds_reason FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_award_mod_fpds_reason_hst();

COMMENT ON COLUMN "aasbs"."lu_award_mod_fpds_reason"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_award_mod_fpds_reason"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_award_mod_fpds_reason"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_award_mod_fpds_reason"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_award_mod_fpds_reason"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_award_mod_fpds_reason"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_award_mod_fpds_reason"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_award_mod_fpds_reason"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_award_mod_fpds_reason" IS 'Lookup table - All Possible FPDS Modification Reasons';

-- ------------------------------------------------------------
-- Table: aasbs.lu_award_validation
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_award_validation" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(500),
    "title" character varying(500),
    "validation_type" character varying(500),
    "remedy" character varying(500),
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_award_validation" PRIMARY KEY (cd),
    CONSTRAINT "ck_awd_validation_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_award_validation_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_award_validation FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_award_validation_hst();

COMMENT ON COLUMN "aasbs"."lu_award_validation"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_award_validation"."description" IS 'Description of the Award Validation';
COMMENT ON COLUMN "aasbs"."lu_award_validation"."title" IS 'Title of the Award Validation';
COMMENT ON COLUMN "aasbs"."lu_award_validation"."validation_type" IS 'Validation Type - Hard or Soft';
COMMENT ON COLUMN "aasbs"."lu_award_validation"."remedy" IS 'Instructions to remedy an unfulfilled Award Validation';
COMMENT ON COLUMN "aasbs"."lu_award_validation"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_award_validation"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_award_validation"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_award_validation"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_award_validation"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_award_validation"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_award_validation" IS 'Lookup table - Award Validation Criteria';

-- ------------------------------------------------------------
-- Table: aasbs.lu_billing_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_billing_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(30) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_billing_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_billing_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_billing_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_billing_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_billing_type_hst();

COMMENT ON COLUMN "aasbs"."lu_billing_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_billing_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_billing_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_billing_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_billing_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_billing_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_billing_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_billing_type" IS 'Lookup table - All Billing Types for a Funding Package (e.g, Standard, Advanced, Pre-Paid, Pre-Validation)';

-- ------------------------------------------------------------
-- Table: aasbs.lu_bundled_contract
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_bundled_contract" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_bundled_contract" PRIMARY KEY (cd),
    CONSTRAINT "ck_bndl_cntrct_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_bundled_contract_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_bundled_contract FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_bundled_contract_hst();

COMMENT ON COLUMN "aasbs"."lu_bundled_contract"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_bundled_contract"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_bundled_contract"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_bundled_contract"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_bundled_contract"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_bundled_contract"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_bundled_contract"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_bundled_contract"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_bundled_contract" IS 'Lookup table - Options for whether the acquisition meets the definition of a bundled requirement, as defined in the FAR.';

-- ------------------------------------------------------------
-- Table: aasbs.lu_bureau
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_bureau" (
    "cd" character varying(30) NOT NULL,
    "agency_cd" character varying(3) NOT NULL,
    "description" character varying(200) NOT NULL,
    "active_yn" character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_bureau" PRIMARY KEY (cd, agency_cd),
    CONSTRAINT "ck_lu_bureau_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_lu_bureau_agency" FOREIGN KEY (agency_cd) REFERENCES aasbs.lu_agency(cd)
);

CREATE TRIGGER lu_bureau_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_bureau FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_bureau_hst();

COMMENT ON COLUMN "aasbs"."lu_bureau"."cd" IS 'Primary key for the table';
COMMENT ON COLUMN "aasbs"."lu_bureau"."agency_cd" IS 'Foreign Key to table AASBS.LU_AGENCY';
COMMENT ON COLUMN "aasbs"."lu_bureau"."description" IS 'Description of the client type';
COMMENT ON COLUMN "aasbs"."lu_bureau"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_bureau"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_bureau"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_bureau"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_bureau"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."lu_bureau" IS 'Lookup table - All Bureaus referenced in Assist';

-- ------------------------------------------------------------
-- Table: aasbs.lu_central_comment_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_central_comment_type" (
    "cd" character varying(30) NOT NULL,
    "table_name_cd" character varying(30),
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_commnt_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_commnt_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_lu_commnt_type_table_name" FOREIGN KEY (table_name_cd) REFERENCES aasbs.lu_table_name(cd)
);

CREATE INDEX ix_lcct_table_name_cd ON aasbs.lu_central_comment_type USING btree (table_name_cd);

CREATE TRIGGER lu_central_comment_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_central_comment_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_central_comment_type_hst();

COMMENT ON COLUMN "aasbs"."lu_central_comment_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_central_comment_type"."table_name_cd" IS 'Foreign Key to table AASBS.LU_TABLE_NAME';
COMMENT ON COLUMN "aasbs"."lu_central_comment_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_central_comment_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_central_comment_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_central_comment_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_central_comment_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_central_comment_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_central_comment_type" IS 'Lookup table - Types of Comments in CENTRAL_COMMENT table';

-- ------------------------------------------------------------
-- Table: aasbs.lu_checklist_item
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_checklist_item" (
    "cd" character varying(100) NOT NULL,
    "checklist_type_cd" character varying(30) NOT NULL,
    "description" character varying(200) NOT NULL,
    "tooltip" character varying(260),
    "sort_order" bigint,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_chklst_item" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_chklst_item_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_lu_chklst_item_cl_type" FOREIGN KEY (checklist_type_cd) REFERENCES aasbs.lu_checklist_type(cd)
);

CREATE INDEX ix_lci_checklist_type_cd ON aasbs.lu_checklist_item USING btree (checklist_type_cd);

CREATE TRIGGER lu_checklist_item_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_checklist_item FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_checklist_item_hst();

COMMENT ON COLUMN "aasbs"."lu_checklist_item"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_checklist_item"."checklist_type_cd" IS 'Foreign Key to table AASBS.LU_CHECKLIST_TYPE - used to group CHECKLIST ITEMS that display together on the same UI';
COMMENT ON COLUMN "aasbs"."lu_checklist_item"."description" IS 'Display name the checklist item';
COMMENT ON COLUMN "aasbs"."lu_checklist_item"."tooltip" IS 'Tooltip for the Checklist Item';
COMMENT ON COLUMN "aasbs"."lu_checklist_item"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_checklist_item"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_checklist_item"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_checklist_item"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_checklist_item"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_checklist_item"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."lu_checklist_item" IS 'Lookup table - Checklist Item - invidivual line items to which answers are attributed manually or automatically';

-- ------------------------------------------------------------
-- Table: aasbs.lu_checklist_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_checklist_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(80) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_chklst_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_chklst_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_checklist_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_checklist_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_checklist_type_hst();

COMMENT ON COLUMN "aasbs"."lu_checklist_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_checklist_type"."description" IS 'Display name the checklist type';
COMMENT ON COLUMN "aasbs"."lu_checklist_type"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_checklist_type"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_checklist_type"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_checklist_type"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_checklist_type"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."lu_checklist_type" IS 'Lookup table - Checklist Type - used to create distinct lists of Checklist Items - Acquisition Closeout, etc';

-- ------------------------------------------------------------
-- Table: aasbs.lu_checklist_value
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_checklist_value" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(120) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "sort_order" bigint,
    CONSTRAINT "pk_lu_chklst_value" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_chklst_value_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "nn_lu_chklst_value_sort_order" CHECK (sort_order IS NOT NULL)
);

CREATE TRIGGER lu_checklist_value_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_checklist_value FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_checklist_value_hst();

COMMENT ON COLUMN "aasbs"."lu_checklist_value"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_checklist_value"."description" IS 'Display name the checklist value';
COMMENT ON COLUMN "aasbs"."lu_checklist_value"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_checklist_value"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_checklist_value"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_checklist_value"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_checklist_value"."updated_dt" IS 'Record last updated on this date';
COMMENT ON COLUMN "aasbs"."lu_checklist_value"."sort_order" IS 'Sort order used for UI Display';

COMMENT ON TABLE "aasbs"."lu_checklist_value" IS 'Lookup table - Checklist Values - dropdown answers that can be attributed to a Checklist Item';

-- ------------------------------------------------------------
-- Table: aasbs.lu_closeout_prohibition
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_closeout_prohibition" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(80) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_prohib" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_prohib_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_closeout_prohibition_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_closeout_prohibition FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_closeout_prohibition_hst();

COMMENT ON COLUMN "aasbs"."lu_closeout_prohibition"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_closeout_prohibition"."description" IS 'Display name for the prohibition';
COMMENT ON COLUMN "aasbs"."lu_closeout_prohibition"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_closeout_prohibition"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_closeout_prohibition"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_closeout_prohibition"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_closeout_prohibition"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."lu_closeout_prohibition" IS 'Lookup table - Prohibitions in General - currently only used for Closeout';

-- ------------------------------------------------------------
-- Table: aasbs.lu_collab_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_collab_status" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_collab_stat" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_collab_stat_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_collab_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_collab_status FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_collab_status_hst();

COMMENT ON COLUMN "aasbs"."lu_collab_status"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_collab_status"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_collab_status"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_collab_status"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_collab_status"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_collab_status"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_collab_status"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_collab_status" IS 'Lookup table - Collaboration Statuses - specific to Collaboration Type - see MAP_COLLAB_TYPE_STATUS table';

-- ------------------------------------------------------------
-- Table: aasbs.lu_collab_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_collab_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_collab_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_collab_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_collab_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_collab_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_collab_type_hst();

COMMENT ON COLUMN "aasbs"."lu_collab_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_collab_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_collab_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_collab_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_collab_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_collab_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_collab_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_collab_type" IS 'Lookup table - Collaboration Types, e.g., Resume, Technical Report';

-- ------------------------------------------------------------
-- Table: aasbs.lu_commercial_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_commercial_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_commercial_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_commrcial_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_commercial_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_commercial_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_commercial_type_hst();

COMMENT ON COLUMN "aasbs"."lu_commercial_type"."cd" IS 'Primary key for the table';
COMMENT ON COLUMN "aasbs"."lu_commercial_type"."description" IS 'Description of commercialization type';
COMMENT ON COLUMN "aasbs"."lu_commercial_type"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_commercial_type"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_commercial_type"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_commercial_type"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_commercial_type"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."lu_commercial_type" IS 'This table contains all possible client types';

-- ------------------------------------------------------------
-- Table: aasbs.lu_competition_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_competition_type" (
    "cd" character varying(50) NOT NULL,
    "description" character varying(150) NOT NULL,
    "level_num" integer NOT NULL,
    "parent_competition_type_cd" character varying(30),
    "next_level_dropdown_label" character varying(100),
    "next_level_tooltip" character varying(300),
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_competition_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_competition_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_competition_type_self" FOREIGN KEY (parent_competition_type_cd) REFERENCES aasbs.lu_competition_type(cd)
);

CREATE INDEX ix_lct_parent_competition_type_cd ON aasbs.lu_competition_type USING btree (parent_competition_type_cd);

CREATE TRIGGER lu_competition_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_competition_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_competition_type_hst();

COMMENT ON COLUMN "aasbs"."lu_competition_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_competition_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_competition_type"."level_num" IS 'Not a business column - indicates the hierarchy level in this self-referencing table';
COMMENT ON COLUMN "aasbs"."lu_competition_type"."parent_competition_type_cd" IS 'Foreign Key to table AASBS.LU_PARENT_COMPETITION_TYPE';
COMMENT ON COLUMN "aasbs"."lu_competition_type"."next_level_dropdown_label" IS 'Not a business column - controls Acquisition Planning UI labels';
COMMENT ON COLUMN "aasbs"."lu_competition_type"."next_level_tooltip" IS 'This is an internally used column - helps to define behavior of ASSIST2.0''s Competition Types UI';
COMMENT ON COLUMN "aasbs"."lu_competition_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_competition_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_competition_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_competition_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_competition_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_competition_type" IS 'Lookup table - Types of bid competition (e.g., Full and Open Competition, Sole source SDVOSB)';

-- ------------------------------------------------------------
-- Table: aasbs.lu_consolidated_contract
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_consolidated_contract" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_consolidated_contract" PRIMARY KEY (cd),
    CONSTRAINT "ck_cons_contrct_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_consolidated_contract_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_consolidated_contract FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_consolidated_contract_hst();

COMMENT ON COLUMN "aasbs"."lu_consolidated_contract"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_consolidated_contract"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_consolidated_contract"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_consolidated_contract"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_consolidated_contract"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_consolidated_contract"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_consolidated_contract"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_consolidated_contract"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_consolidated_contract" IS 'Lookup table - Options for whether the acquisition meets the definition of a consolidated requirement, as defined in the FAR.';

-- ------------------------------------------------------------
-- Table: aasbs.lu_continuity_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_continuity_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_continuity_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_continuity_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_continuity_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_continuity_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_continuity_type_hst();

COMMENT ON COLUMN "aasbs"."lu_continuity_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_continuity_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_continuity_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_continuity_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_continuity_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_continuity_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_continuity_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_continuity_type" IS 'Lookup table - Types of continuity with previous Acquisition';

-- ------------------------------------------------------------
-- Table: aasbs.lu_contract_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_contract_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "accrual_universal_holdback_pct" integer DEFAULT (0)::numeric NOT NULL,
    CONSTRAINT "pk_lu_contract_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_contrct_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_contract_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_contract_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_contract_type_hst();

COMMENT ON COLUMN "aasbs"."lu_contract_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_contract_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_contract_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_contract_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_contract_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_contract_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_contract_type"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."lu_contract_type"."accrual_universal_holdback_pct" IS 'Accrual Universal Holdback Percent';

COMMENT ON TABLE "aasbs"."lu_contract_type" IS 'Lookup table - Acquisition contract type - e.g, Firm Fixed Price, Time and Materials';

-- ------------------------------------------------------------
-- Table: aasbs.lu_cor_delegation
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_cor_delegation" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_cor_delegation" PRIMARY KEY (cd),
    CONSTRAINT "ck_cor_delegation_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_cor_delegation_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_cor_delegation FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_cor_delegation_hst();

COMMENT ON COLUMN "aasbs"."lu_cor_delegation"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_cor_delegation"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_cor_delegation"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_cor_delegation"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_cor_delegation"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_cor_delegation"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_cor_delegation"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_cor_delegation"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_cor_delegation" IS 'Lookup table - Options for identifying the method of Contracting Officer''s Representative (COR) delegation being used for this acquisition.';

-- ------------------------------------------------------------
-- Table: aasbs.lu_cost_rate_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_cost_rate_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "ui_visible_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "activity_address_cd" character varying(6),
    CONSTRAINT "pk_lu_cost_rate_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_cost_rate_type_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_cost_rate_type_uivisibleyn" CHECK (ui_visible_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "nn_cost_rate_type_aac" CHECK (activity_address_cd IS NOT NULL),
    CONSTRAINT "fk_cost_rate_type_aac" FOREIGN KEY (activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd)
);

CREATE INDEX ix_lcrt_activity_address_cd ON aasbs.lu_cost_rate_type USING btree (activity_address_cd);

CREATE TRIGGER lu_cost_rate_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_cost_rate_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_cost_rate_type_hst();

COMMENT ON COLUMN "aasbs"."lu_cost_rate_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_cost_rate_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_cost_rate_type"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_cost_rate_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_cost_rate_type"."ui_visible_yn" IS 'Flag indicating whether this value should be visible in the UI';
COMMENT ON COLUMN "aasbs"."lu_cost_rate_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_cost_rate_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_cost_rate_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_cost_rate_type"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."lu_cost_rate_type"."activity_address_cd" IS 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS';

COMMENT ON TABLE "aasbs"."lu_cost_rate_type" IS 'Lookup table - All Labor Cost Rate Types';

-- ------------------------------------------------------------
-- Table: aasbs.lu_country
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_country" (
    "cd" character varying(16) NOT NULL,
    "description" character varying(60) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_country" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_country_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_country_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_country FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_country_hst();

COMMENT ON COLUMN "aasbs"."lu_country"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_country"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_country"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_country"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_country"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_country"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_country"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_country" IS 'Lookup table - List of countries for which GSA has business ties';

-- ------------------------------------------------------------
-- Table: aasbs.lu_email_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_email_status" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_email_status" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_email_status_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_email_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_email_status FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_email_status_hst();

COMMENT ON COLUMN "aasbs"."lu_email_status"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_email_status"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_email_status"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_email_status"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_email_status"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_email_status"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_email_status"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_email_status" IS 'Lookup table - Creation/Delivery status of emails in EMAIL_LOG table';

-- ------------------------------------------------------------
-- Table: aasbs.lu_emergency_acq
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_emergency_acq" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_emergency_acq" PRIMARY KEY (cd),
    CONSTRAINT "ck_emrgncy_acq_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_emergency_acq_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_emergency_acq FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_emergency_acq_hst();

COMMENT ON COLUMN "aasbs"."lu_emergency_acq"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_emergency_acq"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_emergency_acq"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_emergency_acq"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_emergency_acq"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_emergency_acq"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_emergency_acq"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_emergency_acq"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_emergency_acq" IS 'Lookup table - Options for whether the acquisition is being conducted under FAR Part 18 Emergency Acquisition procedures.';

-- ------------------------------------------------------------
-- Table: aasbs.lu_event_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_event_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_event_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_event_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_event_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_event_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_event_type_hst();

COMMENT ON COLUMN "aasbs"."lu_event_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_event_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_event_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_event_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_event_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_event_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_event_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_event_type" IS 'Lookup table - Event Types related to Chronology';

-- ------------------------------------------------------------
-- Table: aasbs.lu_expedited_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_expedited_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "national_emergency_yn" character(1) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_expedited_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_expedited_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_lu_expedited_type_emergyn" CHECK (national_emergency_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_expedited_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_expedited_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_expedited_type_hst();

COMMENT ON COLUMN "aasbs"."lu_expedited_type"."cd" IS 'Numeric code representing a territory';
COMMENT ON COLUMN "aasbs"."lu_expedited_type"."description" IS 'City-State representing the territory';
COMMENT ON COLUMN "aasbs"."lu_expedited_type"."national_emergency_yn" IS 'Classification of the Expedited type';
COMMENT ON COLUMN "aasbs"."lu_expedited_type"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_expedited_type"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_expedited_type"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_expedited_type"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_expedited_type"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."lu_expedited_type" IS 'Lookup table - Driving cause of processing speed for an Acquisition';

-- ------------------------------------------------------------
-- Table: aasbs.lu_fob_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_fob_type" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_fob_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_fobtyp_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_fob_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_fob_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_fob_type_hst();

COMMENT ON COLUMN "aasbs"."lu_fob_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_fob_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_fob_type"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_fob_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_fob_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_fob_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_fob_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_fob_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_fob_type" IS 'Lookup table - Free/Freight on board (FOB) values';

-- ------------------------------------------------------------
-- Table: aasbs.lu_fpds_car_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_fpds_car_status" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_fpds_car_status" PRIMARY KEY (cd),
    CONSTRAINT "ck_fpds_car_stat_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_fpds_car_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_fpds_car_status FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_fpds_car_status_hst();

COMMENT ON COLUMN "aasbs"."lu_fpds_car_status"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_fpds_car_status"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_fpds_car_status"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_fpds_car_status"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_fpds_car_status"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_fpds_car_status"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_fpds_car_status"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_fpds_car_status"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_fpds_car_status" IS 'Lookup table - FDSP Contract Action Report Statuses';

-- ------------------------------------------------------------
-- Table: aasbs.lu_fund
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_fund" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_fund" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_fund_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_fund_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_fund FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_fund_hst();

COMMENT ON COLUMN "aasbs"."lu_fund"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_fund"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_fund"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_fund"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_fund"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_fund"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_fund"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_fund" IS 'Lookup table - GSA Funds, e.g., 285X, 285F';

-- ------------------------------------------------------------
-- Table: aasbs.lu_fund_amend_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_fund_amend_status" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(80) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_fund_amend_status" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_fund_amend_status_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE INDEX ix_cd ON aasbs.lu_fund_amend_status USING btree (cd);

CREATE TRIGGER lu_fund_amend_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_fund_amend_status FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_fund_amend_status_hst();

COMMENT ON COLUMN "aasbs"."lu_fund_amend_status"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_fund_amend_status"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_fund_amend_status"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_fund_amend_status"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_fund_amend_status"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_fund_amend_status"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_fund_amend_status"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_fund_amend_status" IS 'Lookup table - All Funding Amendment statuses (e.g., Draft, Error, Final, Pending)';

-- ------------------------------------------------------------
-- Table: aasbs.lu_fund_category
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_fund_category" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_fund_category" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_fund_category_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_fund_category_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_fund_category FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_fund_category_hst();

COMMENT ON COLUMN "aasbs"."lu_fund_category"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_fund_category"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_fund_category"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_fund_category"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_fund_category"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_fund_category"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_fund_category"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_fund_category" IS 'Lookup table - GSA Funding Categories, e.g., DIRECT_CITE, REIMBURSE';

-- ------------------------------------------------------------
-- Table: aasbs.lu_fund_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_fund_status" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_fund_status" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_fund_status_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_fund_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_fund_status FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_fund_status_hst();

COMMENT ON COLUMN "aasbs"."lu_fund_status"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_fund_status"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_fund_status"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_fund_status"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_fund_status"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_fund_status"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_fund_status"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_fund_status" IS 'Lookup table - All Funding Package Statuses (e.g., Active, Closed, Draft)';

-- ------------------------------------------------------------
-- Table: aasbs.lu_fund_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_fund_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(30) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_fund_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_fund_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE INDEX " idx_lu_fund_type_cd" ON aasbs.lu_fund_type USING btree (description);

CREATE TRIGGER lu_fund_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_fund_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_fund_type_hst();

COMMENT ON COLUMN "aasbs"."lu_fund_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_fund_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_fund_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_fund_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_fund_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_fund_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_fund_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_fund_type" IS 'Lookup table - All Funding Types for  a Funding Package (e.g., Annual, Multi-year, No-year)';

-- ------------------------------------------------------------
-- Table: aasbs.lu_funds_usage
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_funds_usage" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_funds_usage" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_funds_usage_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_funds_usage_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_funds_usage FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_funds_usage_hst();

COMMENT ON COLUMN "aasbs"."lu_funds_usage"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_funds_usage"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_funds_usage"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_funds_usage"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_funds_usage"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_funds_usage"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_funds_usage"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_funds_usage" IS 'Lookup table - All purposes for allocating a funding source';

-- ------------------------------------------------------------
-- Table: aasbs.lu_group_contact_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_group_contact_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_group_contact_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_group_contact_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_group_contact_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_group_contact_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_group_contact_type_hst();

COMMENT ON COLUMN "aasbs"."lu_group_contact_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_group_contact_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_group_contact_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_group_contact_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_group_contact_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_group_contact_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_group_contact_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_group_contact_type" IS 'Lookup table - Category of Group Contact, e.g., CONTRACTING, NATIONAL_FSD';

-- ------------------------------------------------------------
-- Table: aasbs.lu_ia_amend_action
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_ia_amend_action" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_ia_amend_action" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_ia_amend_action_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_ia_amend_action_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_ia_amend_action FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_ia_amend_action_hst();

COMMENT ON COLUMN "aasbs"."lu_ia_amend_action"."cd" IS 'Action code for an Interagency Agreement Amendment action';
COMMENT ON COLUMN "aasbs"."lu_ia_amend_action"."description" IS 'Display names for an Interagency Agreement Amendment action';
COMMENT ON COLUMN "aasbs"."lu_ia_amend_action"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_ia_amend_action"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_ia_amend_action"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_ia_amend_action"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_ia_amend_action"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."lu_ia_amend_action" IS 'This table contains all possible actions for an Interagency Agreement Amendment';

-- ------------------------------------------------------------
-- Table: aasbs.lu_ia_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_ia_status" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_ia_status" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_ia_status_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_ia_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_ia_status FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_ia_status_hst();

COMMENT ON COLUMN "aasbs"."lu_ia_status"."cd" IS 'State or status code on an Interagency Agreement';
COMMENT ON COLUMN "aasbs"."lu_ia_status"."description" IS 'Display names for an Interagency Agreement status';
COMMENT ON COLUMN "aasbs"."lu_ia_status"."active_yn" IS 'Indicator of whether the record is an active lookup record that can still be used for new/updated records';
COMMENT ON COLUMN "aasbs"."lu_ia_status"."sort_order" IS 'Sort order for UI display';
COMMENT ON COLUMN "aasbs"."lu_ia_status"."created_by_user_name" IS 'Record created by this user';
COMMENT ON COLUMN "aasbs"."lu_ia_status"."created_dt" IS 'Record created on this date';
COMMENT ON COLUMN "aasbs"."lu_ia_status"."updated_by_user_name" IS 'Record last updated by this user';
COMMENT ON COLUMN "aasbs"."lu_ia_status"."updated_dt" IS 'Record last updated on this date';

COMMENT ON TABLE "aasbs"."lu_ia_status" IS 'This table contains all possible statuses of an Interagency Agreement';

-- ------------------------------------------------------------
-- Table: aasbs.lu_idv_referenced_class
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_idv_referenced_class" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_idv_refd_class" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_idv_refd_class_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_idv_referenced_class_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_idv_referenced_class FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_idv_referenced_class_hst();

COMMENT ON COLUMN "aasbs"."lu_idv_referenced_class"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_idv_referenced_class"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_idv_referenced_class"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_idv_referenced_class"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_idv_referenced_class"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_idv_referenced_class"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_idv_referenced_class"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_idv_referenced_class" IS 'Lookup table - Types of contract vehicles (e.g., AAS Vehicle, GSA Vehicle)';

-- ------------------------------------------------------------
-- Table: aasbs.lu_idv_type_of_idc
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_idv_type_of_idc" (
    "cd" character varying(12) NOT NULL,
    "description" character varying(70) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_idv_idc_typ" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_idv_idc_typ_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_idv_type_of_idc_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_idv_type_of_idc FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_idv_type_of_idc_hst();

COMMENT ON COLUMN "aasbs"."lu_idv_type_of_idc"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_idv_type_of_idc"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_idv_type_of_idc"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_idv_type_of_idc"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_idv_type_of_idc"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_idv_type_of_idc"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_idv_type_of_idc"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_idv_type_of_idc" IS 'Lookup table - Types of Delivery/Quantity (e.g., IDDQ, IDIQ)';

-- ------------------------------------------------------------
-- Table: aasbs.lu_idv_who_can_use
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_idv_who_can_use" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_idv_who" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_idv_who_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_idv_who_can_use_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_idv_who_can_use FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_idv_who_can_use_hst();

COMMENT ON COLUMN "aasbs"."lu_idv_who_can_use"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_idv_who_can_use"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_idv_who_can_use"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_idv_who_can_use"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_idv_who_can_use"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_idv_who_can_use"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_idv_who_can_use"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_idv_who_can_use" IS 'Lookup table - Types of Agencies that can use the IDV';

-- ------------------------------------------------------------
-- Table: aasbs.lu_instrument_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_instrument_type" (
    "cd" character varying(3) NOT NULL,
    "instrument_type_1char" character varying(1) NOT NULL,
    "description" character varying(200) NOT NULL,
    "proc_phase_cd" character varying(15) NOT NULL,
    "ui_visible_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "short_description" character varying(30) NOT NULL,
    "sort_order" bigint,
    "can_associate_idv_yn" character(1) DEFAULT 'N'::character(1) NOT NULL,
    "fpds_ng_award_type" character varying(10),
    "fpds_ng_award_code" character varying(2),
    "fpds_ng_award_description" character varying(50),
    CONSTRAINT "pk_lu_instrument_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_instr_type_activeyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_lu_instr_type_can_as_idv_yn" CHECK (can_associate_idv_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_lu_instr_type_uivisibleyn" CHECK (ui_visible_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_lu_instr_type_2_proc_phase" FOREIGN KEY (proc_phase_cd) REFERENCES aasbs.lu_proc_phase(cd)
);

CREATE INDEX ix_lit_proc_phase_cd ON aasbs.lu_instrument_type USING btree (proc_phase_cd);
CREATE UNIQUE INDEX uk_lu_instr_type_sort_order ON aasbs.lu_instrument_type USING btree (sort_order);

CREATE TRIGGER lu_instrument_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_instrument_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_instrument_type_hst();

COMMENT ON COLUMN "aasbs"."lu_instrument_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."instrument_type_1char" IS 'Procurement instrument type 1 character code for use in PIID generation';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."proc_phase_cd" IS 'Foreign Key to table AASBS.LU_PROC_PHASE';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."ui_visible_yn" IS 'Flag indicating whether this value should be visible in the UI';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."short_description" IS 'Short Description';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."can_associate_idv_yn" IS 'Flag indicating whether the INSTRUMENT_TYPE record can be associated with IDV (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."fpds_ng_award_type" IS 'Federal Procurement Data System - Next Generation (FPDS) Award Type';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."fpds_ng_award_code" IS 'Federal Procurement Data System - Next Generation (FPDS) Award Code';
COMMENT ON COLUMN "aasbs"."lu_instrument_type"."fpds_ng_award_description" IS 'Federal Procurement Data System - Next Generation (FPDS) Award Description';

COMMENT ON TABLE "aasbs"."lu_instrument_type" IS 'Lookup table - Procurement Instrument types';

-- ------------------------------------------------------------
-- Table: aasbs.lu_intel_community
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_intel_community" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_intel_community" PRIMARY KEY (cd),
    CONSTRAINT "ck_intel_commun_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_intel_community_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_intel_community FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_intel_community_hst();

COMMENT ON COLUMN "aasbs"."lu_intel_community"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_intel_community"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_intel_community"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_intel_community"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_intel_community"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_intel_community"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_intel_community"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_intel_community"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_intel_community" IS 'Lookup table - Options for whether the acquisition is being conducted on behalf of a member of the Intelligence Community, as identified by the Director of National Intelligence.';

-- ------------------------------------------------------------
-- Table: aasbs.lu_line_item_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_line_item_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_line_item_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_line_item_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_line_item_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_line_item_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_line_item_type_hst();

COMMENT ON COLUMN "aasbs"."lu_line_item_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_line_item_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_line_item_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_line_item_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_line_item_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_line_item_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_line_item_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_line_item_type" IS 'Lookup table - All Line Item Types';

-- ------------------------------------------------------------
-- Table: aasbs.lu_loa_change_reason
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_loa_change_reason" (
    "cd" character varying(40) NOT NULL,
    "description" character varying(50) NOT NULL,
    "sort_order" bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_loa_change_reason" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_loa_change_reason_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE UNIQUE INDEX uk_lu_loa_change_reason_sort ON aasbs.lu_loa_change_reason USING btree (sort_order);

CREATE TRIGGER lu_loa_change_reason_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_loa_change_reason FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_loa_change_reason_hst();

COMMENT ON COLUMN "aasbs"."lu_loa_change_reason"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_loa_change_reason"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_loa_change_reason"."sort_order" IS 'Order in which these valuse are to be displayed';
COMMENT ON COLUMN "aasbs"."lu_loa_change_reason"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_loa_change_reason"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_loa_change_reason"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_loa_change_reason"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_loa_change_reason"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_loa_change_reason" IS 'Lookup table - All Reasons a change could be made on an LOA';

-- ------------------------------------------------------------
-- Table: aasbs.lu_loa_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_loa_status" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_loa_status" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_loa_status_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_loa_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_loa_status FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_loa_status_hst();

COMMENT ON COLUMN "aasbs"."lu_loa_status"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_loa_status"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_loa_status"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_loa_status"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_loa_status"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_loa_status"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_loa_status"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_loa_status" IS 'Lookup table - All LOA statuses';

-- ------------------------------------------------------------
-- Table: aasbs.lu_office_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_office_type" (
    "cd" character varying(15) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_office_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_office_type_activeyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_office_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_office_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_office_type_hst();

COMMENT ON COLUMN "aasbs"."lu_office_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_office_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_office_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_office_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_office_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_office_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_office_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_office_type" IS 'Lookup table - Issuing Office Type - Procurement or Administration';

-- ------------------------------------------------------------
-- Table: aasbs.lu_onefund_activity
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_onefund_activity" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "fund_cd" character varying(30) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_onefund_activity" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_onefund_activity_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_lu_onefund_activity_fc" FOREIGN KEY (fund_cd) REFERENCES aasbs.lu_fund(cd)
);

CREATE INDEX ix_loa_fund_cd ON aasbs.lu_onefund_activity USING btree (fund_cd);

CREATE TRIGGER lu_onefund_activity_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_onefund_activity FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_onefund_activity_hst();

COMMENT ON COLUMN "aasbs"."lu_onefund_activity"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_onefund_activity"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_onefund_activity"."fund_cd" IS 'Fund - Foreign key to AASBS.LU_FUND.CD';
COMMENT ON COLUMN "aasbs"."lu_onefund_activity"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_onefund_activity"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_onefund_activity"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_onefund_activity"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_onefund_activity"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_onefund_activity" IS 'Lookup table - All Activity codes';

-- ------------------------------------------------------------
-- Table: aasbs.lu_onefund_program
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_onefund_program" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_onefund_program" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_onefund_program_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_onefund_program_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_onefund_program FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_onefund_program_hst();

COMMENT ON COLUMN "aasbs"."lu_onefund_program"."cd" IS 'alphanumeric primary key for this table';
COMMENT ON COLUMN "aasbs"."lu_onefund_program"."description" IS 'description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_onefund_program"."active_yn" IS 'flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_onefund_program"."created_by_user_name" IS 'oracle login user id who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_onefund_program"."created_dt" IS 'date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_onefund_program"."updated_by_user_name" IS 'oracle login user id who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_onefund_program"."updated_dt" IS 'most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_onefund_program" IS 'lookup table - all program codes from aasbs.lu_onefund';

-- ------------------------------------------------------------
-- Table: aasbs.lu_performance_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_performance_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_performance_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_perf_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_performance_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_performance_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_performance_type_hst();

COMMENT ON COLUMN "aasbs"."lu_performance_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_performance_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_performance_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_performance_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_performance_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_performance_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_performance_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_performance_type" IS 'Lookup table - Acquisition performance type, e.g., Service where PBA is used, not used';

-- ------------------------------------------------------------
-- Table: aasbs.lu_proc_phase
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_proc_phase" (
    "cd" character varying(15) NOT NULL,
    "description" character varying(100) NOT NULL,
    "extension_name" character varying(20) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "parent_cd" character varying(15),
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "short_description" character varying(15) NOT NULL,
    CONSTRAINT "pk_lu_proc_phase" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_proc_phse_activeyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_proc_phase_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_proc_phase FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_proc_phase_hst();

COMMENT ON COLUMN "aasbs"."lu_proc_phase"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_proc_phase"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_proc_phase"."extension_name" IS 'Phase-specific business terminology for the Extention/Modification/Amendment';
COMMENT ON COLUMN "aasbs"."lu_proc_phase"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_proc_phase"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_proc_phase"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_proc_phase"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_proc_phase"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."lu_proc_phase"."parent_cd" IS 'Foreign Key to table AASBS.LU_PROC_PHASE - self referencing to indicate a phase parent';
COMMENT ON COLUMN "aasbs"."lu_proc_phase"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_proc_phase"."short_description" IS 'Short description';

COMMENT ON TABLE "aasbs"."lu_proc_phase" IS 'Lookup table - Procurement Phases, e.g., Acquisition, Solicitation, Award';

-- ------------------------------------------------------------
-- Table: aasbs.lu_processing_speed
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_processing_speed" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_processing_speed" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_processing_speed_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_processing_speed_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_processing_speed FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_processing_speed_hst();

COMMENT ON COLUMN "aasbs"."lu_processing_speed"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_processing_speed"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_processing_speed"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_processing_speed"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_processing_speed"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_processing_speed"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_processing_speed"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_processing_speed" IS 'Lookup table - Speed of processing the Acquisition - Routine or Expedited';

-- ------------------------------------------------------------
-- Table: aasbs.lu_program
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_program" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_program" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_program_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_program_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_program FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_program_hst();

COMMENT ON COLUMN "aasbs"."lu_program"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_program"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_program"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_program"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_program"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_program"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_program"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_program" IS 'Lookup table - GSA Programs, e.g., AAS, ITS_NSD';

-- ------------------------------------------------------------
-- Table: aasbs.lu_reason_for_direct
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_reason_for_direct" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_reason_for_direct" PRIMARY KEY (cd),
    CONSTRAINT "ck_rsn4direct_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_reason_for_direct_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_reason_for_direct FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_reason_for_direct_hst();

COMMENT ON COLUMN "aasbs"."lu_reason_for_direct"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_reason_for_direct"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_reason_for_direct"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_reason_for_direct"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_reason_for_direct"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_reason_for_direct"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_reason_for_direct"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_reason_for_direct"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_reason_for_direct" IS 'Lookup table - All LU_REASON_FOR_DIRECT';

-- ------------------------------------------------------------
-- Table: aasbs.lu_region
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_region" (
    "cd" character varying(2) NOT NULL,
    "description" character varying(60) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_region" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_region_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_region_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_region FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_region_hst();

COMMENT ON COLUMN "aasbs"."lu_region"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_region"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_region"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_region"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_region"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_region"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_region"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_region" IS 'Lookup table - Regions';

-- ------------------------------------------------------------
-- Table: aasbs.lu_responsible_role
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_responsible_role" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(30) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_responsible_role" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_responsible_role_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_responsible_role_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_responsible_role FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_responsible_role_hst();

COMMENT ON COLUMN "aasbs"."lu_responsible_role"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_responsible_role"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_responsible_role"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_responsible_role"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_responsible_role"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_responsible_role"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_responsible_role"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_responsible_role" IS 'Lookup table - User Application/Business Roles, e.g., Client Representative, Contract Specialist';

-- ------------------------------------------------------------
-- Table: aasbs.lu_review_finding
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_review_finding" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_review_finding" PRIMARY KEY (cd),
    CONSTRAINT "ck_revw_finding_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_review_finding_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_review_finding FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_review_finding_hst();

COMMENT ON COLUMN "aasbs"."lu_review_finding"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_review_finding"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_review_finding"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_review_finding"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_review_finding"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_review_finding"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_review_finding"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_review_finding"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_review_finding" IS 'Lookup table - All Possible Review Findings';

-- ------------------------------------------------------------
-- Table: aasbs.lu_review_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_review_type" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_review_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_revwtyp_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_review_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_review_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_review_type_hst();

COMMENT ON COLUMN "aasbs"."lu_review_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_review_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_review_type"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_review_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_review_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_review_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_review_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_review_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_review_type" IS 'Lookup table - All Possible Review Types';

-- ------------------------------------------------------------
-- Table: aasbs.lu_reviewer_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_reviewer_type" (
    "cd" character varying(50) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_reviewer_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_reviewer_typ_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_reviewer_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_reviewer_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_reviewer_type_hst();

COMMENT ON COLUMN "aasbs"."lu_reviewer_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_reviewer_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_reviewer_type"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_reviewer_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_reviewer_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_reviewer_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_reviewer_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_reviewer_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_reviewer_type" IS 'Lookup table - All Possible Reviewer Types';

-- ------------------------------------------------------------
-- Table: aasbs.lu_severability_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_severability_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_severability_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_sever_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_severability_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_severability_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_severability_type_hst();

COMMENT ON COLUMN "aasbs"."lu_severability_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_severability_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_severability_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_severability_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_severability_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_severability_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_severability_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_severability_type" IS 'Lookup table - Contract severability, e.g., Severable/Nonseverable/Mix services';

-- ------------------------------------------------------------
-- Table: aasbs.lu_sf30_mod_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_sf30_mod_type" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_sf30_mod_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_sf30_mod_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_sf30_mod_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_sf30_mod_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_sf30_mod_type_hst();

COMMENT ON COLUMN "aasbs"."lu_sf30_mod_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_sf30_mod_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_sf30_mod_type"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_sf30_mod_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_sf30_mod_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_sf30_mod_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_sf30_mod_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_sf30_mod_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_sf30_mod_type" IS 'Lookup table - All Invoice Statuses';

-- ------------------------------------------------------------
-- Table: aasbs.lu_small_business_admin
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_small_business_admin" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_small_business_admin" PRIMARY KEY (cd),
    CONSTRAINT "ck_small_business_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_small_business_admin_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_small_business_admin FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_small_business_admin_hst();

COMMENT ON COLUMN "aasbs"."lu_small_business_admin"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_small_business_admin"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_small_business_admin"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_small_business_admin"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_small_business_admin"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_small_business_admin"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_small_business_admin"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_small_business_admin"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_small_business_admin" IS 'Lookup table - Options for whether the acquisition is being conducted using the Small Business Administration's (SBA's) Small Business Innovation Research (SBIR) / Small Business Technology Transfer (STTR) procedures.';

-- ------------------------------------------------------------
-- Table: aasbs.lu_sol_amend_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_sol_amend_status" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_sol_amend_status" PRIMARY KEY (cd),
    CONSTRAINT "ck_solamndstat_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_sol_amend_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_sol_amend_status FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_sol_amend_status_hst();

COMMENT ON COLUMN "aasbs"."lu_sol_amend_status"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_sol_amend_status"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_sol_amend_status"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_sol_amend_status"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_sol_amend_status"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_sol_amend_status"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_sol_amend_status"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_sol_amend_status"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_sol_amend_status" IS 'Lookup table - All Solicitation Amendment Statuses';

-- ------------------------------------------------------------
-- Table: aasbs.lu_sol_posting_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_sol_posting_type" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_sol_posting_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_solposttyp_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_sol_posting_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_sol_posting_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_sol_posting_type_hst();

COMMENT ON COLUMN "aasbs"."lu_sol_posting_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_sol_posting_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_sol_posting_type"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_sol_posting_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_sol_posting_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_sol_posting_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_sol_posting_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_sol_posting_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_sol_posting_type" IS 'Lookup table - All Solicitation Posting Types';

-- ------------------------------------------------------------
-- Table: aasbs.lu_sol_response_aac_visibility
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_sol_response_aac_visibility" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_sol_response_aac_vis" PRIMARY KEY (cd),
    CONSTRAINT "ck_solrespvisaac_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_sol_response_aac_visibility_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_sol_response_aac_visibility FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_sol_response_aac_visibility_hst();

COMMENT ON COLUMN "aasbs"."lu_sol_response_aac_visibility"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_sol_response_aac_visibility"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_sol_response_aac_visibility"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_sol_response_aac_visibility"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_sol_response_aac_visibility"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_sol_response_aac_visibility"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_sol_response_aac_visibility"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_sol_response_aac_visibility"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_sol_response_aac_visibility" IS 'Lookup table - All Solicitation Response visibilities for AACs';

-- ------------------------------------------------------------
-- Table: aasbs.lu_sol_response_cli_visibility
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_sol_response_cli_visibility" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_sol_response_cli_vis" PRIMARY KEY (cd),
    CONSTRAINT "ck_solrespviscli_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_sol_response_cli_visibility_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_sol_response_cli_visibility FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_sol_response_cli_visibility_hst();

COMMENT ON COLUMN "aasbs"."lu_sol_response_cli_visibility"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_sol_response_cli_visibility"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_sol_response_cli_visibility"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_sol_response_cli_visibility"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_sol_response_cli_visibility"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_sol_response_cli_visibility"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_sol_response_cli_visibility"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_sol_response_cli_visibility"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_sol_response_cli_visibility" IS 'Lookup table - All Solicitation Response visibilities for Clients';

-- ------------------------------------------------------------
-- Table: aasbs.lu_sol_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_sol_status" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_sol_status" PRIMARY KEY (cd),
    CONSTRAINT "ck_solstat_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_sol_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_sol_status FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_sol_status_hst();

COMMENT ON COLUMN "aasbs"."lu_sol_status"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_sol_status"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_sol_status"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_sol_status"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_sol_status"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_sol_status"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_sol_status"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_sol_status"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_sol_status" IS 'Lookup table - All Solicitation Statuses';

-- ------------------------------------------------------------
-- Table: aasbs.lu_state
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_state" (
    "cd" character varying(16) NOT NULL,
    "description" character varying(60) NOT NULL,
    "is_usa_yn" character(1) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_state" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_state_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_lu_state_isusayn" CHECK (is_usa_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_state_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_state FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_state_hst();

COMMENT ON COLUMN "aasbs"."lu_state"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_state"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_state"."is_usa_yn" IS 'Flag indicating whether a state is one of the 50 US States (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_state"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_state"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_state"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_state"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_state"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_state" IS 'Lookup table - 50 US States + entities outside the United States, e.g., Virgin Islands';

-- ------------------------------------------------------------
-- Table: aasbs.lu_subsystem
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_subsystem" (
    "cd" character varying(30) NOT NULL,
    "system_cd" character varying(15) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_subsystem" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_subsystem_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_lu_subsystem_system" FOREIGN KEY (system_cd) REFERENCES aasbs.lu_system(cd)
);

CREATE INDEX ix_ls_system_cd ON aasbs.lu_subsystem USING btree (system_cd);

CREATE TRIGGER lu_subsystem_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_subsystem FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_subsystem_hst();

COMMENT ON COLUMN "aasbs"."lu_subsystem"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_subsystem"."system_cd" IS 'Foreign Key to table AASBS.LU_SYSTEM';
COMMENT ON COLUMN "aasbs"."lu_subsystem"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_subsystem"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_subsystem"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_subsystem"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_subsystem"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_subsystem"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_subsystem" IS 'Lookup table - SubSystems within the ASSIST Application';

-- ------------------------------------------------------------
-- Table: aasbs.lu_supply_chain_risk
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_supply_chain_risk" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_supply_chain_risk" PRIMARY KEY (cd),
    CONSTRAINT "ck_supply_chain_rsk_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_supply_chain_risk_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_supply_chain_risk FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_supply_chain_risk_hst();

COMMENT ON COLUMN "aasbs"."lu_supply_chain_risk"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_supply_chain_risk"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_supply_chain_risk"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_supply_chain_risk"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_supply_chain_risk"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_supply_chain_risk"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_supply_chain_risk"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_supply_chain_risk"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_supply_chain_risk" IS 'Lookup table - Options for whether the acquisition is required at Cybersecurity Maturity Model Certifications (CMMC) Level 1, 2, 3, 4, or 5.';

-- ------------------------------------------------------------
-- Table: aasbs.lu_surveillance_spec
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_surveillance_spec" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(200) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_surveillance_spec" PRIMARY KEY (cd),
    CONSTRAINT "ck_surveil_spec_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_surveillance_spec_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_surveillance_spec FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_surveillance_spec_hst();

COMMENT ON COLUMN "aasbs"."lu_surveillance_spec"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_surveillance_spec"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_surveillance_spec"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_surveillance_spec"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_surveillance_spec"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_surveillance_spec"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_surveillance_spec"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_surveillance_spec"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_surveillance_spec" IS 'Lookup table - Options for whether the acquisition requires a Contractor Security Classification Specification (DD-254).';

-- ------------------------------------------------------------
-- Table: aasbs.lu_table_name
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_table_name" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_table_name" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_table_name_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_table_name_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_table_name FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_table_name_hst();

COMMENT ON COLUMN "aasbs"."lu_table_name"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_table_name"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_table_name"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_table_name"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_table_name"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_table_name"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_table_name"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_table_name" IS 'Lookup table - List of AASBS tables and views that are referenced by "CENTRAL" tables, currently CENTRAL_COMMENT';

-- ------------------------------------------------------------
-- Table: aasbs.lu_transaction_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_transaction_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_transaction_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_transaction_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_transaction_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_transaction_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_transaction_type_hst();

COMMENT ON COLUMN "aasbs"."lu_transaction_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_transaction_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_transaction_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_transaction_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_transaction_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_transaction_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_transaction_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_transaction_type" IS 'Lookup table - All Transaction Types in LOA_Ledger table';

-- ------------------------------------------------------------
-- Table: aasbs.lu_transfer
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_transfer" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_transfer" PRIMARY KEY (cd),
    CONSTRAINT "ck_transfer_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_transfer_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_transfer FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_transfer_hst();

COMMENT ON COLUMN "aasbs"."lu_transfer"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_transfer"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_transfer"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_transfer"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_transfer"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_transfer"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_transfer"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_transfer"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_transfer" IS 'Lookup table - Options for whether the acquisition represents the transfer of contract administration responsibilities.';

-- ------------------------------------------------------------
-- Table: aasbs.lu_unit_of_measure
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_unit_of_measure" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(100) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_unit_of_measure" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_unit_of_measure_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE UNIQUE INDEX uk_lu_unit_of_measure_sort ON aasbs.lu_unit_of_measure USING btree (sort_order);

CREATE TRIGGER lu_unit_of_measure_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_unit_of_measure FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_unit_of_measure_hst();

COMMENT ON COLUMN "aasbs"."lu_unit_of_measure"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_unit_of_measure"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_unit_of_measure"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs"."lu_unit_of_measure"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_unit_of_measure"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_unit_of_measure"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_unit_of_measure"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_unit_of_measure"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_unit_of_measure" IS 'Lookup table - All Units of Measure';

-- ------------------------------------------------------------
-- Table: aasbs.lu_vehicle_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lu_vehicle_type" (
    "cd" character varying(30) NOT NULL,
    "description" character varying(50) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_vehicle_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_lu_vehicle_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_vehicle_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lu_vehicle_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lu_vehicle_type_hst();

COMMENT ON COLUMN "aasbs"."lu_vehicle_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs"."lu_vehicle_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs"."lu_vehicle_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."lu_vehicle_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs"."lu_vehicle_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lu_vehicle_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."lu_vehicle_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."lu_vehicle_type" IS 'Lookup table - Contract Vehicle Types, e.g., Single Award / Multiple Agency Use';

-- ------------------------------------------------------------
-- Table: aasbs.lus_psc
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."lus_psc" (
    "psc_id" bigint,
    "psc_code" character varying(50),
    "psc_name" character varying(200),
    "psc_full_name" character varying(200),
    "psc_include" character varying(2000),
    "active" character(1),
    "psc_exclue" character varying(2000),
    "parent_psc_code" character varying(50),
    "active_start_dt" timestamp(6) without time zone,
    "update_dt" timestamp(6) without time zone,
    "level_1_category_name" character varying(200),
    "level_2_category_name" character varying(200),
    "psc_type" character varying(10),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "level_1_category" character varying(255),
    "level_2_category" character varying(255),
    "active_end_dt" timestamp(6) without time zone,
    CONSTRAINT "fk_lus_psc_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_lus_psc_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_lp_created_by_user_id ON aasbs.lus_psc USING btree (created_by_user_id);
CREATE INDEX ix_lp_updated_by_user_id ON aasbs.lus_psc USING btree (updated_by_user_id);

CREATE TRIGGER lus_psc_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.lus_psc FOR EACH ROW EXECUTE FUNCTION aasbs.trg_lus_psc_hst();

COMMENT ON COLUMN "aasbs"."lus_psc"."psc_id" IS 'Numeric ID column but all foreign key references to this table should point to the PSC_CODE column';
COMMENT ON COLUMN "aasbs"."lus_psc"."psc_code" IS 'GSA-defined codes - this should be considered the Primary Key column for this table';
COMMENT ON COLUMN "aasbs"."lus_psc"."psc_name" IS 'Externally defined column';
COMMENT ON COLUMN "aasbs"."lus_psc"."psc_full_name" IS 'Externally defined column';
COMMENT ON COLUMN "aasbs"."lus_psc"."psc_include" IS 'Externally defined column';
COMMENT ON COLUMN "aasbs"."lus_psc"."active" IS 'Flag indicating whether a record is active (=Y) or not (=N) (Externally defined and imported to this table)';
COMMENT ON COLUMN "aasbs"."lus_psc"."psc_exclue" IS 'Externally defined column';
COMMENT ON COLUMN "aasbs"."lus_psc"."parent_psc_code" IS 'Externally defined column';
COMMENT ON COLUMN "aasbs"."lus_psc"."active_start_dt" IS 'Date indicating when this value became Active (Externally defined and imported to this table)';
COMMENT ON COLUMN "aasbs"."lus_psc"."update_dt" IS 'Externally defined column';
COMMENT ON COLUMN "aasbs"."lus_psc"."level_1_category_name" IS 'Externally defined column';
COMMENT ON COLUMN "aasbs"."lus_psc"."level_2_category_name" IS 'Externally defined column';
COMMENT ON COLUMN "aasbs"."lus_psc"."psc_type" IS 'Externally defined column';
COMMENT ON COLUMN "aasbs"."lus_psc"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."lus_psc"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."lus_psc"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."lus_psc"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."lus_psc"."level_1_category" IS 'The level 1 category of the PSC Code. A number.';
COMMENT ON COLUMN "aasbs"."lus_psc"."level_2_category" IS 'The level 2 category of the PSC Code. A number.';
COMMENT ON COLUMN "aasbs"."lus_psc"."active_end_dt" IS 'End date of the PSC Code';

COMMENT ON TABLE "aasbs"."lus_psc" IS 'Synchronized Lookup Table ("LUS_") - Product Service Codes (PSC) - Updated nightly by the ASSIST application layer';

-- ------------------------------------------------------------
-- Table: aasbs.map_aac_onefund
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."map_aac_onefund" (
    "activity_address_cd" character varying(8) NOT NULL,
    "onefund_cd" character varying(8) NOT NULL,
    "cost_element_cd" character varying(3) NOT NULL,
    "object_class_cd" character varying(2) NOT NULL,
    "onefund_program_cd" character varying(4) NOT NULL,
    "onefund_activity_cd" character varying(5) NOT NULL,
    "fund_cd" character varying(4) NOT NULL,
    "active_yn" character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_map_aac_onefund" PRIMARY KEY (activity_address_cd, onefund_cd),
    CONSTRAINT "fk_mao_2_aac" FOREIGN KEY (activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd),
    CONSTRAINT "fk_mao_2_cec" FOREIGN KEY (cost_element_cd) REFERENCES aasbs.lu_cost_element(cd),
    CONSTRAINT "fk_mao_2_objclass" FOREIGN KEY (object_class_cd) REFERENCES aasbs.lu_object_classes(cd),
    CONSTRAINT "fk_mao_2_of_activity" FOREIGN KEY (onefund_activity_cd) REFERENCES aasbs.lu_onefund_activity(cd),
    CONSTRAINT "fk_mao_2_of_fund" FOREIGN KEY (fund_cd) REFERENCES aasbs.lu_fund(cd),
    CONSTRAINT "fk_mao_2_of_program" FOREIGN KEY (onefund_program_cd) REFERENCES aasbs.lu_onefund_program(cd),
    CONSTRAINT "fk_mao_2_onefund" FOREIGN KEY (onefund_cd) REFERENCES aasbs.lu_onefund(cd)
);

CREATE INDEX ix_mao_cost_element_cd ON aasbs.map_aac_onefund USING btree (cost_element_cd);
CREATE INDEX ix_mao_fund_cd ON aasbs.map_aac_onefund USING btree (fund_cd);
CREATE INDEX ix_mao_object_class_cd ON aasbs.map_aac_onefund USING btree (object_class_cd);
CREATE INDEX ix_mao_onefund_activity_cd ON aasbs.map_aac_onefund USING btree (onefund_activity_cd);
CREATE INDEX ix_mao_onefund_program_cd ON aasbs.map_aac_onefund USING btree (onefund_program_cd);
CREATE UNIQUE INDEX uk_map_aac_onefund_activity_address_cd ON aasbs.map_aac_onefund USING btree (activity_address_cd);

CREATE TRIGGER map_aac_onefund_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.map_aac_onefund FOR EACH ROW EXECUTE FUNCTION aasbs.trg_map_aac_onefund_hst();

COMMENT ON COLUMN "aasbs"."map_aac_onefund"."active_yn" IS 'Contains whether the mapping is active';
COMMENT ON COLUMN "aasbs"."map_aac_onefund"."created_by_user_name" IS 'User that created this record';
COMMENT ON COLUMN "aasbs"."map_aac_onefund"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."map_aac_onefund"."updated_by_user_name" IS 'User that most recently updated this record';
COMMENT ON COLUMN "aasbs"."map_aac_onefund"."updated_dt" IS 'Most recent date this database record was updated';

-- ------------------------------------------------------------
-- Table: aasbs.map_onefund_program_accrual_income_doc_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."map_onefund_program_accrual_income_doc_type" (
    "onefund_program_cd" character varying(8) NOT NULL,
    "accrual_income_doc_type_cd" character varying(8),
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1),
    CONSTRAINT "ck_map_onefund_program_accrual_income_doc_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "nn_map_onefund_program_accrual_income_doc_type_actyn" CHECK (active_yn IS NOT NULL),
    CONSTRAINT "fk_map_onefund_program_accrual_income_doc_type_aidtc" FOREIGN KEY (accrual_income_doc_type_cd) REFERENCES aasbs.lu_accrual_income_doc_type(cd),
    CONSTRAINT "fk_map_onefund_program_accrual_income_doc_type_ofpc" FOREIGN KEY (onefund_program_cd) REFERENCES aasbs.lu_onefund_program(cd)
);

CREATE INDEX ix_mopa_accrual_income_doc_type_cd ON aasbs.map_onefund_program_accrual_income_doc_type USING btree (accrual_income_doc_type_cd);
CREATE UNIQUE INDEX un_map_onefund_program_accrual_income_doc_type_actyn ON aasbs.map_onefund_program_accrual_income_doc_type USING btree (onefund_program_cd);

CREATE TRIGGER map_onefund_program_accrual_income_doc_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.map_onefund_program_accrual_income_doc_type FOR EACH ROW EXECUTE FUNCTION aasbs.trg_map_onefund_program_accrual_income_doc_type_hst();

COMMENT ON COLUMN "aasbs"."map_onefund_program_accrual_income_doc_type"."onefund_program_cd" IS 'Foreign Key to table aasbs.lu_onefund_program';
COMMENT ON COLUMN "aasbs"."map_onefund_program_accrual_income_doc_type"."accrual_income_doc_type_cd" IS 'Foreign Key to table aasbs.lu_accrual_income_doc_type';
COMMENT ON COLUMN "aasbs"."map_onefund_program_accrual_income_doc_type"."created_by_user_name" IS 'oracle login user id who created this database table record';
COMMENT ON COLUMN "aasbs"."map_onefund_program_accrual_income_doc_type"."created_dt" IS 'date this database record was created';
COMMENT ON COLUMN "aasbs"."map_onefund_program_accrual_income_doc_type"."updated_by_user_name" IS 'oracle login user id who most recently updated this database table record';
COMMENT ON COLUMN "aasbs"."map_onefund_program_accrual_income_doc_type"."updated_dt" IS 'most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."map_onefund_program_accrual_income_doc_type"."active_yn" IS 'flag indicating whether a record is active (=Y) or not (=N)';

COMMENT ON TABLE "aasbs"."map_onefund_program_accrual_income_doc_type" IS 'Map table - Mapping of onefund program code to accrual income document type';

-- ------------------------------------------------------------
-- Table: aasbs.piid
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."piid" (
    "id" bigint NOT NULL,
    "activity_address_cd" character varying(6) NOT NULL,
    "fiscal_year" integer NOT NULL,
    "instrument_type_cd" character varying(3) NOT NULL,
    "instrument_type_1char" character varying(1) NOT NULL,
    "sequence" bigint NOT NULL,
    "piid" character varying(50) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "apex_emp_org_id" bigint,
    CONSTRAINT "pk_piid" PRIMARY KEY (id),
    CONSTRAINT "fk_piid_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_piid_eoi" FOREIGN KEY (apex_emp_org_id) REFERENCES assist.lu_emp_orgs(emp_org_id),
    CONSTRAINT "fk_piid_lu_aac" FOREIGN KEY (activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd),
    CONSTRAINT "fk_piid_lu_instr_type" FOREIGN KEY (instrument_type_cd) REFERENCES aasbs.lu_instrument_type(cd),
    CONSTRAINT "fk_piid_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_p_apex_emp_org_id ON aasbs.piid USING btree (apex_emp_org_id);
CREATE INDEX ix_piid_cbui ON aasbs.piid USING btree (created_by_user_id);
CREATE INDEX ix_piid_lu_instr_type ON aasbs.piid USING btree (instrument_type_cd);
CREATE INDEX ix_piid_ubui ON aasbs.piid USING btree (updated_by_user_id);
CREATE UNIQUE INDEX ixu_piid_aac_fy_itc_rn ON aasbs.piid USING btree (activity_address_cd, fiscal_year, instrument_type_cd, sequence);
CREATE UNIQUE INDEX uk_piid_piid ON aasbs.piid USING btree (piid);

CREATE TRIGGER piid_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.piid FOR EACH ROW EXECUTE FUNCTION aasbs.trg_piid_hst();

COMMENT ON COLUMN "aasbs"."piid"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.PIID_SEQ';
COMMENT ON COLUMN "aasbs"."piid"."activity_address_cd" IS 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE';
COMMENT ON COLUMN "aasbs"."piid"."fiscal_year" IS 'Fiscal Year of PIID';
COMMENT ON COLUMN "aasbs"."piid"."instrument_type_cd" IS 'Foreign Key to table AASBS.LU_INSTRUMENT_TYPE';
COMMENT ON COLUMN "aasbs"."piid"."instrument_type_1char" IS 'Single digit instrument type code used in PIID';
COMMENT ON COLUMN "aasbs"."piid"."sequence" IS 'Unique sequence number allocated for PIID for combination of AAC, FY, Instrument Code 1 Char';
COMMENT ON COLUMN "aasbs"."piid"."piid" IS 'The generated unique PIID';
COMMENT ON COLUMN "aasbs"."piid"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."piid"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."piid"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."piid"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."piid"."apex_emp_org_id" IS 'Apex office associated to this piid';

COMMENT ON TABLE "aasbs"."piid" IS 'Generated Procurement Instrument IDs';

-- ------------------------------------------------------------
-- Table: aasbs.piid_ext
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."piid_ext" (
    "id" bigint NOT NULL,
    "piid_id" bigint NOT NULL,
    "parent_ext_id" bigint,
    "office_type_cd" character varying(15),
    "extension_sequence" bigint NOT NULL,
    "extension" character varying(6) NOT NULL,
    "subsystem_cd" character varying(30) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_piid_ext" PRIMARY KEY (id),
    CONSTRAINT "fk_piid_ext_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_piid_ext_lu_off_type" FOREIGN KEY (office_type_cd) REFERENCES aasbs.lu_office_type(cd),
    CONSTRAINT "fk_piid_ext_lu_subsystem" FOREIGN KEY (subsystem_cd) REFERENCES aasbs.lu_subsystem(cd),
    CONSTRAINT "fk_piid_ext_piid" FOREIGN KEY (piid_id) REFERENCES aasbs.piid(id),
    CONSTRAINT "fk_piid_ext_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_piid_ext_cbui ON aasbs.piid_ext USING btree (created_by_user_id);
CREATE INDEX ix_piid_ext_lu_off_type ON aasbs.piid_ext USING btree (office_type_cd);
CREATE INDEX ix_piid_ext_lu_system ON aasbs.piid_ext USING btree (subsystem_cd);
CREATE INDEX ix_piid_ext_ubui ON aasbs.piid_ext USING btree (updated_by_user_id);
CREATE UNIQUE INDEX ixu_piid_ext_ui_en ON aasbs.piid_ext USING btree (piid_id, extension_sequence);

CREATE TRIGGER piid_ext_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.piid_ext FOR EACH ROW EXECUTE FUNCTION aasbs.trg_piid_ext_hst();

COMMENT ON COLUMN "aasbs"."piid_ext"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.PIID_EXT_SEQ';
COMMENT ON COLUMN "aasbs"."piid_ext"."piid_id" IS 'Foreign Key to table AASBS.PIID';
COMMENT ON COLUMN "aasbs"."piid_ext"."parent_ext_id" IS 'Self Referencing Foreigh key - ';
COMMENT ON COLUMN "aasbs"."piid_ext"."office_type_cd" IS 'Foreign Key to table AASBS.LU_OFFICE_TYPE';
COMMENT ON COLUMN "aasbs"."piid_ext"."extension_sequence" IS 'Extension / Mod sequence - purpose of this table';
COMMENT ON COLUMN "aasbs"."piid_ext"."extension" IS 'The generated extension for the PIID';
COMMENT ON COLUMN "aasbs"."piid_ext"."subsystem_cd" IS 'Foreign Key to table AASBS.LU_SYSTEM - system that requested the PIID';
COMMENT ON COLUMN "aasbs"."piid_ext"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."piid_ext"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."piid_ext"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."piid_ext"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."piid_ext" IS 'Generated next sequential mod/amendment extension to a Procurement Instrument IDs';

-- ------------------------------------------------------------
-- Table: aasbs.review
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."review" (
    "id" bigint NOT NULL,
    "review_type_cd" character varying(20) NOT NULL,
    "review_parent_table_name_cd" character varying(30) NOT NULL,
    "solicit_id" bigint,
    "award_mod_id" bigint,
    "attach_hash_id" character varying(64),
    "draft_review_yn" character(1) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_review" PRIMARY KEY (id),
    CONSTRAINT "ck_review_draft_yn" CHECK (draft_review_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_review_fk_col_chk_parenttab" CHECK (review_parent_table_name_cd::text = 'AWARD_MOD'::text AND award_mod_id IS NOT NULL AND solicit_id IS NULL OR review_parent_table_name_cd::text = 'SOLICIT'::text AND award_mod_id IS NULL AND solicit_id IS NOT NULL),
    CONSTRAINT "ck_review_fk_col_chk_rev_type" CHECK ((review_type_cd::text = ANY (ARRAY['AWARD'::character varying::text, 'FUNDING_CERTIF'::character varying::text])) AND award_mod_id IS NOT NULL AND solicit_id IS NULL OR review_type_cd::text = 'PRE_SOLICITATION'::text AND award_mod_id IS NULL AND solicit_id IS NOT NULL),
    CONSTRAINT "fk_review_awd_mod" FOREIGN KEY (award_mod_id) REFERENCES aasbs.award_mod(id),
    CONSTRAINT "fk_review_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_review_parent_tabname" FOREIGN KEY (review_parent_table_name_cd) REFERENCES aasbs.lu_table_name(cd),
    CONSTRAINT "fk_review_revw_typ" FOREIGN KEY (review_type_cd) REFERENCES aasbs.lu_review_type(cd),
    CONSTRAINT "fk_review_solicit" FOREIGN KEY (solicit_id) REFERENCES aasbs.solicit(id),
    CONSTRAINT "fk_review_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_review_awd_mod ON aasbs.review USING btree (award_mod_id);
CREATE INDEX ix_review_cbui ON aasbs.review USING btree (created_by_user_id);
CREATE INDEX ix_review_parent_tabname ON aasbs.review USING btree (review_parent_table_name_cd);
CREATE INDEX ix_review_revw_typ ON aasbs.review USING btree (review_type_cd);
CREATE INDEX ix_review_sol_id ON aasbs.review USING btree (solicit_id);
CREATE INDEX ix_review_ubui ON aasbs.review USING btree (updated_by_user_id);

CREATE TRIGGER review_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.review FOR EACH ROW EXECUTE FUNCTION aasbs.trg_review_hst();

COMMENT ON COLUMN "aasbs"."review"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.REVIEW_SEQ';
COMMENT ON COLUMN "aasbs"."review"."review_type_cd" IS 'Foreign Key to table AASBS.LU_SOL_REVIEW_TYPE';
COMMENT ON COLUMN "aasbs"."review"."review_parent_table_name_cd" IS 'Foreign Key to table AASBS.LU_TABLE_NAME';
COMMENT ON COLUMN "aasbs"."review"."solicit_id" IS 'Foreign Key to table AASBS.SOLICIT';
COMMENT ON COLUMN "aasbs"."review"."award_mod_id" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs"."review"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."review"."draft_review_yn" IS 'Flag indicating whether the Review Form is (=Y) or is not (=N) in DRAFT state';
COMMENT ON COLUMN "aasbs"."review"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."review"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."review"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."review"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."review" IS 'Contains high-level review details for all reviews within the ASSIST application';

-- ------------------------------------------------------------
-- Table: aasbs.review_determination
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."review_determination" (
    "id" bigint NOT NULL,
    "review_id" bigint NOT NULL,
    "reviewer_determination_num" character varying(20) NOT NULL,
    "review_finding_cd" character varying(20) NOT NULL,
    "reviewer_type_cd" character varying(35) NOT NULL,
    "review_is_required_yn" character(1) NOT NULL,
    "reviewer_comments" character varying(2000),
    "reviewer_user_id" character varying(16),
    "reviewer_full_name" character varying(100),
    "reviewer_started_dt" timestamp(6) without time zone NOT NULL,
    "reviewer_completed_dt" timestamp(6) without time zone,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_review_determ" PRIMARY KEY (id),
    CONSTRAINT "ck_review_determ_reqyn" CHECK (review_is_required_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_revw_determ_revwr_user_cols" CHECK ((
CASE
    WHEN reviewer_user_id IS NULL THEN 0
    ELSE 1
END +
CASE
    WHEN reviewer_full_name IS NULL THEN 0
    ELSE 1
END) = 1),
    CONSTRAINT "fk_review_determ_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_review_determ_review_id" FOREIGN KEY (review_id) REFERENCES aasbs.review(id),
    CONSTRAINT "fk_review_determ_revw_finding" FOREIGN KEY (review_finding_cd) REFERENCES aasbs.lu_review_finding(cd),
    CONSTRAINT "fk_review_determ_revw_userid" FOREIGN KEY (reviewer_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_review_determ_revwr_typ" FOREIGN KEY (reviewer_type_cd) REFERENCES aasbs.lu_reviewer_type(cd),
    CONSTRAINT "fk_review_determ_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_review_determ_cbui ON aasbs.review_determination USING btree (created_by_user_id);
CREATE INDEX ix_review_determ_review_id ON aasbs.review_determination USING btree (review_id);
CREATE INDEX ix_review_determ_revw_find ON aasbs.review_determination USING btree (review_finding_cd);
CREATE INDEX ix_review_determ_revw_userid ON aasbs.review_determination USING btree (reviewer_user_id);
CREATE INDEX ix_review_determ_revwer_typ ON aasbs.review_determination USING btree (reviewer_type_cd);
CREATE INDEX ix_review_determ_ubui ON aasbs.review_determination USING btree (updated_by_user_id);

CREATE TRIGGER review_determination_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.review_determination FOR EACH ROW EXECUTE FUNCTION aasbs.trg_review_determination_hst();

COMMENT ON COLUMN "aasbs"."review_determination"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.REVIEW_SEQ';
COMMENT ON COLUMN "aasbs"."review_determination"."review_id" IS 'Foreign Key to table AASBS.REVIEW';
COMMENT ON COLUMN "aasbs"."review_determination"."reviewer_determination_num" IS 'Reviewer number within the larger Review';
COMMENT ON COLUMN "aasbs"."review_determination"."review_finding_cd" IS 'Foreign Key to table AASBS.LU_REVIEW_FINDING';
COMMENT ON COLUMN "aasbs"."review_determination"."reviewer_type_cd" IS 'Foreign Key to table AASBS.LU_REVIEWER_TYPE';
COMMENT ON COLUMN "aasbs"."review_determination"."review_is_required_yn" IS 'Flag indicating whether a Review is required (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."review_determination"."reviewer_comments" IS 'Reviewer Comments';
COMMENT ON COLUMN "aasbs"."review_determination"."reviewer_user_id" IS 'Foreign Key to table AASBS.REVIEWER_USER - when this column has a value - REVIEWER_FULL_NAME must be null';
COMMENT ON COLUMN "aasbs"."review_determination"."reviewer_full_name" IS 'Reviewer''s full name - when this column has a value - REVIEWER_USER_ID must be null';
COMMENT ON COLUMN "aasbs"."review_determination"."reviewer_started_dt" IS 'Date the Solicitation Review was started';
COMMENT ON COLUMN "aasbs"."review_determination"."reviewer_completed_dt" IS 'Date the Solicitation Review was completed';
COMMENT ON COLUMN "aasbs"."review_determination"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."review_determination"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."review_determination"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."review_determination"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."review_determination" IS 'Contains review details entered for each individual reviewer within the larger Review';

-- ------------------------------------------------------------
-- Table: aasbs.service_charge
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."service_charge" (
    "id" bigint NOT NULL,
    "act_number" character varying(15),
    "is_credit_yn" character(1) NOT NULL,
    "credit_to_service_charge_id" bigint,
    "omis_billings_bill_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_service_charge" PRIMARY KEY (id),
    CONSTRAINT "ck_service_charge_credit_yn" CHECK (is_credit_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_service_charge_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_service_charge_credit_sc_id" FOREIGN KEY (credit_to_service_charge_id) REFERENCES aasbs.service_charge(id),
    CONSTRAINT "fk_service_charge_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_service_charge_act_num ON aasbs.service_charge USING btree (act_number);
CREATE INDEX ix_service_charge_cbui ON aasbs.service_charge USING btree (created_by_user_id);
CREATE INDEX ix_service_charge_credit_sc_id ON aasbs.service_charge USING btree (credit_to_service_charge_id);
CREATE INDEX ix_service_charge_obbi ON aasbs.service_charge USING btree (omis_billings_bill_id);
CREATE INDEX ix_service_charge_ubui ON aasbs.service_charge USING btree (updated_by_user_id);

CREATE TRIGGER service_charge_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.service_charge FOR EACH ROW EXECUTE FUNCTION aasbs.trg_service_charge_hst();

COMMENT ON COLUMN "aasbs"."service_charge"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.SERVICE_CHARGE_SEQ';
COMMENT ON COLUMN "aasbs"."service_charge"."act_number" IS 'Accounting Control Transaction number';
COMMENT ON COLUMN "aasbs"."service_charge"."is_credit_yn" IS 'Identifies if charge is a bill or a credit';
COMMENT ON COLUMN "aasbs"."service_charge"."credit_to_service_charge_id" IS 'If a credit, identifies the SERVICE_CHARGE.ID which is credited.';
COMMENT ON COLUMN "aasbs"."service_charge"."omis_billings_bill_id" IS 'For Service Charges which also exist in OMIS, foreign key to OMIS.BILLINGS.BILL_ID';
COMMENT ON COLUMN "aasbs"."service_charge"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."service_charge"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."service_charge"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."service_charge"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."service_charge" IS 'Service Charge details';

-- ------------------------------------------------------------
-- Table: aasbs.service_charge_schedule
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."service_charge_schedule" (
    "id" bigint NOT NULL,
    "line_item_id" bigint NOT NULL,
    "service_start_dt" timestamp(6) without time zone NOT NULL,
    "service_end_dt" timestamp(6) without time zone NOT NULL,
    "scheduled_amt" numeric(15,2) NOT NULL,
    "description" character varying(2000) NOT NULL,
    "sc_schedule_status_cd" character varying(20) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_sc_schedule" PRIMARY KEY (id),
    CONSTRAINT "ck_sc_sched_se_dt_ge_ss_dt" CHECK (service_end_dt >= service_start_dt),
    CONSTRAINT "fk_sc_sched_li_id" FOREIGN KEY (line_item_id) REFERENCES aasbs.line_item(id),
    CONSTRAINT "fk_sc_schedule_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_sc_schedule_scss" FOREIGN KEY (sc_schedule_status_cd) REFERENCES aasbs.lu_sc_schedule_status(cd),
    CONSTRAINT "fk_sc_schedule_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_sc_schedule_cbui ON aasbs.service_charge_schedule USING btree (created_by_user_id);
CREATE INDEX ix_sc_schedule_li_id ON aasbs.service_charge_schedule USING btree (line_item_id);
CREATE INDEX ix_sc_schedule_scss ON aasbs.service_charge_schedule USING btree (sc_schedule_status_cd);
CREATE INDEX ix_sc_schedule_se_dt ON aasbs.service_charge_schedule USING btree (service_end_dt);
CREATE INDEX ix_sc_schedule_ubui ON aasbs.service_charge_schedule USING btree (updated_by_user_id);

CREATE TRIGGER service_charge_schedule_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.service_charge_schedule FOR EACH ROW EXECUTE FUNCTION aasbs.trg_service_charge_schedule_hst();

COMMENT ON COLUMN "aasbs"."service_charge_schedule"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.SERVICE_CHARGE_SCHEDULE_SEQ';
COMMENT ON COLUMN "aasbs"."service_charge_schedule"."line_item_id" IS 'Foreign Key to table AASBS.LINE_ITEM';
COMMENT ON COLUMN "aasbs"."service_charge_schedule"."service_start_dt" IS 'The date on which this scheduled service starts';
COMMENT ON COLUMN "aasbs"."service_charge_schedule"."service_end_dt" IS 'The date on which this scheduled service ends and will be billed';
COMMENT ON COLUMN "aasbs"."service_charge_schedule"."scheduled_amt" IS 'The amount to be billed for this scheduled service charge';
COMMENT ON COLUMN "aasbs"."service_charge_schedule"."description" IS 'The description of this scheduled service charge';
COMMENT ON COLUMN "aasbs"."service_charge_schedule"."sc_schedule_status_cd" IS 'Foreign Key to table AASBS.SC_SCHEDULE_STATUS';
COMMENT ON COLUMN "aasbs"."service_charge_schedule"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."service_charge_schedule"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."service_charge_schedule"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."service_charge_schedule"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."service_charge_schedule" IS 'This table defines the billing schedule for scheduled (fixed fee) service charges.';

-- ------------------------------------------------------------
-- Table: aasbs.snap_award_mod_amts
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."snap_award_mod_amts" (
    "source_system" character(4),
    "award_mod_id" bigint NOT NULL,
    "award_piid" character varying(22),
    "mod_num" character varying(3),
    "pop_end_dt" timestamp(0) without time zone,
    "cum_funded_amt" numeric,
    "base_all_opt_amt" numeric,
    "base_exc_opt_amt" numeric(19,2),
    "cost_to_gsa_diff_amt" numeric,
    "cost_to_gsa_total_amt" numeric,
    "service_charges_diff_amt" numeric,
    "service_charges_total_amt" numeric,
    "cost_to_client_diff_amt" numeric,
    "cost_to_client_total_amt" numeric,
    CONSTRAINT "snama_pk" PRIMARY KEY (award_mod_id)
);

CREATE INDEX snama_piid_idx ON aasbs.snap_award_mod_amts USING btree (award_piid);

-- ------------------------------------------------------------
-- Table: aasbs.solicit
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."solicit" (
    "id" bigint NOT NULL,
    "acquisition_id" bigint NOT NULL,
    "instrument_type_cd" character varying(3) NOT NULL,
    "lastfinal_solicit_amendment_id" bigint,
    "solicit_piid" character varying(50) NOT NULL,
    "solicit_title" character varying(250) NOT NULL,
    "solicit_description" character varying(2000) NOT NULL,
    "sol_status_cd" character varying(20) NOT NULL,
    "sol_response_cli_visibility_cd" character varying(20) NOT NULL,
    "sol_response_aac_visibility_cd" character varying(20) NOT NULL,
    "at_award_bid_cnt" integer,
    "winner_selection_start_dt" timestamp(6) without time zone,
    "winner_selection_end_dt" timestamp(6) without time zone,
    "draft_frm_selection_yn" character(1) NOT NULL,
    "draft_frm_presol_pkg_yn" character(1) NOT NULL,
    "attach_hash_id" character varying(64),
    "migration_type" character varying(10),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "apex_emp_org_id" bigint,
    CONSTRAINT "pk_solicit" PRIMARY KEY (id),
    CONSTRAINT "ck_draft_frm_presol_pkg_yn" CHECK (draft_frm_presol_pkg_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_draft_frm_selection_yn" CHECK (draft_frm_selection_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "nn_sol_lastfin_solamend_id" CHECK (draft_frm_selection_yn = 'Y'::bpchar OR draft_frm_selection_yn = 'N'::bpchar AND lastfinal_solicit_amendment_id IS NOT NULL),
    CONSTRAINT "nn_solicit_award_bid_cnt" CHECK ((sol_status_cd::text = ANY (ARRAY['SELECTED'::character varying::text, 'COMPLETE'::character varying::text])) AND at_award_bid_cnt IS NOT NULL OR (sol_status_cd::text <> ALL (ARRAY['SELECTED'::character varying::text, 'COMPLETE'::character varying::text]))),
    CONSTRAINT "fk_sol_acq_id" FOREIGN KEY (acquisition_id) REFERENCES aasbs.acquisition(id),
    CONSTRAINT "fk_sol_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_sol_insttyp" FOREIGN KEY (instrument_type_cd) REFERENCES aasbs.lu_instrument_type(cd),
    CONSTRAINT "fk_sol_latest_solamend" FOREIGN KEY (lastfinal_solicit_amendment_id) REFERENCES aasbs.solicit_amendment(id),
    CONSTRAINT "fk_sol_rspns_vis_aac" FOREIGN KEY (sol_response_aac_visibility_cd) REFERENCES aasbs.lu_sol_response_aac_visibility(cd),
    CONSTRAINT "fk_sol_rspns_vis_cli" FOREIGN KEY (sol_response_cli_visibility_cd) REFERENCES aasbs.lu_sol_response_cli_visibility(cd),
    CONSTRAINT "fk_sol_stat" FOREIGN KEY (sol_status_cd) REFERENCES aasbs.lu_sol_status(cd),
    CONSTRAINT "fk_sol_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_solicit_eoi" FOREIGN KEY (apex_emp_org_id) REFERENCES assist.lu_emp_orgs(emp_org_id)
);

CREATE INDEX idx_sol_migration_type ON aasbs.solicit USING btree (migration_type);
CREATE INDEX ix_s_apex_emp_org_id ON aasbs.solicit USING btree (apex_emp_org_id);
CREATE INDEX ix_sol_acq_id ON aasbs.solicit USING btree (acquisition_id);
CREATE INDEX ix_sol_cbui ON aasbs.solicit USING btree (created_by_user_id);
CREATE INDEX ix_sol_insttyp ON aasbs.solicit USING btree (instrument_type_cd);
CREATE INDEX ix_sol_lasffinal_solamd_id ON aasbs.solicit USING btree (lastfinal_solicit_amendment_id);
CREATE INDEX ix_sol_rspns_vis_aac ON aasbs.solicit USING btree (sol_response_aac_visibility_cd);
CREATE INDEX ix_sol_rspns_vis_cli ON aasbs.solicit USING btree (sol_response_cli_visibility_cd);
CREATE INDEX ix_sol_stat ON aasbs.solicit USING btree (sol_status_cd);
CREATE INDEX ix_sol_ubui ON aasbs.solicit USING btree (updated_by_user_id);

CREATE TRIGGER solicit_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.solicit FOR EACH ROW EXECUTE FUNCTION aasbs.trg_solicit_hst();

COMMENT ON COLUMN "aasbs"."solicit"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_SEQ';
COMMENT ON COLUMN "aasbs"."solicit"."acquisition_id" IS 'Foreign Key to table AASBS.ACQUISITION';
COMMENT ON COLUMN "aasbs"."solicit"."instrument_type_cd" IS 'Foreign Key to table AASBS.LU_INSTRUMENT_TYPE';
COMMENT ON COLUMN "aasbs"."solicit"."lastfinal_solicit_amendment_id" IS 'Foreign Key to table AASBS.LATEST_SOLICIT_AMENDMENT';
COMMENT ON COLUMN "aasbs"."solicit"."solicit_piid" IS 'Solicitation procurement instrument identifier';
COMMENT ON COLUMN "aasbs"."solicit"."solicit_title" IS 'Title of the Solicitation';
COMMENT ON COLUMN "aasbs"."solicit"."solicit_description" IS 'Description of Solicitation';
COMMENT ON COLUMN "aasbs"."solicit"."sol_status_cd" IS 'Foreign Key to table AASBS.LU_SOL_STATUS';
COMMENT ON COLUMN "aasbs"."solicit"."sol_response_cli_visibility_cd" IS 'Foreign Key to table AASBS.LU_SOL_RESPONSE_CLI_VISIBILITY';
COMMENT ON COLUMN "aasbs"."solicit"."sol_response_aac_visibility_cd" IS 'Foreign Key to table AASBS.LU_SOL_RESPONSE_AAC_VISIBILITY';
COMMENT ON COLUMN "aasbs"."solicit"."at_award_bid_cnt" IS 'Number of Responses/Bids from Contractors on a Solicitation at the time it is Awarded';
COMMENT ON COLUMN "aasbs"."solicit"."winner_selection_start_dt" IS 'The start date of the period of selecting a Solicitation Winner';
COMMENT ON COLUMN "aasbs"."solicit"."winner_selection_end_dt" IS 'The end date of the period of selecting a Solicitation Winner';
COMMENT ON COLUMN "aasbs"."solicit"."draft_frm_selection_yn" IS 'Flag indicating whether Solicitation Selection Form is (=Y) or is not (=N) in DRAFT state';
COMMENT ON COLUMN "aasbs"."solicit"."draft_frm_presol_pkg_yn" IS 'Flag indicating whether Solicitation Presolicitation Package Form is (=Y) or is not (=N) in DRAFT state';
COMMENT ON COLUMN "aasbs"."solicit"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."solicit"."migration_type" IS 'Internal use only - defines the data structure for the Solicitation';
COMMENT ON COLUMN "aasbs"."solicit"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."solicit"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."solicit"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."solicit"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."solicit"."apex_emp_org_id" IS 'References ASSIST.LU_EMP_ORGS.EMP_ORG_ID';

COMMENT ON TABLE "aasbs"."solicit" IS 'Solicitation Details';

-- ------------------------------------------------------------
-- Table: aasbs.solicit_amendment
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."solicit_amendment" (
    "id" bigint NOT NULL,
    "solicit_id" bigint NOT NULL,
    "amendment_num" character varying(4) NOT NULL,
    "sol_posting_type_cd" character varying(20),
    "posting_round_num" bigint,
    "close_dt" timestamp(6) without time zone,
    "draft_frm_solamend_yn" character(1) NOT NULL,
    "closing_comment" character varying(2000),
    "attach_hash_id" character varying(64),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_solamnd" PRIMARY KEY (id),
    CONSTRAINT "ck_solamend_posting_type_cd_not_null" CHECK (amendment_num::text = 'PSOL'::text AND draft_frm_solamend_yn = 'Y'::bpchar OR sol_posting_type_cd IS NOT NULL),
    CONSTRAINT "ck_solamnd_draft_frm_solamd_yn" CHECK (draft_frm_solamend_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "fk_solamnd_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_solamnd_sol_pst_typ" FOREIGN KEY (sol_posting_type_cd) REFERENCES aasbs.lu_sol_posting_type(cd),
    CONSTRAINT "fk_solamnd_solicit_id" FOREIGN KEY (solicit_id) REFERENCES aasbs.solicit(id),
    CONSTRAINT "fk_solamnd_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_solamend_amndnum ON aasbs.solicit_amendment USING btree (amendment_num);
CREATE INDEX ix_solamend_cbui ON aasbs.solicit_amendment USING btree (created_by_user_id);
CREATE INDEX ix_solamend_solamndposttyp ON aasbs.solicit_amendment USING btree (sol_posting_type_cd);
CREATE INDEX ix_solamend_solid ON aasbs.solicit_amendment USING btree (solicit_id);
CREATE INDEX ix_solamend_ubui ON aasbs.solicit_amendment USING btree (updated_by_user_id);
CREATE UNIQUE INDEX ux_solamend_dupe_amend_num ON aasbs.solicit_amendment USING btree (solicit_id, amendment_num);

CREATE TRIGGER solicit_amendment_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.solicit_amendment FOR EACH ROW EXECUTE FUNCTION aasbs.trg_solicit_amendment_hst();

COMMENT ON COLUMN "aasbs"."solicit_amendment"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_AMENDMENT_SEQ';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."solicit_id" IS 'Foreign Key to table AASBS.SOLICIT';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."amendment_num" IS 'Solicitation Amendment Number';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."sol_posting_type_cd" IS 'Foreign Key to table AASBS.LU_SOL_POSTING_TYPE';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."posting_round_num" IS 'Posting Round Number';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."close_dt" IS 'Close Date of the Solicitation Period';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."draft_frm_solamend_yn" IS 'Flag indicating whether Solictation Amendment Form is (=Y) or is not (=N) in DRAFT state';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."closing_comment" IS 'User comments made during the closing of the Solicitation Amendment';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."solicit_amendment"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."solicit_amendment" IS 'Solicitation Amendment Details - child of SOLICIT table';

-- ------------------------------------------------------------
-- Table: aasbs.solicit_company
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."solicit_company" (
    "id" bigint NOT NULL,
    "solicit_amendment_id" bigint NOT NULL,
    "ipartner_id" character varying(16) NOT NULL,
    "contract_id" character varying(16),
    "distribution_list_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_sol_amend_cmpny" PRIMARY KEY (id),
    CONSTRAINT "fk_sol_amend_cmpny_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_sol_amend_cmpny_distlist_id" FOREIGN KEY (distribution_list_id) REFERENCES table_master.distribution_list(distribution_list_id),
    CONSTRAINT "fk_sol_amend_cmpny_solamend_id" FOREIGN KEY (solicit_amendment_id) REFERENCES aasbs.solicit_amendment(id),
    CONSTRAINT "fk_sol_amend_cmpny_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_sol_amend_cmpny_cbui ON aasbs.solicit_company USING btree (created_by_user_id);
CREATE INDEX ix_sol_amend_cmpny_contractid ON aasbs.solicit_company USING btree (contract_id);
CREATE INDEX ix_sol_amend_cmpny_distlist_id ON aasbs.solicit_company USING btree (distribution_list_id);
CREATE INDEX ix_sol_amend_cmpny_iparterid ON aasbs.solicit_company USING btree (ipartner_id);
CREATE INDEX ix_sol_amend_cmpny_solamend_id ON aasbs.solicit_company USING btree (solicit_amendment_id);
CREATE INDEX ix_sol_amend_cmpny_ubui ON aasbs.solicit_company USING btree (updated_by_user_id);

CREATE TRIGGER solicit_company_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.solicit_company FOR EACH ROW EXECUTE FUNCTION aasbs.trg_solicit_company_hst();

COMMENT ON COLUMN "aasbs"."solicit_company"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_COMPANY_SEQ';
COMMENT ON COLUMN "aasbs"."solicit_company"."solicit_amendment_id" IS 'Foreign Key to table AASBS.SOLICIT_AMENDMENT';
COMMENT ON COLUMN "aasbs"."solicit_company"."ipartner_id" IS 'Foreign Key to table TABLE_MASTER.INDUSTRY_PARTNERS';
COMMENT ON COLUMN "aasbs"."solicit_company"."contract_id" IS 'Foreign Key to table TABLE_MASTER.CONTRACTS';
COMMENT ON COLUMN "aasbs"."solicit_company"."distribution_list_id" IS 'Foreign Key to table TABLE_MASTER.DISTRIBUTION_LIST';
COMMENT ON COLUMN "aasbs"."solicit_company"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."solicit_company"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."solicit_company"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."solicit_company"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."solicit_company" IS 'Company/Contract associated with a Solicitation';

-- ------------------------------------------------------------
-- Table: aasbs.solicit_posting_connect
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."solicit_posting_connect" (
    "id" bigint NOT NULL,
    "solicit_amendment_id" bigint NOT NULL,
    "posting_connect_instructions" character varying(2000) NOT NULL,
    "pm_responsible_user_id" character varying(16) NOT NULL,
    "pm_responsible_email" character varying(100) NOT NULL,
    "pm_responsible_phone" character varying(50) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "open_dt" timestamp(6) without time zone,
    CONSTRAINT "pk_sol_postconnect" PRIMARY KEY (id),
    CONSTRAINT "fk_sol_postconnect_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_sol_postconnect_pm_user_id" FOREIGN KEY (pm_responsible_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_sol_postconnect_sol_amnd_id" FOREIGN KEY (solicit_amendment_id) REFERENCES aasbs.solicit_amendment(id),
    CONSTRAINT "fk_sol_postconnect_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_sol_postconnect_cbui ON aasbs.solicit_posting_connect USING btree (created_by_user_id);
CREATE INDEX ix_sol_postconnect_pm_user_id ON aasbs.solicit_posting_connect USING btree (pm_responsible_user_id);
CREATE INDEX ix_sol_postconnect_sol_amnd_id ON aasbs.solicit_posting_connect USING btree (solicit_amendment_id);
CREATE INDEX ix_sol_postconnect_ubui ON aasbs.solicit_posting_connect USING btree (updated_by_user_id);

CREATE TRIGGER solicit_posting_connect_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.solicit_posting_connect FOR EACH ROW EXECUTE FUNCTION aasbs.trg_solicit_posting_connect_hst();

COMMENT ON COLUMN "aasbs"."solicit_posting_connect"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_POSTING_CONNECT_SEQ';
COMMENT ON COLUMN "aasbs"."solicit_posting_connect"."solicit_amendment_id" IS 'Foreign Key to table AASBS.SOLICIT_AMENDMENT';
COMMENT ON COLUMN "aasbs"."solicit_posting_connect"."posting_connect_instructions" IS 'Posting Connect Instructions';
COMMENT ON COLUMN "aasbs"."solicit_posting_connect"."pm_responsible_user_id" IS 'Foreign Key to table ASSIST.USERS - Responsible Project Manager for this Posting Connect Solicitation';
COMMENT ON COLUMN "aasbs"."solicit_posting_connect"."pm_responsible_email" IS 'Responsible Project Manager email';
COMMENT ON COLUMN "aasbs"."solicit_posting_connect"."pm_responsible_phone" IS 'Responsible Project Manager phone';
COMMENT ON COLUMN "aasbs"."solicit_posting_connect"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."solicit_posting_connect"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."solicit_posting_connect"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."solicit_posting_connect"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."solicit_posting_connect"."open_dt" IS 'Date of opening Posting';

COMMENT ON TABLE "aasbs"."solicit_posting_connect" IS 'Direct Connect Posting Details';

-- ------------------------------------------------------------
-- Table: aasbs.solicit_posting_direct
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."solicit_posting_direct" (
    "id" bigint NOT NULL,
    "solicit_amendment_id" bigint NOT NULL,
    "reason_for_direct_cd" character varying(20) NOT NULL,
    "external_source_ref" character varying(100) NOT NULL,
    "external_source_desc" character varying(2000) NOT NULL,
    "open_dt" timestamp(6) without time zone NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_sol_postdirect" PRIMARY KEY (id),
    CONSTRAINT "fk_sol_postdirect_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_sol_postdirect_rsn_direct" FOREIGN KEY (reason_for_direct_cd) REFERENCES aasbs.lu_reason_for_direct(cd),
    CONSTRAINT "fk_sol_postdirect_sol_amnd_id" FOREIGN KEY (solicit_amendment_id) REFERENCES aasbs.solicit_amendment(id),
    CONSTRAINT "fk_sol_postdirect_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_sol_postdirect_cbui ON aasbs.solicit_posting_direct USING btree (created_by_user_id);
CREATE INDEX ix_sol_postdirect_rsn_direct ON aasbs.solicit_posting_direct USING btree (reason_for_direct_cd);
CREATE INDEX ix_sol_postdirect_sol_amnd_id ON aasbs.solicit_posting_direct USING btree (solicit_amendment_id);
CREATE INDEX ix_sol_postdirect_ubui ON aasbs.solicit_posting_direct USING btree (updated_by_user_id);

CREATE TRIGGER solicit_posting_direct_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.solicit_posting_direct FOR EACH ROW EXECUTE FUNCTION aasbs.trg_solicit_posting_direct_hst();

COMMENT ON COLUMN "aasbs"."solicit_posting_direct"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_POSTING_DIRECT_SEQ';
COMMENT ON COLUMN "aasbs"."solicit_posting_direct"."solicit_amendment_id" IS 'Foreign Key to table AASBS.SOLICIT_AMENDMENT';
COMMENT ON COLUMN "aasbs"."solicit_posting_direct"."reason_for_direct_cd" IS 'Foreign Key to table AASBS.LU_REASON_FOR_DIRECT';
COMMENT ON COLUMN "aasbs"."solicit_posting_direct"."external_source_ref" IS 'User entered external sourcing reference';
COMMENT ON COLUMN "aasbs"."solicit_posting_direct"."external_source_desc" IS 'User entered external source description';
COMMENT ON COLUMN "aasbs"."solicit_posting_direct"."open_dt" IS 'Date of opening Posting';
COMMENT ON COLUMN "aasbs"."solicit_posting_direct"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."solicit_posting_direct"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."solicit_posting_direct"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."solicit_posting_direct"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."solicit_posting_direct" IS 'Direct Connect Posting Details';

-- ------------------------------------------------------------
-- Table: aasbs.solicit_response
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."solicit_response" (
    "id" bigint NOT NULL,
    "solicit_amendment_id" bigint NOT NULL,
    "solicit_id" bigint NOT NULL,
    "solicit_response_num" character varying(4) NOT NULL,
    "good_through_dt" timestamp(6) without time zone,
    "solicit_company_id" bigint NOT NULL,
    "latest_response_update_dt" timestamp(6) without time zone NOT NULL,
    "winning_response_yn" character(1),
    "net_days_cnt" bigint,
    "discount_days_cnt" bigint,
    "discount_pct" numeric(6,3),
    "fob_type_cd" character varying(20),
    "origin_transport_cost_amt" numeric(15,2),
    "is_editable_yn" character(1),
    "response_description" character varying(2000),
    "response_company_name" character varying(120),
    "response_company_duns" character varying(13),
    "response_company_duns_plus4" character varying(4),
    "response_poc_user_id" character varying(16) NOT NULL,
    "response_poc_email" character varying(100),
    "response_poc_phone" character varying(30),
    "response_poc_fax" character varying(30),
    "response_company_address_id" bigint,
    "attach_hash_id" character varying(64),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "no_response_reason" character varying(2000),
    "no_response_yn" character(1) DEFAULT 'N'::character(1) NOT NULL,
    "response_company_uei" character varying(16),
    "response_company_uei_plus4" character varying(8),
    "response_company_vuei" character varying(16) GENERATED ALWAYS AS (
CASE
    WHEN (response_company_uei IS NULL) THEN response_company_duns
    ELSE response_company_uei
END) STORED,
    "response_company_vuei_plus4" character varying(13) GENERATED ALWAYS AS (
CASE
    WHEN (response_company_uei_plus4 IS NULL) THEN response_company_duns_plus4
    ELSE response_company_uei_plus4
END) STORED,
    CONSTRAINT "pk_solrespns" PRIMARY KEY (id),
    CONSTRAINT "ck_solresp_editable_yn" CHECK (is_editable_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ck_solrespns_winning_yn" CHECK (winning_response_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "nn_solresp_editable_yn" CHECK (no_response_yn = 'Y'::bpchar OR is_editable_yn IS NOT NULL),
    CONSTRAINT "nn_solrespns_fobytpe" CHECK (no_response_yn = 'Y'::bpchar OR fob_type_cd IS NOT NULL),
    CONSTRAINT "nn_solrespns_good_dt" CHECK (no_response_yn = 'Y'::bpchar OR good_through_dt IS NOT NULL),
    CONSTRAINT "nn_solrespns_no_resp_reason" CHECK (no_response_yn = 'N'::bpchar OR no_response_reason IS NOT NULL),
    CONSTRAINT "nn_solrespns_orgn_trns_cst_amt" CHECK (no_response_yn = 'Y'::bpchar OR origin_transport_cost_amt IS NOT NULL),
    CONSTRAINT "fk_solrespns_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_solrespns_fobytpe" FOREIGN KEY (fob_type_cd) REFERENCES aasbs.lu_fob_type(cd),
    CONSTRAINT "fk_solrespns_poc_userid" FOREIGN KEY (response_poc_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_solrespns_respcomp_addr_id" FOREIGN KEY (response_company_address_id) REFERENCES aasbs.address(id) NOT VALID,
    CONSTRAINT "fk_solrespns_sol_company_id" FOREIGN KEY (solicit_company_id) REFERENCES aasbs.solicit_company(id),
    CONSTRAINT "fk_solrespns_sol_id" FOREIGN KEY (solicit_id) REFERENCES aasbs.solicit(id),
    CONSTRAINT "fk_solrespns_solamnd_id" FOREIGN KEY (solicit_amendment_id) REFERENCES aasbs.solicit_amendment(id),
    CONSTRAINT "fk_solrespns_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_solrespns_cbui ON aasbs.solicit_response USING btree (created_by_user_id);
CREATE INDEX ix_solrespns_fobtype ON aasbs.solicit_response USING btree (fob_type_cd);
CREATE INDEX ix_solrespns_poc_userid ON aasbs.solicit_response USING btree (response_poc_user_id);
CREATE INDEX ix_solrespns_respcomp_addr_id ON aasbs.solicit_response USING btree (response_company_address_id);
CREATE INDEX ix_solrespns_sol_company_id ON aasbs.solicit_response USING btree (solicit_company_id);
CREATE INDEX ix_solrespns_sol_id ON aasbs.solicit_response USING btree (solicit_id);
CREATE INDEX ix_solrespns_solamnd_id ON aasbs.solicit_response USING btree (solicit_amendment_id);
CREATE INDEX ix_solrespns_ubui ON aasbs.solicit_response USING btree (updated_by_user_id);
CREATE UNIQUE INDEX ux_solrespns_sol_id_resp_num ON aasbs.solicit_response USING btree (solicit_id, solicit_response_num);

CREATE TRIGGER solicit_response_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.solicit_response FOR EACH ROW EXECUTE FUNCTION aasbs.trg_solicit_response_hst();

COMMENT ON COLUMN "aasbs"."solicit_response"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_RESPONSE_SEQ';
COMMENT ON COLUMN "aasbs"."solicit_response"."solicit_amendment_id" IS 'Foreign Key to table AASBS.SOLICIT_AMENDMENT';
COMMENT ON COLUMN "aasbs"."solicit_response"."solicit_id" IS 'Foreign Key to table AASBS.SOLICIT';
COMMENT ON COLUMN "aasbs"."solicit_response"."solicit_response_num" IS 'System generated Solicitation Response Number - unique within a single Solicitation';
COMMENT ON COLUMN "aasbs"."solicit_response"."good_through_dt" IS 'Contractor-entered expiration of their Solicitation Response';
COMMENT ON COLUMN "aasbs"."solicit_response"."solicit_company_id" IS 'Foreign Key to table AASBS.SOLICIT_COMPANY';
COMMENT ON COLUMN "aasbs"."solicit_response"."latest_response_update_dt" IS 'Date of latest update to the submitted Solicitation Response';
COMMENT ON COLUMN "aasbs"."solicit_response"."winning_response_yn" IS 'Flag indicating whether the Solicitation Response has been chosen as the Winning Bid (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."solicit_response"."net_days_cnt" IS 'Number of days set by Contractor defining the discount period';
COMMENT ON COLUMN "aasbs"."solicit_response"."discount_days_cnt" IS 'Contractor-specified number of days to achieve the discount percentage (DISCOUNT_PCT column)';
COMMENT ON COLUMN "aasbs"."solicit_response"."discount_pct" IS 'Contractor-specified discount percentage if paid within specified number of days (DISCOUNT_DAYS_CNT column)';
COMMENT ON COLUMN "aasbs"."solicit_response"."fob_type_cd" IS 'Foreign Key to table AASBS.LU_FOB_TYPE';
COMMENT ON COLUMN "aasbs"."solicit_response"."origin_transport_cost_amt" IS 'Cost of Transportation in the case that transportation is necessary from Origin';
COMMENT ON COLUMN "aasbs"."solicit_response"."is_editable_yn" IS 'Flag indicating if the Solicitation Response is still Editable (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs"."solicit_response"."response_description" IS 'Contractor-entered Solicitation Reponse Description';
COMMENT ON COLUMN "aasbs"."solicit_response"."response_company_name" IS 'Name of the company that responded to the solicitation';
COMMENT ON COLUMN "aasbs"."solicit_response"."response_company_duns" IS 'The Responding Company''s DUNS Number';
COMMENT ON COLUMN "aasbs"."solicit_response"."response_company_duns_plus4" IS 'DUNS number assigned to a Client';
COMMENT ON COLUMN "aasbs"."solicit_response"."response_poc_user_id" IS 'Foreign Key to table ASSIST.USERS';
COMMENT ON COLUMN "aasbs"."solicit_response"."response_poc_email" IS 'Email of the Point of Contact (POC) of the company that responded to the solicitation';
COMMENT ON COLUMN "aasbs"."solicit_response"."response_poc_phone" IS 'Phone Number of the Point of Contact (POC) of the company that responded to the solicitation';
COMMENT ON COLUMN "aasbs"."solicit_response"."response_poc_fax" IS 'Fax Number of the Point of Contact (POC) of the company that responded to the solicitation';
COMMENT ON COLUMN "aasbs"."solicit_response"."response_company_address_id" IS 'Foreign Key to table AASBS.ADDRESS';
COMMENT ON COLUMN "aasbs"."solicit_response"."attach_hash_id" IS 'This is an unenforceable foreign key to TABLE_MASTER.ATTACHMENTS (parent_id).  There are duplicates in parent_id thus unenforceable.';
COMMENT ON COLUMN "aasbs"."solicit_response"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."solicit_response"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."solicit_response"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."solicit_response"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."solicit_response"."response_company_uei" IS 'Unique Entity Identifier (UEI) - replaces DUNS';
COMMENT ON COLUMN "aasbs"."solicit_response"."response_company_uei_plus4" IS 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4';

COMMENT ON TABLE "aasbs"."solicit_response" IS 'Solicitation Response from Contractor';

-- ------------------------------------------------------------
-- Table: aasbs.solicit_responsible
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."solicit_responsible" (
    "id" bigint NOT NULL,
    "solicit_id" bigint NOT NULL,
    "user_id" character varying(16) NOT NULL,
    "responsible_role_cd" character varying(20) NOT NULL,
    "is_primary_yn" character(1) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_sol_respnsbl" PRIMARY KEY (id),
    CONSTRAINT "fk_sol_respnsbl_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_sol_respnsbl_resprole" FOREIGN KEY (responsible_role_cd) REFERENCES aasbs.lu_responsible_role(cd),
    CONSTRAINT "fk_sol_respnsbl_solamnd" FOREIGN KEY (solicit_id) REFERENCES aasbs.solicit(id),
    CONSTRAINT "fk_sol_respnsbl_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_sol_respnsbl_userid" FOREIGN KEY (user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_sol_respnsbl_cbui ON aasbs.solicit_responsible USING btree (created_by_user_id);
CREATE INDEX ix_sol_respnsbl_resproleid ON aasbs.solicit_responsible USING btree (responsible_role_cd);
CREATE INDEX ix_sol_respnsbl_solamnd ON aasbs.solicit_responsible USING btree (solicit_id);
CREATE INDEX ix_sol_respnsbl_ubui ON aasbs.solicit_responsible USING btree (updated_by_user_id);
CREATE INDEX ix_sol_respnsbl_userid ON aasbs.solicit_responsible USING btree (user_id);

CREATE TRIGGER solicit_responsible_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.solicit_responsible FOR EACH ROW EXECUTE FUNCTION aasbs.trg_solicit_responsible_hst();

COMMENT ON COLUMN "aasbs"."solicit_responsible"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.SOLICIT_RESPONSIBLE_SEQ';
COMMENT ON COLUMN "aasbs"."solicit_responsible"."solicit_id" IS 'Foreign Key to table AASBS.SOLICIT';
COMMENT ON COLUMN "aasbs"."solicit_responsible"."user_id" IS 'Foreign Key to table AASBS.USER';
COMMENT ON COLUMN "aasbs"."solicit_responsible"."responsible_role_cd" IS 'Foreign Key to table AASBS.LU_RESPONSIBLE_ROLE';
COMMENT ON COLUMN "aasbs"."solicit_responsible"."is_primary_yn" IS 'Flag indicating whether a person associated with an Solicitation is the primary for their role (=Y)  or not (=N)';
COMMENT ON COLUMN "aasbs"."solicit_responsible"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."solicit_responsible"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."solicit_responsible"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."solicit_responsible"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs"."solicit_responsible" IS 'Personnel associated with a Solicitation';

-- ------------------------------------------------------------
-- Table: aasbs.tracking_item
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."tracking_item" (
    "id" bigint NOT NULL,
    "li_tracking_num" bigint NOT NULL,
    "line_item_type_cd" character varying(30) NOT NULL,
    "tracking_item_amt" numeric(15,2) NOT NULL,
    "ia_id" bigint NOT NULL,
    "src_tkeeping_monthly_hours_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "src_travel_voucher_id" bigint,
    "src_financial_services_id" bigint,
    CONSTRAINT "pk_trking_item" PRIMARY KEY (id),
    CONSTRAINT "fk_trck_item_src_trav_vch_id" FOREIGN KEY (src_travel_voucher_id) REFERENCES aasbs.travel_voucher(id),
    CONSTRAINT "fk_trking_item_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_trking_item_ia_id" FOREIGN KEY (ia_id) REFERENCES aasbs.ia(id),
    CONSTRAINT "fk_trking_item_li_type" FOREIGN KEY (line_item_type_cd) REFERENCES aasbs.lu_line_item_type(cd),
    CONSTRAINT "fk_trking_item_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_tracking_item_ia_id ON aasbs.tracking_item USING btree (ia_id);
CREATE INDEX ix_trck_item_src_trav_vch_id ON aasbs.tracking_item USING btree (src_travel_voucher_id);
CREATE INDEX ix_trking_item_cbui ON aasbs.tracking_item USING btree (created_by_user_id);
CREATE INDEX ix_trking_item_li_trk_num ON aasbs.tracking_item USING btree (li_tracking_num);
CREATE INDEX ix_trking_item_li_type ON aasbs.tracking_item USING btree (line_item_type_cd);
CREATE INDEX ix_trking_item_src_tkeep_mon_hrs ON aasbs.tracking_item USING btree (src_tkeeping_monthly_hours_id);
CREATE INDEX ix_trking_item_trk_itm_amt ON aasbs.tracking_item USING btree (tracking_item_amt);
CREATE INDEX ix_trking_item_ubui ON aasbs.tracking_item USING btree (updated_by_user_id);

CREATE TRIGGER tracking_item_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.tracking_item FOR EACH ROW EXECUTE FUNCTION aasbs.trg_tracking_item_hst();

COMMENT ON COLUMN "aasbs"."tracking_item"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.TRACKING_ITEM_SEQ';
COMMENT ON COLUMN "aasbs"."tracking_item"."li_tracking_num" IS 'Identifier of a real-world "Line Item".  It is the ID of the first record (/ version) in LINE_ITEM table representing the real-world "Line Item"';
COMMENT ON COLUMN "aasbs"."tracking_item"."line_item_type_cd" IS 'Foreign Key to table AASBS.LU_LINE_ITEM_TYPE - can only be LI_IA_SC_TRAVEL_TRACKING, LI_IA_SC_LABOR_TRACKING';
COMMENT ON COLUMN "aasbs"."tracking_item"."tracking_item_amt" IS 'Amount being Tracked';
COMMENT ON COLUMN "aasbs"."tracking_item"."ia_id" IS 'Foreign Key to table AASBS.IA';
COMMENT ON COLUMN "aasbs"."tracking_item"."src_tkeeping_monthly_hours_id" IS 'Foreign Key to table AASBS_TIMEKEEPING.MONTHLY_HOURS';
COMMENT ON COLUMN "aasbs"."tracking_item"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs"."tracking_item"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs"."tracking_item"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs"."tracking_item"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs"."tracking_item"."src_financial_services_id" IS 'Foreign Key to table AASBS.FINANCIAL_SERVICES';

COMMENT ON TABLE "aasbs"."tracking_item" IS 'Contains dollar amount tracking for Tracking Line Items';

-- ------------------------------------------------------------
-- Table: aasbs.travel_voucher
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."travel_voucher" (
    "id" bigint NOT NULL,
    "line_item_id" bigint NOT NULL,
    "travel_start_dt" timestamp(6) without time zone NOT NULL,
    "travel_end_dt" timestamp(6) without time zone NOT NULL,
    "destination" character varying(300) NOT NULL,
    "travel_auth_num" character varying(20) NOT NULL,
    "traveling_user_id" character varying(16) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "voucher_amt" numeric(15,2),
    CONSTRAINT "pk_trav_voucher" PRIMARY KEY (id),
    CONSTRAINT "nn_trav_voucher_amt" CHECK (voucher_amt IS NOT NULL),
    CONSTRAINT "fk_trav_vochr_trav_user" FOREIGN KEY (traveling_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_trav_voucher_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_trav_voucher_ia_id" FOREIGN KEY (line_item_id) REFERENCES aasbs.line_item(id),
    CONSTRAINT "fk_trav_voucher_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_trav_vochr_trav_user ON aasbs.travel_voucher USING btree (traveling_user_id);
CREATE INDEX ix_trav_voucher_cbui ON aasbs.travel_voucher USING btree (created_by_user_id);
CREATE INDEX ix_trav_voucher_ia_id ON aasbs.travel_voucher USING btree (line_item_id);
CREATE INDEX ix_trav_voucher_trav_auth_num ON aasbs.travel_voucher USING btree (travel_auth_num);
CREATE INDEX ix_trav_voucher_ubui ON aasbs.travel_voucher USING btree (updated_by_user_id);

CREATE TRIGGER travel_voucher_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs.travel_voucher FOR EACH ROW EXECUTE FUNCTION aasbs.trg_travel_voucher_hst();

COMMENT ON COLUMN "aasbs"."travel_voucher"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS.TRAVEL_VOUCHER_SEQ';

COMMENT ON TABLE "aasbs"."travel_voucher" IS 'Contains the details of Travel Vouchers submitted for billing';

-- ------------------------------------------------------------
-- Table: aasbs.vw_procurement_order
-- ------------------------------------------------------------

CREATE TABLE "aasbs"."vw_procurement_order" (
    "procurement_id" double precision,
    "realm" character varying(256),
    "front_end_order_id" character varying(32),
    "order_id" character varying(40),
    "piid" character varying(50)
);

CREATE INDEX idx_vw_proc_order_fe_ord_id ON aasbs.vw_procurement_order USING btree (front_end_order_id);
CREATE INDEX idx_vw_proc_order_ord_id ON aasbs.vw_procurement_order USING btree (order_id);
CREATE INDEX idx_vw_proc_order_piid ON aasbs.vw_procurement_order USING btree (piid);
CREATE INDEX idx_vw_proc_order_proc_id ON aasbs.vw_procurement_order USING btree (procurement_id);
CREATE INDEX idx_vw_proc_order_realm ON aasbs.vw_procurement_order USING btree (realm);

-- ------------------------------------------------------------
-- Table: assist.award
-- ------------------------------------------------------------

CREATE TABLE "assist"."award" (
    "award_id" bigint NOT NULL,
    "procurement_id" bigint NOT NULL,
    "form_type" character varying(10),
    "form_revision_num" character varying(22),
    "status" character varying(50),
    "order_mod_id" character varying(32),
    "mod_num" character varying(3),
    "type_of_order" character varying(1),
    "certification_of_funds_id" numeric(22,0),
    "contract_num" character varying(30),
    "contract_id" character varying(16),
    "order_num" character varying(22),
    "order_date" timestamp(0) without time zone,
    "payment_term" character varying(20),
    "po_discount_terms" character varying(70),
    "net_days" numeric(19,2),
    "discount_percent" numeric(6,3),
    "discount_days" smallint,
    "payment_inquiry_name" character varying(35),
    "payment_inquiry_phone" character varying(24),
    "certified_date" timestamp(0) without time zone,
    "certified_correct_by" character varying(75),
    "contract_officer_name" character varying(35),
    "contract_officer_phone" character varying(24),
    "contract_officer_phone_ext" character varying(5),
    "contracting_office_id" character varying(10),
    "signed_by_id" character varying(16),
    "signature_name" character varying(35),
    "signature_date" timestamp(0) without time zone,
    "lookup_emp_name" character varying(75),
    "lading_bill_num" character varying(20),
    "fob_point_name" character varying(35),
    "fob_date" date,
    "small_business_class" character varying(2),
    "business_type" character varying(1),
    "duns_num" character varying(13),
    "ipartner_ftin" character varying(50),
    "project_number" character varying(40),
    "amount" text,
    "item_num" text,
    "supplies_or_services" text,
    "unit_price" text,
    "units" text,
    "quantity_ordered" text,
    "description_of_amendment" text,
    "schedule_appropriation_text" text,
    "ip_sign_by_id" character varying(16),
    "ip_signature_date" date,
    "organization_code" character varying(10),
    "quantity_status" character varying(50),
    "quote_or_proposal_date" date,
    "ccr_exempt_flag" character varying(1),
    "ipartner_name" character varying(120),
    "lookup_ip_poc_name" character varying(75),
    "ip_address_id" bigint,
    "different_remit_addr_flag" character varying(1),
    "place_of_accept_name" character varying(100),
    "place_of_accept_address_id" bigint,
    "invoice_office" character varying(75),
    "invoice_address_id" bigint,
    "invoice_instructions" character varying(2000),
    "different_invoice_addr_flag" character varying(1),
    "remit_to_company" character varying(175),
    "remit_address_id" bigint,
    "issuing_office" character varying(100),
    "issuing_address_id" bigint,
    "requisition_office" character varying(35),
    "requisition_name" character varying(75),
    "requisition_address_id" bigint,
    "ship_address_id" bigint,
    "created_by" character varying(30),
    "creation_date" timestamp without time zone,
    "submitted_by" character varying(75),
    "submitted_date" timestamp without time zone,
    "last_updated_by" character varying(30),
    "last_update_date" timestamp without time zone,
    "delete_flag" character varying(1),
    "ipartner_sign_reqd_flag" character varying(1),
    "ip_signed_flag" character varying(1),
    "num_copies_ip_return" numeric(19,2),
    "contractor_agrees_flag" character varying(1),
    "ship_num" character varying(20),
    "payment_status" character varying(8),
    "admin_by_name" character varying(35),
    "admin_by_office" character varying(75),
    "admin_by_address_id" bigint,
    "ecf_approval_flag" character varying(1),
    "agreement_number" character varying(30),
    "status_change_date" date,
    "act_num" character varying(9),
    "contractor_comments" text,
    "shipping_location" character varying(200),
    "shipping_office" character varying(200),
    "ipartner_id" character varying(16),
    "small_business_pre_assist" character varying(128),
    "uei" character varying(16),
    "vuei" character varying(16) GENERATED ALWAYS AS (
CASE
    WHEN (uei IS NULL) THEN duns_num
    ELSE uei
END) STORED,
    CONSTRAINT "award_pk" PRIMARY KEY (award_id),
    CONSTRAINT "award_ccr_exempt_flag_ck" CHECK (ccr_exempt_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "award_cont_agrees_flg_ck" CHECK (contractor_agrees_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "award_del_ck" CHECK (delete_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "award_diff_inv_ck" CHECK (different_invoice_addr_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "award_diff_remit_ck" CHECK (different_remit_addr_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "award_fob_pnt_name_ck" CHECK (fob_point_name::text = ANY (ARRAY['Destination'::character varying::text, 'Point of Origin'::character varying::text, NULL::text::character varying::text])),
    CONSTRAINT "award_ip_sign_ck" CHECK (ip_signed_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "award_ip_sign_req_ck" CHECK (ipartner_sign_reqd_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "award_paystatus_ck" CHECK (payment_status::text = ANY (ARRAY['COMPLETE'::character varying::text, 'PARTIAL'::character varying::text, 'FINAL'::character varying::text])),
    CONSTRAINT "award_quantity_status_ck" CHECK (quantity_status::text = ANY (ARRAY['Inspected'::character varying::text, 'Received'::character varying::text, 'Accepted and conforms to the contract as noted'::character varying::text])),
    CONSTRAINT "award_status_ck" CHECK (status::text = ANY (ARRAY['Draft'::character varying::text, 'Signed'::character varying::text, 'Returned for Revision'::character varying::text, 'Pending CO Signature'::character varying::text, 'Pending Contractor Signature'::character varying::text])),
    CONSTRAINT "form_type_ck" CHECK (form_type::text = ANY (ARRAY['Form30'::character varying::text, 'Form300'::character varying::text, 'Form1449'::character varying::text, 'Form1155'::character varying::text, 'Form26'::character varying::text])),
    CONSTRAINT "award_adminby_address_fk" FOREIGN KEY (admin_by_address_id) REFERENCES assist.address(address_id),
    CONSTRAINT "award_invoice_address_fk" FOREIGN KEY (invoice_address_id) REFERENCES assist.address(address_id),
    CONSTRAINT "award_ip_address_fk" FOREIGN KEY (ip_address_id) REFERENCES assist.address(address_id),
    CONSTRAINT "award_issuing_address_fk" FOREIGN KEY (issuing_address_id) REFERENCES assist.address(address_id),
    CONSTRAINT "award_plofacc_address_fk" FOREIGN KEY (place_of_accept_address_id) REFERENCES assist.address(address_id),
    CONSTRAINT "award_procurement_fk" FOREIGN KEY (procurement_id) REFERENCES assist.procurement(procurement_id),
    CONSTRAINT "award_remit_address_fk" FOREIGN KEY (remit_address_id) REFERENCES assist.address(address_id),
    CONSTRAINT "award_req_address_fk" FOREIGN KEY (requisition_address_id) REFERENCES assist.address(address_id),
    CONSTRAINT "award_ship_address_fk" FOREIGN KEY (ship_address_id) REFERENCES assist.address(address_id)
);

CREATE INDEX award_bus_type_idx ON assist.award USING btree (business_type);
CREATE INDEX award_contract_id_idx ON assist.award USING btree (contract_id);
CREATE INDEX award_delete_flag_idx ON assist.award USING btree (delete_flag);
CREATE INDEX award_ip_address_id_idx ON assist.award USING btree (ip_address_id);
CREATE INDEX award_issuing_address_id_idx ON assist.award USING btree (issuing_address_id);
CREATE INDEX award_last_update_date_idx ON assist.award USING btree (last_update_date);
CREATE INDEX award_order_mod_id_idx ON assist.award USING btree (order_mod_id);
CREATE INDEX award_org_idx ON assist.award USING btree (organization_code);
CREATE INDEX award_poa_address_id_idx ON assist.award USING btree (place_of_accept_address_id);
CREATE INDEX award_procurement_id_idx ON assist.award USING btree (procurement_id);
CREATE INDEX award_remit_address_id_idx ON assist.award USING btree (remit_address_id);
CREATE INDEX award_req_address_id_idx ON assist.award USING btree (requisition_address_id);
CREATE INDEX award_sbc_idx ON assist.award USING btree (small_business_class);
CREATE INDEX award_sig_date_fidx ON assist.award USING btree (date_trunc('month'::text, signature_date));
CREATE INDEX award_signed_by_id_idx ON assist.award USING btree (signed_by_id);
CREATE INDEX award_status_fidx ON assist.award USING btree (upper((status)::text));
CREATE INDEX ix_a_admin_by_address_id ON assist.award USING btree (admin_by_address_id);
CREATE INDEX ix_a_invoice_address_id ON assist.award USING btree (invoice_address_id);
CREATE INDEX ix_a_ship_address_id ON assist.award USING btree (ship_address_id);

COMMENT ON COLUMN "assist"."award"."uei" IS 'Unique Entity Identifier (UEI) - replaces DUNS';

COMMENT ON TABLE "assist"."award" IS 'Top level table for Award Documents';

-- ------------------------------------------------------------
-- Table: assist.lu_emp_org_xref
-- ------------------------------------------------------------

CREATE TABLE "assist"."lu_emp_org_xref" (
    "emp_org_id" integer NOT NULL,
    "emp_org_child_id" integer NOT NULL,
    CONSTRAINT "lu_emp_org_xref_pk" PRIMARY KEY (emp_org_id, emp_org_child_id),
    CONSTRAINT "lu_eo_xref_chk" CHECK (emp_org_child_id <> emp_org_id),
    CONSTRAINT "lu_eo_xref_child_fk" FOREIGN KEY (emp_org_child_id) REFERENCES assist.lu_emp_orgs(emp_org_id),
    CONSTRAINT "lu_eo_xref_parent_fk" FOREIGN KEY (emp_org_id) REFERENCES assist.lu_emp_orgs(emp_org_id)
);

CREATE UNIQUE INDEX lu_eo_xref_child_uk ON assist.lu_emp_org_xref USING btree (emp_org_child_id);

CREATE TRIGGER "lu_emp_org_xref$audtrg" AFTER INSERT OR DELETE OR UPDATE ON assist.lu_emp_org_xref FOR EACH ROW EXECUTE FUNCTION assist."lu_emp_org_xref$audtrg$lu_emp_org_xref"();

COMMENT ON TABLE "assist"."lu_emp_org_xref" IS 'The assist.lu_emp_orgs parent-child relationships';

-- ------------------------------------------------------------
-- Table: assist.lu_emp_orgs
-- ------------------------------------------------------------

CREATE TABLE "assist"."lu_emp_orgs" (
    "emp_org_id" integer NOT NULL,
    "emp_org_name" character varying(128) NOT NULL,
    "emp_org_short_name" character varying(128),
    "tier_id" integer NOT NULL,
    "gsa_org_id" integer,
    "nba_org_id" integer,
    "nba_group_id" integer,
    "emp_org_version_cd" character varying(50) DEFAULT 'EMP_ORG_VERSION_PRE_2023'::character varying,
    CONSTRAINT "lu_emp_orgs_pk" PRIMARY KEY (emp_org_id),
    CONSTRAINT "nn_lu_emp_orgs_eovcd" CHECK (emp_org_version_cd IS NOT NULL),
    CONSTRAINT "fk_lu_emp_orgs_eovcd" FOREIGN KEY (emp_org_version_cd) REFERENCES assist.lu_emp_org_version(cd),
    CONSTRAINT "lu_eo_gsaorg_fk" FOREIGN KEY (gsa_org_id) REFERENCES table_master.gsa_organizations(organization_id),
    CONSTRAINT "lu_eo_nbagrp_fk" FOREIGN KEY (nba_group_id) REFERENCES assist.lu_nba_groups(nba_group_id),
    CONSTRAINT "lu_eo_nbaorg_fk" FOREIGN KEY (nba_org_id) REFERENCES assist.lu_nba_orgs(nba_org_id),
    CONSTRAINT "lu_eo_tier_version_fk" FOREIGN KEY (tier_id, emp_org_version_cd) REFERENCES assist.lu_emp_org_tiers(tier_id, emp_org_version_cd)
);

CREATE INDEX ix_leo_emp_org_version_cd ON assist.lu_emp_orgs USING btree (emp_org_version_cd);
CREATE INDEX ix_leo_gsa_org_id ON assist.lu_emp_orgs USING btree (gsa_org_id);
CREATE INDEX ix_leo_nba_group_id ON assist.lu_emp_orgs USING btree (nba_group_id);
CREATE INDEX ix_leo_nba_org_id ON assist.lu_emp_orgs USING btree (nba_org_id);
CREATE INDEX ix_leo_tier_id ON assist.lu_emp_orgs USING btree (tier_id);

CREATE TRIGGER "lu_emp_orgs$audtrg" AFTER INSERT OR DELETE OR UPDATE ON assist.lu_emp_orgs FOR EACH ROW EXECUTE FUNCTION assist."lu_emp_orgs$audtrg$lu_emp_orgs"();
CREATE TRIGGER lu_emp_orgs_a_i_u_d_row AFTER INSERT OR DELETE OR UPDATE ON assist.lu_emp_orgs FOR EACH ROW EXECUTE FUNCTION assist."lu_emp_orgs_a_i_u_d_row$audtrg"();

COMMENT ON COLUMN "assist"."lu_emp_orgs"."emp_org_version_cd" IS 'GSA Employee Hierarchy Version';

COMMENT ON TABLE "assist"."lu_emp_orgs" IS 'EMPloyee user ORGanizational heirarchy, each record is one node in the tree';

-- ------------------------------------------------------------
-- Table: assist.procurement
-- ------------------------------------------------------------

CREATE TABLE "assist"."procurement" (
    "procurement_id" bigint NOT NULL,
    "system" character varying(256) NOT NULL,
    "created_by" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "creation_date" timestamp(0) without time zone DEFAULT now() NOT NULL,
    "system_id" character varying(30) NOT NULL,
    CONSTRAINT "sys_c0061788" PRIMARY KEY (procurement_id)
);

CREATE UNIQUE INDEX proc_sys_sys_id_idx ON assist.procurement USING btree (system, system_id);
CREATE INDEX procurement_system_idx ON assist.procurement USING btree (system);

CREATE TRIGGER "procurement$audtrg" AFTER INSERT OR DELETE OR UPDATE ON assist.procurement FOR EACH ROW EXECUTE FUNCTION assist."procurement$audtrg$procurement"();

-- ------------------------------------------------------------
-- Table: assist.users
-- ------------------------------------------------------------

CREATE TABLE "assist"."users" (
    "id" character varying(16) NOT NULL,
    "reg_status" character varying(22) NOT NULL,
    "username" character varying(75) NOT NULL,
    "created_by" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "creation_date" timestamp(0) without time zone DEFAULT now() NOT NULL,
    "last_updated_by" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "last_update_date" timestamp(0) without time zone DEFAULT now() NOT NULL,
    "delete_flag" character varying(1) DEFAULT 'N'::character varying NOT NULL,
    "first_name" character varying(34) NOT NULL,
    "middle_initial" character varying(2),
    "last_name" character varying(34) NOT NULL,
    "address_id" bigint,
    "phone" character varying(24),
    "phone_ext" character varying(5),
    "fax" character varying(24),
    "email" character varying(80) NOT NULL,
    "last_login" timestamp(0) without time zone DEFAULT now() NOT NULL,
    "last_failed_login" timestamp(0) without time zone,
    "success_ip_address" character varying(15),
    "fail_ip_address" character varying(15),
    "authentication_type" character varying(50),
    "inactive_date" timestamp(0) without time zone,
    "remarks" character varying(2000),
    "password_reset_token" character varying(40),
    "password_reset_expiration_date" timestamp(0) without time zone,
    "user_type" character varying(20) NOT NULL,
    "password" character varying(100),
    "login_attempts" integer NOT NULL,
    "last_failed_login_timestamp" timestamp(6) without time zone,
    "last_reg_status_updated_date" timestamp(0) without time zone,
    "default_module" character varying(22) DEFAULT 'TOS'::character varying,
    "password_change_flag" character varying(1) DEFAULT 'N'::character varying NOT NULL,
    "password_change_date" timestamp(0) without time zone,
    "activation_date" timestamp(0) without time zone,
    "special_instructions" character varying(2000),
    "password_expirable" character varying(1) DEFAULT 'Y'::character varying NOT NULL,
    "departure_date" date,
    "otp_secret" character varying(100),
    "otp_last_login_time" character varying(200),
    "otp_successful_login_yn" character varying(1),
    "allow_mfa_by_email_yn" character varying(1) DEFAULT 'N'::character varying NOT NULL,
    "login_email" character varying(80),
    "account_to_retain_yn" character varying(1),
    "requires_fas_email_yn" character varying(1),
    "okta_user_id" character varying(32),
    CONSTRAINT "users_pk" PRIMARY KEY (id),
    CONSTRAINT "uk_login_email_id" UNIQUE (login_email),
    CONSTRAINT "ck_account_to_retain_yn" CHECK (account_to_retain_yn::text = ANY (ARRAY['Y'::character varying, 'N'::character varying]::text[])),
    CONSTRAINT "ck_requires_fas_email_yn" CHECK (requires_fas_email_yn::text = ANY (ARRAY['Y'::character varying, 'N'::character varying]::text[])),
    CONSTRAINT "ck_users_allow_mfa_email_yn" CHECK (allow_mfa_by_email_yn::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "ck_users_otp_login_yn" CHECK (otp_successful_login_yn::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "u_df" CHECK (delete_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "users_addr_fk" FOREIGN KEY (address_id) REFERENCES assist.addresses(address_id),
    CONSTRAINT "users_lurs_fk" FOREIGN KEY (reg_status) REFERENCES table_master.lu_reg_status(reg_status)
);

CREATE INDEX ix_u_address_id ON assist.users USING btree (address_id);
CREATE INDEX users_email_indx ON assist.users USING btree (email);
CREATE INDEX users_first_name_idx ON assist.users USING btree (first_name);
CREATE INDEX users_index1 ON assist.users USING btree (reg_status);
CREATE UNIQUE INDEX users_index2 ON assist.users USING btree (upper((username)::text));
CREATE INDEX users_index5 ON assist.users USING btree (upper((email)::text));
CREATE INDEX users_last_name_idx ON assist.users USING btree (last_name);
CREATE INDEX users_norm_name_idx ON assist.users USING btree (upper(assist.normalize_username((username)::text)));
CREATE INDEX users_username_indx ON assist.users USING btree (username);
CREATE INDEX users_usertype_idx ON assist.users USING btree (user_type);

CREATE TRIGGER "users$audtrg" AFTER INSERT OR DELETE OR UPDATE ON assist.users FOR EACH ROW EXECUTE FUNCTION assist."users$audtrg$users"();
CREATE TRIGGER users_last_update BEFORE INSERT OR UPDATE ON assist.users FOR EACH ROW EXECUTE FUNCTION assist."users_last_update$users"();

COMMENT ON COLUMN "assist"."users"."id" IS 'The user's system-generated  ID.';
COMMENT ON COLUMN "assist"."users"."created_by" IS 'The system-generated ID of person creating record.';
COMMENT ON COLUMN "assist"."users"."creation_date" IS 'The calendar date when record was created.';
COMMENT ON COLUMN "assist"."users"."last_updated_by" IS 'The ID number of person who last modified record.';
COMMENT ON COLUMN "assist"."users"."last_update_date" IS 'System-generated date record was last modified.';
COMMENT ON COLUMN "assist"."users"."delete_flag" IS 'The indicator of whether the record is to be deleted.';
COMMENT ON COLUMN "assist"."users"."first_name" IS 'The user's first name.';
COMMENT ON COLUMN "assist"."users"."middle_initial" IS 'The user's middle initial.';
COMMENT ON COLUMN "assist"."users"."last_name" IS 'The user's last name.';
COMMENT ON COLUMN "assist"."users"."phone" IS 'The user's office phone number.';
COMMENT ON COLUMN "assist"."users"."phone_ext" IS 'The user's office phone extension.';
COMMENT ON COLUMN "assist"."users"."fax" IS 'The user's office facsimile number.';
COMMENT ON COLUMN "assist"."users"."email" IS 'The user's office E-mail address.';
COMMENT ON COLUMN "assist"."users"."last_login" IS 'The date the user registered or last successfully logged in.';
COMMENT ON COLUMN "assist"."users"."last_failed_login" IS 'The date the user last failed at a login attempt.';
COMMENT ON COLUMN "assist"."users"."success_ip_address" IS 'The IP address for the last successful login attempt.';
COMMENT ON COLUMN "assist"."users"."fail_ip_address" IS 'The IP address for the last failed login attempt.';
COMMENT ON COLUMN "assist"."users"."authentication_type" IS 'The type of authentication performed. For example - Windows or Domino.';
COMMENT ON COLUMN "assist"."users"."inactive_date" IS 'The date a user is no longer registered.';
COMMENT ON COLUMN "assist"."users"."remarks" IS 'The text that describes comments about user.';
COMMENT ON COLUMN "assist"."users"."last_reg_status_updated_date" IS 'The last updated date when the reg status has been change.';
COMMENT ON COLUMN "assist"."users"."activation_date" IS 'The date at which the scheduled job, "ActivateEmployeeUserJob", should set the user's account to APPROVED;  the scheduled job only works on users with PENDING_ACTIVATION status.  This field is currently only utilized for EMPLOYEE users.';
COMMENT ON COLUMN "assist"."users"."special_instructions" IS 'Special Instructions related to the creation of the user.';
COMMENT ON COLUMN "assist"."users"."password_expirable" IS 'Y if the password can expire, otherwise N.';
COMMENT ON COLUMN "assist"."users"."departure_date" IS 'Used by Reg to auto-reject user on specified date';
COMMENT ON COLUMN "assist"."users"."login_email" IS 'User's unique email id used for FAS ID login';
COMMENT ON COLUMN "assist"."users"."account_to_retain_yn" IS 'Boolean indicator to identify the account to retain for users with multiple accounts';
COMMENT ON COLUMN "assist"."users"."requires_fas_email_yn" IS 'Boolean indicator to flag if a FAS ID login email needs to be requested for the user';
COMMENT ON COLUMN "assist"."users"."okta_user_id" IS 'Unique Okta-generated ID of User';

-- ------------------------------------------------------------
-- Table: table_master.clients
-- ------------------------------------------------------------

CREATE TABLE "table_master"."clients" (
    "client_id" character varying(16) NOT NULL,
    "client_name" character varying(75) NOT NULL,
    "reg_status" character varying(15) NOT NULL,
    "created_by" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "creation_date" timestamp(0) without time zone DEFAULT now() NOT NULL,
    "last_updated_by" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "last_update_date" timestamp(0) without time zone DEFAULT now() NOT NULL,
    "client_code" character varying(9),
    "phone" character varying(24),
    "phone_ext" character varying(5),
    "fax" character varying(24),
    "email" character varying(80),
    "street_1" character varying(75),
    "street_2" character varying(75),
    "city" character varying(30),
    "state" character varying(2),
    "zip" character varying(10),
    "country" character varying(30) DEFAULT 'United States'::character varying,
    "agency_code" character varying(2),
    "bureau_code" character varying(2),
    "ship_to_attention" character varying(35),
    "ship_to_street_1" character varying(75),
    "ship_to_street_2" character varying(75),
    "ship_to_city" character varying(30),
    "ship_to_state" character varying(2),
    "ship_to_zip" character varying(10),
    "ship_to_country" character varying(30) DEFAULT 'United States'::character varying,
    "client_acct_num" character varying(7),
    "client_remarks" character varying(250),
    "gsa_client_flag" character varying(1) DEFAULT 'N'::character varying,
    "client_active_flag" character varying(1) DEFAULT 'N'::character varying,
    "gsa_contact" character varying(75),
    "mou_number" character varying(50),
    "delete_flag" character varying(1) DEFAULT 'N'::character varying NOT NULL,
    "contract_country" character varying(30),
    CONSTRAINT "cli_pk" PRIMARY KEY (client_id),
    CONSTRAINT "avcon_1194864_clien_000" CHECK (client_active_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "avcon_1194864_delet_000" CHECK (delete_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "avcon_1194864_gsa_c_000" CHECK (gsa_client_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "cli_ctry_fk" FOREIGN KEY (country) REFERENCES table_master.lu_country(country),
    CONSTRAINT "cli_f952_fk" FOREIGN KEY (bureau_code, agency_code) REFERENCES table_master.lu_federal_agency(bureau_code, agency_code),
    CONSTRAINT "cli_lurs_fk" FOREIGN KEY (reg_status) REFERENCES table_master.lu_reg_status(reg_status),
    CONSTRAINT "cli_lust_cli_fk" FOREIGN KEY (state) REFERENCES table_master.lu_state(state),
    CONSTRAINT "cli_lust_ship_fk" FOREIGN KEY (ship_to_state) REFERENCES table_master.lu_state(state),
    CONSTRAINT "cli_sh_to_ctry_fk" FOREIGN KEY (ship_to_country) REFERENCES table_master.lu_country(country)
);

CREATE INDEX cli_emp_fk_i ON table_master.clients USING btree (gsa_contact);
CREATE INDEX cli_f952_fk_i ON table_master.clients USING btree (bureau_code, agency_code);
CREATE INDEX cli_idx1 ON table_master.clients USING btree (bureau_code);
CREATE INDEX cli_idx2 ON table_master.clients USING btree (agency_code);
CREATE INDEX cli_idx3 ON table_master.clients USING btree (last_update_date);
CREATE INDEX cli_lurs_fk_i ON table_master.clients USING btree (reg_status);
CREATE INDEX cli_lust_cli_fk_i ON table_master.clients USING btree (state);
CREATE INDEX cli_lust_ship_fk_i ON table_master.clients USING btree (ship_to_state);
CREATE UNIQUE INDEX cli_name_uk ON table_master.clients USING btree (client_name);
CREATE INDEX ix_c_country ON table_master.clients USING btree (country);
CREATE INDEX ix_c_ship_to_country ON table_master.clients USING btree (ship_to_country);

CREATE TRIGGER cli_a_u_d_row BEFORE DELETE OR UPDATE ON table_master.clients FOR EACH ROW EXECUTE FUNCTION table_master."cli_a_u_d_row$clients"();
CREATE TRIGGER cli_b_i_u_row BEFORE INSERT OR UPDATE ON table_master.clients FOR EACH ROW EXECUTE FUNCTION table_master."cli_b_i_u_row$clients"();

COMMENT ON COLUMN "table_master"."clients"."client_id" IS 'The ID number of client organization.';
COMMENT ON COLUMN "table_master"."clients"."client_name" IS 'The name of client organization.';
COMMENT ON COLUMN "table_master"."clients"."reg_status" IS 'The registration status of the client.';
COMMENT ON COLUMN "table_master"."clients"."created_by" IS 'The system ID of client generating record.';
COMMENT ON COLUMN "table_master"."clients"."creation_date" IS 'The calendar date the record was generated.';
COMMENT ON COLUMN "table_master"."clients"."last_updated_by" IS 'The system ID of person last modifying record.';
COMMENT ON COLUMN "table_master"."clients"."last_update_date" IS 'The calendar date when the record was last modified.';
COMMENT ON COLUMN "table_master"."clients"."client_code" IS 'The 2 digit agency code + 5 digit zipcode + 2 digit sequence.';
COMMENT ON COLUMN "table_master"."clients"."phone" IS 'The phone number associated with address.';
COMMENT ON COLUMN "table_master"."clients"."phone_ext" IS 'The phone number extension associated with address.';
COMMENT ON COLUMN "table_master"."clients"."fax" IS 'The client''s facsimile number.';
COMMENT ON COLUMN "table_master"."clients"."email" IS 'The E-mail address of client.';
COMMENT ON COLUMN "table_master"."clients"."street_1" IS 'The first line of the address.';
COMMENT ON COLUMN "table_master"."clients"."street_2" IS 'The second line of address.';
COMMENT ON COLUMN "table_master"."clients"."city" IS 'The city line of address.';
COMMENT ON COLUMN "table_master"."clients"."state" IS 'The state line of address.';
COMMENT ON COLUMN "table_master"."clients"."zip" IS 'The zipcode line of address.';
COMMENT ON COLUMN "table_master"."clients"."country" IS 'The country line of address.';
COMMENT ON COLUMN "table_master"."clients"."agency_code" IS 'The code that represents the FIPS agency.';
COMMENT ON COLUMN "table_master"."clients"."bureau_code" IS 'The code that represents the FIPS Bureau.';
COMMENT ON COLUMN "table_master"."clients"."ship_to_attention" IS 'The default ship to address attention line.';
COMMENT ON COLUMN "table_master"."clients"."ship_to_street_1" IS 'The first line of the default ship to address.';
COMMENT ON COLUMN "table_master"."clients"."ship_to_street_2" IS 'The second line of default ship to address.';
COMMENT ON COLUMN "table_master"."clients"."ship_to_city" IS 'The default ship to address city line.';
COMMENT ON COLUMN "table_master"."clients"."ship_to_state" IS 'The default ship to address state line.';
COMMENT ON COLUMN "table_master"."clients"."ship_to_zip" IS 'The default ship to address zipcode line.';
COMMENT ON COLUMN "table_master"."clients"."ship_to_country" IS 'The default ship to address country line.';
COMMENT ON COLUMN "table_master"."clients"."client_acct_num" IS 'The client''s account number.';
COMMENT ON COLUMN "table_master"."clients"."client_remarks" IS 'The text that describes the client''s comments.';
COMMENT ON COLUMN "table_master"."clients"."gsa_client_flag" IS 'The identifier that indicates if  this is a GSA client.';
COMMENT ON COLUMN "table_master"."clients"."client_active_flag" IS 'The identifier that indicates if the client is active.';
COMMENT ON COLUMN "table_master"."clients"."gsa_contact" IS 'The GSA contact to notify when an order is started.';
COMMENT ON COLUMN "table_master"."clients"."mou_number" IS 'The MOU number associated with client.';
COMMENT ON COLUMN "table_master"."clients"."delete_flag" IS 'The identifier that indicates if the record is to be deleted.';

-- ------------------------------------------------------------
-- Table: table_master.contracts
-- ------------------------------------------------------------

CREATE TABLE "table_master"."contracts" (
    "contract_id" character varying(16) NOT NULL,
    "created_by" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "creation_date" timestamp(0) without time zone DEFAULT now() NOT NULL,
    "last_updated_by" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "last_update_date" timestamp(0) without time zone DEFAULT now() NOT NULL,
    "delete_flag" character varying(1) DEFAULT 'N'::character varying NOT NULL,
    "ipartner_id" character varying(16) NOT NULL,
    "contract_num" character varying(30) NOT NULL,
    "reg_status" character varying(15) NOT NULL,
    "ipartner_ftin" character varying(11) NOT NULL,
    "contract_to_date" timestamp(0) without time zone NOT NULL,
    "contact_street_1" character varying(75) NOT NULL,
    "contact_city" character varying(30) NOT NULL,
    "contact_state" character varying(2) NOT NULL,
    "contact_zip" character varying(10) NOT NULL,
    "contact" character varying(35),
    "contact_street_2" character varying(75),
    "contact_country" character varying(30) DEFAULT 'United States'::character varying,
    "contract_name" character varying(250),
    "contract_from_date" timestamp(0) without time zone,
    "duns_num" character varying(13),
    "co_emp_id" character varying(16),
    "remarks" character varying(2000),
    "lookup_ipartner_company_name" character varying(120),
    "lookup_emp_name" character varying(75),
    "lookup_contract_number" character varying(75),
    "cage_code" character varying(6),
    "contract_expire_date" timestamp(0) without time zone,
    "contract_street_1" character varying(75),
    "contract_street_2" character varying(75),
    "contract_city" character varying(30),
    "contract_state" character varying(2),
    "contract_zip" character varying(10),
    "contract_company" character varying(120),
    "contract_status" character varying(15),
    "company_org_type" character varying(1),
    "company_classification" character varying(2),
    "industry_type" character varying(30),
    "email" character varying(80),
    "fss_contract_flag" character varying(1),
    "reseller_id" character varying(16),
    "reseller_company_name" character varying(120),
    "contract_family_id" bigint,
    "fss_contract_update_date" timestamp(0) without time zone,
    "phone" character varying(24),
    "discount_terms" character varying(50),
    "issuing_agency" character varying(50),
    "payment_terms" character varying(20),
    "parent_name" character varying(50),
    "government_co" character varying(100),
    "government_co_phone" character varying(20),
    "naics" character varying(20),
    "contact_fax" character varying(24),
    "duns_plus4" character varying(4),
    "assist_idv" character varying(11),
    "uei" character varying(16),
    "uei_plus4" character varying(8),
    "cohort_num" bigint,
    "vuei" character varying(16) GENERATED ALWAYS AS (
CASE
    WHEN (uei IS NULL) THEN duns_num
    ELSE uei
END) STORED,
    "vuei_plus4" character varying(13) GENERATED ALWAYS AS (
CASE
    WHEN (uei_plus4 IS NULL) THEN duns_plus4
    ELSE uei_plus4
END) STORED,
    CONSTRAINT "con_pk" PRIMARY KEY (contract_id),
    CONSTRAINT "avcon_1194866_delet_000" CHECK (delete_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "ck_cont_fss_contr_flag" CHECK (fss_contract_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "con_co_emp_fk" FOREIGN KEY (co_emp_id) REFERENCES assist.users_employees(emp_id),
    CONSTRAINT "con_cont_fmly_id_fk" FOREIGN KEY (contract_family_id) REFERENCES table_master.contract_families(contract_family_id),
    CONSTRAINT "con_ip_fk" FOREIGN KEY (ipartner_id) REFERENCES table_master.industry_partners(ipartner_id),
    CONSTRAINT "con_lu_bus_type_fk" FOREIGN KEY (company_org_type) REFERENCES table_master.lu_business_type(business_type),
    CONSTRAINT "con_lust_contact_fk" FOREIGN KEY (contact_state) REFERENCES table_master.lu_state(state),
    CONSTRAINT "con_sh_to_ctry_fk" FOREIGN KEY (contact_country) REFERENCES table_master.lu_country(country),
    CONSTRAINT "cont_res_id_fk" FOREIGN KEY (reseller_id) REFERENCES table_master.industry_partners(ipartner_id),
    CONSTRAINT "contracts_sam_bus_type_fk" FOREIGN KEY (company_classification) REFERENCES table_master.lu_sam_bus_type_mapping(bus_type_code)
);

CREATE INDEX con_co_emp_fk_i ON table_master.contracts USING btree (co_emp_id);
CREATE INDEX con_idx1 ON table_master.contracts USING btree (last_update_date);
CREATE INDEX con_ip_fk_i ON table_master.contracts USING btree (ipartner_id);
CREATE INDEX con_lurs_fk_i ON table_master.contracts USING btree (reg_status);
CREATE INDEX con_lust_contact_fk_i ON table_master.contracts USING btree (contact_state);
CREATE UNIQUE INDEX con_num_uk ON table_master.contracts USING btree (lookup_contract_number);
CREATE INDEX contracts_lu_cont_num_func_idx ON table_master.contracts USING btree (upper((lookup_contract_number)::text));
CREATE INDEX ix_c_company_classification ON table_master.contracts USING btree (company_classification);
CREATE INDEX ix_c_company_org_type ON table_master.contracts USING btree (company_org_type);
CREATE INDEX ix_c_contact_country ON table_master.contracts USING btree (contact_country);
CREATE INDEX ix_c_contract_family_id ON table_master.contracts USING btree (contract_family_id);
CREATE INDEX ix_c_reseller_id ON table_master.contracts USING btree (reseller_id);

CREATE TRIGGER con_a_u_d_row BEFORE DELETE OR UPDATE ON table_master.contracts FOR EACH ROW EXECUTE FUNCTION table_master."con_a_u_d_row$contracts"();
CREATE TRIGGER con_b_i_u_row BEFORE INSERT OR UPDATE ON table_master.contracts FOR EACH ROW EXECUTE FUNCTION table_master."con_b_i_u_row$contracts"();

COMMENT ON COLUMN "table_master"."contracts"."contract_id" IS 'The system-generated ID of contract.';
COMMENT ON COLUMN "table_master"."contracts"."created_by" IS 'The system ID of person generating record.';
COMMENT ON COLUMN "table_master"."contracts"."creation_date" IS 'The calendar date when the record was generated.';
COMMENT ON COLUMN "table_master"."contracts"."last_updated_by" IS 'The ID number of person who last modified record.';
COMMENT ON COLUMN "table_master"."contracts"."last_update_date" IS 'The calendar date that denotes when the record last modified.';
COMMENT ON COLUMN "table_master"."contracts"."delete_flag" IS 'The identifier that indicates if the record is to be deleted.';
COMMENT ON COLUMN "table_master"."contracts"."ipartner_id" IS 'The ID number of the Industry Partner owning contract.';
COMMENT ON COLUMN "table_master"."contracts"."contract_num" IS 'The contract number.';
COMMENT ON COLUMN "table_master"."contracts"."reg_status" IS 'The registration status of the contract.';
COMMENT ON COLUMN "table_master"."contracts"."ipartner_ftin" IS 'The tax ID number of the Industry Partner.';
COMMENT ON COLUMN "table_master"."contracts"."contract_to_date" IS 'The calendar date that denotes when the contract expires.';
COMMENT ON COLUMN "table_master"."contracts"."contact_street_1" IS 'The contact address first line.';
COMMENT ON COLUMN "table_master"."contracts"."contact_city" IS 'The contact address city line.';
COMMENT ON COLUMN "table_master"."contracts"."contact_state" IS 'The contact address state line.';
COMMENT ON COLUMN "table_master"."contracts"."contact_zip" IS 'The contact address zipcode line.';
COMMENT ON COLUMN "table_master"."contracts"."contact" IS 'The point of contact for the contract.';
COMMENT ON COLUMN "table_master"."contracts"."contact_street_2" IS 'The contact address second line.';
COMMENT ON COLUMN "table_master"."contracts"."contact_country" IS 'The contact address country line.';
COMMENT ON COLUMN "table_master"."contracts"."contract_name" IS 'The name of the contract.';
COMMENT ON COLUMN "table_master"."contracts"."contract_from_date" IS 'The calendar date that denotes the start of contract.';
COMMENT ON COLUMN "table_master"."contracts"."duns_num" IS 'The industry partner DUNS number.';
COMMENT ON COLUMN "table_master"."contracts"."co_emp_id" IS 'The contracting officer's employee ID number.';
COMMENT ON COLUMN "table_master"."contracts"."remarks" IS 'The text that describes comments about the contract.';
COMMENT ON COLUMN "table_master"."contracts"."lookup_ipartner_company_name" IS ''Used by ITSS to lookup unique name.';
COMMENT ON COLUMN "table_master"."contracts"."lookup_emp_name" IS 'Used by ITSS to lookup unique name.';
COMMENT ON COLUMN "table_master"."contracts"."lookup_contract_number" IS 'The unique contract number, used by ITSS to create foreign key links.';
COMMENT ON COLUMN "table_master"."contracts"."cage_code" IS 'The cage code of the company.';
COMMENT ON COLUMN "table_master"."contracts"."contract_expire_date" IS 'The expiration date of the contract.';
COMMENT ON COLUMN "table_master"."contracts"."contract_street_1" IS 'The street address 1 on the contract.';
COMMENT ON COLUMN "table_master"."contracts"."contract_street_2" IS 'The street address 2 on the contract.';
COMMENT ON COLUMN "table_master"."contracts"."contract_city" IS 'The city address on the contract.';
COMMENT ON COLUMN "table_master"."contracts"."contract_state" IS 'The state address on the contract.';
COMMENT ON COLUMN "table_master"."contracts"."contract_zip" IS 'The zip code address on the contract.';
COMMENT ON COLUMN "table_master"."contracts"."contract_company" IS 'The company name on the contract.';
COMMENT ON COLUMN "table_master"."contracts"."contract_status" IS 'The status of the contract.';
COMMENT ON COLUMN "table_master"."contracts"."company_org_type" IS 'The type of organization the company is.';
COMMENT ON COLUMN "table_master"."contracts"."company_classification" IS 'The company classification.';
COMMENT ON COLUMN "table_master"."contracts"."industry_type" IS 'The type of industry on the contract.';
COMMENT ON COLUMN "table_master"."contracts"."email" IS 'The email address on the contract.';
COMMENT ON COLUMN "table_master"."contracts"."fss_contract_flag" IS 'The identifier that indicates the contract as an FSS contract.';
COMMENT ON COLUMN "table_master"."contracts"."reseller_id" IS 'The Industry Partner ID of the reseller.';
COMMENT ON COLUMN "table_master"."contracts"."reseller_company_name" IS 'The Industry Partner Name of the reseller.';
COMMENT ON COLUMN "table_master"."contracts"."contract_family_id" IS 'The unique identifier for the contract family the contract belongs to.';
COMMENT ON COLUMN "table_master"."contracts"."fss_contract_update_date" IS 'The date the record was last updated with FSS-19 contract data.';
COMMENT ON COLUMN "table_master"."contracts"."discount_terms" IS 'The discount terms.';
COMMENT ON COLUMN "table_master"."contracts"."uei" IS 'Unique Entity Identifier (UEI) - replaces DUNS';
COMMENT ON COLUMN "table_master"."contracts"."uei_plus4" IS 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4';
COMMENT ON COLUMN "table_master"."contracts"."cohort_num" IS 'The cohort number that the contract was awarded in.';

-- ------------------------------------------------------------
-- Table: table_master.industry_partners
-- ------------------------------------------------------------

CREATE TABLE "table_master"."industry_partners" (
    "ipartner_id" character varying(16) NOT NULL,
    "reg_status" character varying(15) NOT NULL,
    "created_by" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "creation_date" timestamp(0) without time zone DEFAULT now() NOT NULL,
    "last_updated_by" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "last_update_date" timestamp(0) without time zone DEFAULT now() NOT NULL,
    "delete_flag" character varying(1) DEFAULT 'N'::character varying NOT NULL,
    "ipartner_company_name" character varying(120) NOT NULL,
    "ipartner_location" character varying(30),
    "street_1" character varying(75),
    "street_2" character varying(75),
    "city" character varying(30),
    "state" character varying(2),
    "zip" character varying(10),
    "country" character varying(30) DEFAULT 'United States'::character varying,
    "phone" character varying(24),
    "phone_ext" character varying(5),
    "fax" character varying(24),
    "email" character varying(80),
    "duns_num" character varying(13),
    "small_business_class" character varying(1),
    "ipartner_inc_status" character varying(1),
    "industry_type" character varying(30),
    "ipartner_cec" character varying(10),
    "certified_8a_flag" character varying(1),
    "sba_contact" character varying(35),
    "sba_street_1" character varying(75),
    "sba_street_2" character varying(75),
    "sba_city" character varying(30),
    "sba_state" character varying(2),
    "sba_zip" character varying(10),
    "sba_country" character varying(30),
    "sba_phone" character varying(24),
    "sba_phone_ext" character varying(5),
    "sba_fax" character varying(24),
    "sba_email" character varying(80),
    "open_market_flag" character(1) DEFAULT 'N'::character(1),
    "ipartner_cage_code" character varying(6),
    "ipartner_web_site" character varying(80),
    "ipartner_eft_bank_name" character varying(120),
    "ipartner_eft_num" numeric(19,2),
    "ipartner_eft_acct_type" character varying(1),
    "ipartner_eft_transfer_num" character varying(9),
    "ipartner_ftin" character varying(11),
    "ipartner_parent_name" character varying(35),
    "ipartner_parent_duns_num" character varying(13),
    "ipartner_parent_tin_num" character varying(11),
    "ipartner_contact" character varying(35),
    "net_days" numeric(19,2),
    "discount_days" numeric(19,2),
    "discount_percentage" numeric(6,3),
    "remit_street_1" character varying(75),
    "remit_street_2" character varying(75),
    "remit_city" character varying(30),
    "remit_state" character varying(100),
    "remit_zip" character varying(10),
    "remit_country" character varying(50) DEFAULT 'United States'::character varying,
    "remit_phone" character varying(24),
    "remit_phone_ext" character varying(5),
    "remit_fax" character varying(24),
    "remit_email" character varying(80),
    "remarks" character varying(250),
    "products_and_services" character varying(2000),
    "order_street_1" character varying(75),
    "order_street_2" character varying(75),
    "order_city" character varying(30),
    "order_state" character varying(2),
    "order_zip" character varying(10),
    "order_country" character varying(30),
    "coverage_areas" character varying(600),
    "buyers_role" character varying(1),
    "duns_plus4" character varying(4),
    "sam_last_update_date" timestamp(0) without time zone,
    "native_american_company" character varying(1) DEFAULT 'N'::character varying,
    "ipartner_parent_uei" character varying(16),
    "uei" character varying(16),
    "uei_plus4" character varying(8),
    "ipartner_parent_vuei" character varying(16) GENERATED ALWAYS AS (
CASE
    WHEN (ipartner_parent_uei IS NULL) THEN ipartner_parent_duns_num
    ELSE ipartner_parent_uei
END) STORED,
    "vuei" character varying(16) GENERATED ALWAYS AS (
CASE
    WHEN (NULLIF((uei)::text, ''::text) IS NULL) THEN (NULLIF((duns_num)::text, ''::text))::character varying
    ELSE uei
END) STORED,
    "vuei_plus4" character varying(13) GENERATED ALWAYS AS (
CASE
    WHEN (NULLIF((uei_plus4)::text, ''::text) IS NULL) THEN (NULLIF((duns_plus4)::text, ''::text))::character varying
    ELSE uei_plus4
END) STORED,
    CONSTRAINT "ip_pk" PRIMARY KEY (ipartner_id),
    CONSTRAINT "avcon_1194871_certi_000" CHECK (certified_8a_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "avcon_1194871_delet_000" CHECK (delete_flag::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "avcon_1194871_ipart_000" CHECK (ipartner_eft_acct_type::text = ANY (ARRAY['S'::character varying::text, 'C'::character varying::text])),
    CONSTRAINT "avcon_1194871_open__000" CHECK (open_market_flag = ANY (ARRAY['Y'::bpchar, 'N'::bpchar])),
    CONSTRAINT "ip_ctry_fk" FOREIGN KEY (country) REFERENCES table_master.lu_country(country),
    CONSTRAINT "ip_lubst_fk" FOREIGN KEY (ipartner_inc_status) REFERENCES table_master.lu_business_type(business_type),
    CONSTRAINT "ip_luit_fk" FOREIGN KEY (industry_type) REFERENCES table_master.lu_industry_type(industry_type),
    CONSTRAINT "ip_lurs_fk" FOREIGN KEY (reg_status) REFERENCES table_master.lu_reg_status(reg_status),
    CONSTRAINT "ip_lusbc_fk" FOREIGN KEY (small_business_class) REFERENCES table_master.lu_small_business_class(small_business_class),
    CONSTRAINT "ip_lust_fk" FOREIGN KEY (state) REFERENCES table_master.lu_state(state),
    CONSTRAINT "ip_lust_order_fk" FOREIGN KEY (order_state) REFERENCES table_master.lu_state(state),
    CONSTRAINT "ip_lust_sba_fk" FOREIGN KEY (sba_state) REFERENCES table_master.lu_state(state),
    CONSTRAINT "ip_ord_ctry_fk" FOREIGN KEY (order_country) REFERENCES table_master.lu_country(country),
    CONSTRAINT "ip_rmt_ctry_fk" FOREIGN KEY (remit_country) REFERENCES table_master.lu_country(country),
    CONSTRAINT "ip_sba_ctry_fk" FOREIGN KEY (sba_country) REFERENCES table_master.lu_country(country)
);

CREATE INDEX ip_city_idx ON table_master.industry_partners USING btree (upper((city)::text));
CREATE INDEX ip_duns_num_idx ON table_master.industry_partners USING btree (duns_num);
CREATE INDEX ip_duns_plus4_nvl_idx ON table_master.industry_partners USING btree (COALESCE(duns_plus4, '    '::character varying));
CREATE INDEX ip_duns_plus_4_idx ON table_master.industry_partners USING btree (duns_plus4);
CREATE INDEX ip_idx1 ON table_master.industry_partners USING btree (last_update_date);
CREATE INDEX ip_ipartner_cage_code_idx ON table_master.industry_partners USING btree (upper((ipartner_cage_code)::text));
CREATE INDEX ip_lubst_fk_i ON table_master.industry_partners USING btree (ipartner_inc_status);
CREATE INDEX ip_luit_fk_i ON table_master.industry_partners USING btree (industry_type);
CREATE INDEX ip_lurs_fk_i ON table_master.industry_partners USING btree (reg_status);
CREATE INDEX ip_lusbc_fk_i ON table_master.industry_partners USING btree (small_business_class);
CREATE INDEX ip_lust_fk_i ON table_master.industry_partners USING btree (state);
CREATE INDEX ip_lust_order_fk_i ON table_master.industry_partners USING btree (order_state);
CREATE INDEX ip_lust_remit_fk_i ON table_master.industry_partners USING btree (remit_state);
CREATE INDEX ip_lust_sba_fk_i ON table_master.industry_partners USING btree (sba_state);
CREATE INDEX ip_state_idx ON table_master.industry_partners USING btree (upper((state)::text));
CREATE INDEX ip_street_1_idx ON table_master.industry_partners USING btree (upper((street_1)::text));
CREATE INDEX ip_upper_ip_company_name_idx ON table_master.industry_partners USING btree (upper((ipartner_company_name)::text));
CREATE INDEX ip_zip_substr_idx ON table_master.industry_partners USING btree (substr((zip)::text, 1, 5));
CREATE INDEX ix_ip_country ON table_master.industry_partners USING btree (country);
CREATE INDEX ix_ip_order_country ON table_master.industry_partners USING btree (order_country);
CREATE INDEX ix_ip_remit_country ON table_master.industry_partners USING btree (remit_country);
CREATE INDEX ix_ip_sba_country ON table_master.industry_partners USING btree (sba_country);

CREATE TRIGGER ip_a_u_d_row BEFORE DELETE OR UPDATE ON table_master.industry_partners FOR EACH ROW EXECUTE FUNCTION table_master."ip_a_u_d_row$industry_partners"();
CREATE TRIGGER ip_b_u_row BEFORE UPDATE ON table_master.industry_partners FOR EACH ROW EXECUTE FUNCTION table_master."ip_b_u_row$industry_partners"();

COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_id" IS 'The ITSS system-generated Primary Key.';
COMMENT ON COLUMN "table_master"."industry_partners"."reg_status" IS 'The registration status.';
COMMENT ON COLUMN "table_master"."industry_partners"."created_by" IS 'The system ID of the person generating the record.';
COMMENT ON COLUMN "table_master"."industry_partners"."creation_date" IS 'The calendar date that denotes when record was generated.';
COMMENT ON COLUMN "table_master"."industry_partners"."last_updated_by" IS 'The ID number of person who last modified record.';
COMMENT ON COLUMN "table_master"."industry_partners"."last_update_date" IS 'The system-generated date the record was last modified.';
COMMENT ON COLUMN "table_master"."industry_partners"."delete_flag" IS 'The identifier that indicates whether the record is to be deleted.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_company_name" IS 'The industry partner company name.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_location" IS 'The operating location of company.';
COMMENT ON COLUMN "table_master"."industry_partners"."street_1" IS 'The first line of the address.';
COMMENT ON COLUMN "table_master"."industry_partners"."street_2" IS 'The second line of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."city" IS 'The city line of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."state" IS 'The state line of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."zip" IS 'The zipcode line of address.  The first 5 numbers are required.';
COMMENT ON COLUMN "table_master"."industry_partners"."country" IS 'The country line of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."phone" IS 'The phone number associated with the address.';
COMMENT ON COLUMN "table_master"."industry_partners"."phone_ext" IS 'The phone number extension associated with the address.';
COMMENT ON COLUMN "table_master"."industry_partners"."fax" IS 'The facsimile number associated with the industry partner.';
COMMENT ON COLUMN "table_master"."industry_partners"."email" IS 'The E-mail address.';
COMMENT ON COLUMN "table_master"."industry_partners"."duns_num" IS 'The industry partner DUNS number.';
COMMENT ON COLUMN "table_master"."industry_partners"."small_business_class" IS 'The small business code, 8a.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_inc_status" IS 'The incorporation status (corporation, partnership, etc).';
COMMENT ON COLUMN "table_master"."industry_partners"."industry_type" IS 'The industry type.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_cec" IS 'The code that represents the contractor establishment.';
COMMENT ON COLUMN "table_master"."industry_partners"."certified_8a_flag" IS 'The identifier that indicates if company is 8A certified.';
COMMENT ON COLUMN "table_master"."industry_partners"."sba_contact" IS 'The name of SBA contact.';
COMMENT ON COLUMN "table_master"."industry_partners"."sba_street_1" IS 'The SBA contact first line of the address.';
COMMENT ON COLUMN "table_master"."industry_partners"."sba_street_2" IS 'The SBA contact second line of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."sba_city" IS 'The SBA contact city line.';
COMMENT ON COLUMN "table_master"."industry_partners"."sba_state" IS 'The SBA contact state line.';
COMMENT ON COLUMN "table_master"."industry_partners"."sba_zip" IS 'The SBA contact zipcode.  The first 5 numbers are required.';
COMMENT ON COLUMN "table_master"."industry_partners"."sba_country" IS 'The SBA contact country line.';
COMMENT ON COLUMN "table_master"."industry_partners"."sba_phone" IS 'The SBA contact phone number associated with address.';
COMMENT ON COLUMN "table_master"."industry_partners"."sba_phone_ext" IS 'The SBA contact phone extension associated with address.';
COMMENT ON COLUMN "table_master"."industry_partners"."sba_fax" IS 'The SBA contact facsimile number associated with address.';
COMMENT ON COLUMN "table_master"."industry_partners"."sba_email" IS 'The SBA contact E-mail address.';
COMMENT ON COLUMN "table_master"."industry_partners"."open_market_flag" IS 'The identifier that indicates whether the industry partner participates in open market orders.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_cage_code" IS 'The code that represents the industry partner''s cage code.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_web_site" IS 'The industry partner''s web page address.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_eft_bank_name" IS 'The industry partner''s bank name.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_eft_num" IS 'The industry partner''s electronic funds transfer account number.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_eft_acct_type" IS 'The identifier that indicates whether this is a savings or checking account.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_eft_transfer_num" IS 'The 9-digit routing code for an account.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_ftin" IS 'The industry partner''s federal taxpayer ID number.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_parent_name" IS 'The industry partner''s parent company name.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_parent_duns_num" IS 'The industry partner''s parent company''s DUNS number.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_parent_tin_num" IS 'The industry partner''s parent company''s Taxpayer Identification Number.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_contact" IS 'The industry partner point of contact name.';
COMMENT ON COLUMN "table_master"."industry_partners"."net_days" IS 'The default days to pay an invoice before a penalty.';
COMMENT ON COLUMN "table_master"."industry_partners"."discount_days" IS 'The default discount days to populate order.';
COMMENT ON COLUMN "table_master"."industry_partners"."discount_percentage" IS 'The default discount percentage to populate order.';
COMMENT ON COLUMN "table_master"."industry_partners"."remit_street_1" IS 'The first line of the address.';
COMMENT ON COLUMN "table_master"."industry_partners"."remit_street_2" IS 'The second line of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."remit_city" IS 'The remmittance city of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."remit_state" IS 'The remmittance state line of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."remit_zip" IS 'The remmittance zipcode line of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."remit_country" IS 'The remmittance country line of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."remit_phone" IS 'The remmittance phone number associated with address.';
COMMENT ON COLUMN "table_master"."industry_partners"."remit_phone_ext" IS 'The remmittance phone extension associated with address.';
COMMENT ON COLUMN "table_master"."industry_partners"."remit_fax" IS 'The remmittance facsimile number associated with address.';
COMMENT ON COLUMN "table_master"."industry_partners"."remit_email" IS 'The remmittance E-mail address.';
COMMENT ON COLUMN "table_master"."industry_partners"."remarks" IS 'The text that describes comments about industry partner.';
COMMENT ON COLUMN "table_master"."industry_partners"."products_and_services" IS 'The industry partner list of products and services.';
COMMENT ON COLUMN "table_master"."industry_partners"."order_street_1" IS 'The first line of the address.';
COMMENT ON COLUMN "table_master"."industry_partners"."order_street_2" IS 'The second line of the address.';
COMMENT ON COLUMN "table_master"."industry_partners"."order_city" IS 'The city of address line.';
COMMENT ON COLUMN "table_master"."industry_partners"."order_state" IS 'The state line of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."order_zip" IS 'The zipcode line of address.  The first 5 numbers are required.';
COMMENT ON COLUMN "table_master"."industry_partners"."order_country" IS 'The country line of address.';
COMMENT ON COLUMN "table_master"."industry_partners"."coverage_areas" IS 'The geographic areas the industry partner supports.';
COMMENT ON COLUMN "table_master"."industry_partners"."buyers_role" IS 'The Buyers.gov role, if applicable.';
COMMENT ON COLUMN "table_master"."industry_partners"."ipartner_parent_uei" IS 'Unique Entity Identifier (UEI) - replaces DUNS';
COMMENT ON COLUMN "table_master"."industry_partners"."uei" IS 'Unique Entity Identifier (UEI) - replaces DUNS';
COMMENT ON COLUMN "table_master"."industry_partners"."uei_plus4" IS 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4';
COMMENT ON COLUMN "table_master"."industry_partners"."vuei" IS 'Generated column:  show UEI if present, otherwise show DUNS_NUM';
COMMENT ON COLUMN "table_master"."industry_partners"."vuei_plus4" IS 'Generated column:  show UEI_PLUS4 if present, otherwise show DUNS_PLUS4';

-- ------------------------------------------------------------
-- Table: table_master.lu_federal_agency
-- ------------------------------------------------------------

CREATE TABLE "table_master"."lu_federal_agency" (
    "agency_code" character varying(2) NOT NULL,
    "bureau_code" character varying(2) NOT NULL,
    "agency_description" character varying(50),
    "bureau_description" character varying(120),
    "inactive_date" timestamp(0) without time zone,
    "agency_code_3" character varying(3) NOT NULL,
    "agency_type" character varying(10),
    CONSTRAINT "f952_pk" PRIMARY KEY (bureau_code, agency_code)
);

CREATE INDEX f952_idx1 ON table_master.lu_federal_agency USING btree (bureau_code, inactive_date);
CREATE INDEX f952_idx2 ON table_master.lu_federal_agency USING btree (agency_code, inactive_date);
CREATE UNIQUE INDEX lufa_uk ON table_master.lu_federal_agency USING btree (bureau_code, agency_code_3);

COMMENT ON COLUMN "table_master"."lu_federal_agency"."agency_code" IS 'The code that represents the FIPS agency.';
COMMENT ON COLUMN "table_master"."lu_federal_agency"."bureau_code" IS 'The code that represents the FIPS Bureau.';
COMMENT ON COLUMN "table_master"."lu_federal_agency"."agency_description" IS 'The text that describes the Agency.';
COMMENT ON COLUMN "table_master"."lu_federal_agency"."bureau_description" IS 'The text that describes the Bureau.';
COMMENT ON COLUMN "table_master"."lu_federal_agency"."inactive_date" IS 'The calendar date that denotes when inactive.';
COMMENT ON COLUMN "table_master"."lu_federal_agency"."agency_code_3" IS 'UI Element: Requesting Agency pick-list. New 3-digit code for requesting agency on citations.';
COMMENT ON COLUMN "table_master"."lu_federal_agency"."agency_type" IS 'Use for Requesting Agency pick-list on funding citation and OMIS to differentiate between DOD vs other agencies.  Value is DOD or null';

-- ------------------------------------------------------------
-- Table: aasbs_history.accrual_inclusion_tracker
-- ------------------------------------------------------------

CREATE TABLE "aasbs_history"."accrual_inclusion_tracker" (
    "id" bigint,
    "li_tracking_num" bigint,
    "accrual_inclusion_status_cd" character varying(20),
    "was_manually_set_yn" character(1),
    "created_by_user_id" character varying(16),
    "created_dt" timestamp(6) without time zone,
    "updated_by_user_id" character varying(16),
    "updated_dt" timestamp(6) without time zone,
    "history_id" double precision,
    "hrec_archive_ts" timestamp(6) without time zone,
    "hrec_action_cd" character varying(8),
    "hrec_deleted_dt" timestamp(6) without time zone,
    "hrec_user_id" character varying(16),
    "usr_keyed_holdback" numeric(19,2),
    "comments" character varying(3000),
    "accrual_holdback_type_cd" character varying(30),
    "invalid_udo_amt" numeric(15,2),
    "local_transaction_id" character varying(50),
    "client_host" character varying(200),
    "client_ip_address" character varying(200)
);

CREATE INDEX idx_accrual_inclusion_tracker_hist_created_dt ON aasbs_history.accrual_inclusion_tracker USING btree (created_dt);
CREATE INDEX idx_accrual_inclusion_tracker_hist_hrec_action_cd ON aasbs_history.accrual_inclusion_tracker USING btree (hrec_action_cd);
CREATE INDEX idx_accrual_inclusion_tracker_hist_updated_dt ON aasbs_history.accrual_inclusion_tracker USING btree (updated_dt);
CREATE INDEX ix_accrual_inclusion_tracker_id ON aasbs_history.accrual_inclusion_tracker USING btree (id);

-- ------------------------------------------------------------
-- Table: aasbs_transmit.aac_envelope
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."aac_envelope" (
    "id" bigint NOT NULL,
    "activity_address_cd" character varying(6) NOT NULL,
    "transmittal_poc_id" bigint NOT NULL,
    "transmittal_envelope_id" bigint NOT NULL,
    "transmittal_status_cd" character varying(20) NOT NULL,
    "transmittal_type_cd" character varying(20) NOT NULL,
    "transmittal_prefix" character varying(20) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_aac_envelope" PRIMARY KEY (id),
    CONSTRAINT "fk_aac_envlop_aac" FOREIGN KEY (activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd),
    CONSTRAINT "fk_aac_envlop_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_aac_envlop_envelope_id" FOREIGN KEY (transmittal_envelope_id) REFERENCES aasbs_transmit.transmittal_envelope(id),
    CONSTRAINT "fk_aac_envlop_poc_id" FOREIGN KEY (transmittal_poc_id) REFERENCES aasbs_transmit.transmittal_poc(id),
    CONSTRAINT "fk_aac_envlop_trans_status_cd" FOREIGN KEY (transmittal_status_cd) REFERENCES aasbs_transmit.lu_transmittal_status(cd),
    CONSTRAINT "fk_aac_envlop_type_cd" FOREIGN KEY (transmittal_type_cd) REFERENCES aasbs_transmit.lu_transmittal_type(cd),
    CONSTRAINT "fk_aac_envlop_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_aac_envlop_aac ON aasbs_transmit.aac_envelope USING btree (activity_address_cd);
CREATE INDEX ix_aac_envlop_cbui ON aasbs_transmit.aac_envelope USING btree (created_by_user_id);
CREATE INDEX ix_aac_envlop_envelope_id ON aasbs_transmit.aac_envelope USING btree (transmittal_envelope_id);
CREATE INDEX ix_aac_envlop_poc_id ON aasbs_transmit.aac_envelope USING btree (transmittal_poc_id);
CREATE INDEX ix_aac_envlop_trans_status_cd ON aasbs_transmit.aac_envelope USING btree (transmittal_status_cd);
CREATE INDEX ix_aac_envlop_type_cd ON aasbs_transmit.aac_envelope USING btree (transmittal_type_cd);
CREATE INDEX ix_aac_envlop_ubui ON aasbs_transmit.aac_envelope USING btree (updated_by_user_id);

CREATE TRIGGER aac_envelope_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.aac_envelope FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_aac_envelope_hst();

COMMENT ON COLUMN "aasbs_transmit"."aac_envelope"."id" IS 'Numeric Primary Key for this table - fill using sequence: aasbs_transmit.AAC_ENVELOPE_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."aac_envelope"."activity_address_cd" IS 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE';
COMMENT ON COLUMN "aasbs_transmit"."aac_envelope"."transmittal_poc_id" IS 'Foreign Key to table AASBS_TRANSMIT.TRANSMITTAL_POC';
COMMENT ON COLUMN "aasbs_transmit"."aac_envelope"."transmittal_envelope_id" IS 'Foreign Key to table AASBS_TRANSMIT.TRANSMITTAL_ENVELOPE';
COMMENT ON COLUMN "aasbs_transmit"."aac_envelope"."transmittal_status_cd" IS 'Foreign Key to table AASBS_TRANSMIT.LU_TRANSMITTAL_STATUS';
COMMENT ON COLUMN "aasbs_transmit"."aac_envelope"."transmittal_type_cd" IS 'Foreign Key to table AASBS_TRANSMIT.LU_TRANSMITTAL_TYPE';
COMMENT ON COLUMN "aasbs_transmit"."aac_envelope"."transmittal_prefix" IS 'Transmittal Prefix';
COMMENT ON COLUMN "aasbs_transmit"."aac_envelope"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."aac_envelope"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."aac_envelope"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."aac_envelope"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."aac_envelope" IS 'Activity Address Code Envelope';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.accrual_expense_summary
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."accrual_expense_summary" (
    "id" bigint NOT NULL,
    "transmittal_id" bigint,
    "transmittal_status_cd" character varying(20) NOT NULL,
    "accrual_expense_id" bigint NOT NULL,
    "activity_address_cd" character varying(6),
    "ui_description" character varying(400),
    "act_number" character varying(9) NOT NULL,
    "mdl_subtask" character varying(20) NOT NULL,
    "adjustment_credit_indicator" character varying(1),
    "organization_code" character varying(8),
    "amount" numeric(15,2) NOT NULL,
    "service_my" character varying(4) NOT NULL,
    "income_expense_indicator" character varying(1) NOT NULL,
    "task_order" character varying(8) NOT NULL,
    "contract_number" character varying(8),
    "function_code" character varying(5) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_accrual_expense_summary" PRIMARY KEY (id),
    CONSTRAINT "fk_acrl_expsu_aac" FOREIGN KEY (activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd),
    CONSTRAINT "fk_acrl_expsu_acrl_exp_id" FOREIGN KEY (accrual_expense_id) REFERENCES aasbs.accrual_expense(id),
    CONSTRAINT "fk_acrl_expsu_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acrl_expsu_trans_stat_cd" FOREIGN KEY (transmittal_status_cd) REFERENCES aasbs_transmit.lu_transmittal_status(cd),
    CONSTRAINT "fk_acrl_expsu_transmit_id" FOREIGN KEY (transmittal_id) REFERENCES aasbs_transmit.transmittal(id),
    CONSTRAINT "fk_acrl_expsu_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_acrl_expsu_aac ON aasbs_transmit.accrual_expense_summary USING btree (activity_address_cd);
CREATE INDEX ix_acrl_expsu_acrl_exp_id ON aasbs_transmit.accrual_expense_summary USING btree (accrual_expense_id);
CREATE INDEX ix_acrl_expsu_cbui ON aasbs_transmit.accrual_expense_summary USING btree (created_by_user_id);
CREATE INDEX ix_acrl_expsu_trans_stat_cd ON aasbs_transmit.accrual_expense_summary USING btree (transmittal_status_cd);
CREATE INDEX ix_acrl_expsu_transmit_id ON aasbs_transmit.accrual_expense_summary USING btree (transmittal_id);
CREATE INDEX ix_acrl_expsu_ubui ON aasbs_transmit.accrual_expense_summary USING btree (updated_by_user_id);

CREATE TRIGGER accrual_expense_summary_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.accrual_expense_summary FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_accrual_expense_summary_hst();

COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS_TRANSMIT.ACCRUAL_EXPENSE_SUMMARY_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."transmittal_id" IS 'Foreign key to AASBS_TRANSMIT.TRANSMITTAL.ID.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."transmittal_status_cd" IS 'Foreign key to AASBS_TRANSMIT.LU_TRANSMITTAL_STATUS';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."accrual_expense_id" IS 'Foreign Key to table AASBS.ACCRUAL_EXPENSE';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."activity_address_cd" IS 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."ui_description" IS 'User Entered Description';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."act_number" IS 'Award FIN';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."mdl_subtask" IS 'Multiple Distribution Line/Subtask';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."adjustment_credit_indicator" IS 'Adjustment/Credit Indicator.  Blank for plus or "-" for minus.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."organization_code" IS 'Organization Code';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."amount" IS 'Expense Accrual Amount.  Must be greater than 0.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."service_my" IS 'Represents calendar month and year service was provided (Format: MMYY)';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."income_expense_indicator" IS 'Income/Expense Indicator.  Valid entries are "I" for Income, "E" for Expense.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."task_order" IS 'Task Order';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."contract_number" IS 'Contract Number';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."function_code" IS 'Function Code';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."accrual_expense_summary"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."accrual_expense_summary" IS 'Expense Accruals to send to VITAP';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.accrual_income_acct_line
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."accrual_income_acct_line" (
    "id" bigint NOT NULL,
    "accrual_income_header_id" bigint,
    "batch_type_indicator" character varying(2) NOT NULL,
    "information_indicator" character varying(1) NOT NULL,
    "accomplished_dt" timestamp(6) without time zone NOT NULL,
    "activity_code" character varying(20) NOT NULL,
    "agreement_line_num" bigint NOT NULL,
    "agreement_num" character varying(20) NOT NULL,
    "bbfy" integer NOT NULL,
    "commodity_inc_dec_indicator" character varying(1) NOT NULL,
    "contract_blanket_agreement" character varying(1) NOT NULL,
    "tsym_customer_sublevel_prefix" character varying(2),
    "division_region" character varying(20) NOT NULL,
    "description" character varying(255),
    "fund" character varying(20) NOT NULL,
    "increase_decrease_indicator" character varying(1) NOT NULL,
    "line_num" bigint NOT NULL,
    "organization_code" character varying(20) NOT NULL,
    "program_code" character varying(20) NOT NULL,
    "posting_event" character varying(15) NOT NULL,
    "revenue_source" character varying(20) NOT NULL,
    "transaction_currency" character varying(3) NOT NULL,
    "transaction_currency_amt" numeric(28,2) NOT NULL,
    "transaction_exchange_rate" numeric(25,0) NOT NULL,
    "transaction_type" character varying(4) NOT NULL,
    "vendor_address_code" character varying(13) NOT NULL,
    "vendor_code" character varying(10) NOT NULL,
    "accounting_line_type" character varying(5) NOT NULL,
    "tsym_agency_identifier" character varying(3),
    "tsym_allocation_trans_agency" character varying(3),
    "tsym_availability_type" character varying(1),
    "tsym_begin_period" character varying(4),
    "tsym_end_period" character varying(4),
    "tsym_main_account" character varying(4),
    "tsym_sub_account" character varying(3),
    "cust_tsym_agy_identifier" character varying(3),
    "cust_tsym_alloc_trans_agy" character varying(3),
    "cust_tsym_avail_type" character varying(1),
    "cust_tsym_begin_period" character varying(4),
    "cust_tsym_end_period" character varying(4),
    "cust_tsym_main_acct" character varying(4),
    "cust_tsym_sub_acct" character varying(3),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_accrual_income_acct_line" PRIMARY KEY (id),
    CONSTRAINT "fk_acrl_inc_acct_ln_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acrl_inc_acct_ln_header_id" FOREIGN KEY (accrual_income_header_id) REFERENCES aasbs_transmit.accrual_income_header(id),
    CONSTRAINT "fk_acrl_inc_acct_ln_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_acrl_inc_acct_ln_cbui ON aasbs_transmit.accrual_income_acct_line USING btree (created_by_user_id);
CREATE INDEX ix_acrl_inc_acct_ln_header_id ON aasbs_transmit.accrual_income_acct_line USING btree (accrual_income_header_id);
CREATE INDEX ix_acrl_inc_acct_ln_ubui ON aasbs_transmit.accrual_income_acct_line USING btree (updated_by_user_id);

CREATE TRIGGER accrual_income_acct_line_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.accrual_income_acct_line FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_accrual_income_acct_line_hst();

COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS_TRANSMIT.ACCRUAL_INCOME_ACCT_LINE_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."accrual_income_header_id" IS 'Foreign Key to table AASBS_TRANSMIT.ACCRUAL_INCOME_HEADER';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."batch_type_indicator" IS 'Identifies the Batch Type indicator for AAS Revenue Accruals';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."information_indicator" IS 'Information Indicator.  Identifies the line as a Detail.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."accomplished_dt" IS 'ACMP_DT_CH - Accomplished Date.  Format: MM/DD/YYYY';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."activity_code" IS 'ACTY - Activity Code';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."agreement_line_num" IS 'AGRE_LNUM_CH - Agreement Line Number';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."agreement_num" IS 'AGRE_NUM - Agreement Number';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."bbfy" IS 'BBFY - Beginning Budget Fiscal Year.  Fiscal Year from the Accounting Period (ACPD from Accrual Header)  Format: YYYY';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."commodity_inc_dec_indicator" IS 'CMDT_INCR_DCRS_IN - Commodity Increase/Decrease Indicator.  If the accrual amount is positive or zero, set to "I". If negative, set to "D".';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."contract_blanket_agreement" IS 'CTRC_FL - Contract or Blanket Agreement';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."tsym_customer_sublevel_prefix" IS 'CUST_SLVL_PRFX_CH - Customer Sublevel Prefix.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."division_region" IS 'DIV - Region/Division.  Valid values are 00 through 11.  Positions 2 and 3 of the organization code associated with the agreement';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."description" IS 'DSCR - Description';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."fund" IS 'FUND - Fund';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."increase_decrease_indicator" IS 'INCR_DCRS_IN - Increase/Decrease Indicator.  If the accrual amount is positive or zero, set to "I". If negative, set to "D".';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."line_num" IS 'LNUM_CH - Line Number.  A sequential number beginning with 1. Must be unique to the document.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."organization_code" IS 'ORGN - Organization Code.  Based on Agreement.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."program_code" IS 'PROG - Program Code.  Based on Agreement.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."posting_event" IS 'PSTG_EVNT - Posting Event';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."revenue_source" IS 'REV_SRCE - Revenue Source';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."transaction_currency" IS 'TRAN_CRCY - Transaction Currency';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."transaction_currency_amt" IS 'TRAN_CRCY_AM_CH - Transaction Currency Amount.  Unsigned accrual amount with up to two decimal places. A decimal point must be included if the amount is not a whole dollar. Example: $1,134.00 can be sent as 1134 or 1134.00';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."transaction_exchange_rate" IS 'TRAN_EXCH_RT_CH - Transaction to system currency exchange rate';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."transaction_type" IS 'TT - Transaction Type.  Indicates accounting events.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."vendor_address_code" IS 'VEND_ADDR_CD - Vendor Address Code.  BOAC2 from the Agreement.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."vendor_code" IS 'VEND_CD - Vendor Code.  BOAC2 from the Agreement.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."accounting_line_type" IS 'LINE_TYP_CH - Accounting Line Type';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."tsym_agency_identifier" IS 'TRFR_AID - Transfer Treasury Symbol: Agency Identifier.  The three-digit agency identifier.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."tsym_allocation_trans_agency" IS 'TSYM Allocation Trasmit Agency';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."tsym_availability_type" IS 'TRFR_AVAL_TYP - Transfer Treasury Symbol: Availability Type.  For funding that is not fiscal year based, identifies no-year accounts "X", clearing/suspense accounts "F", and Treasurys central summary general ledger accounts "A".';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."tsym_begin_period" IS 'TRFR_EPOA - Transfer Treasury Symbol: Ending Period of Availability.  For fiscal year-based funding.  Last fiscal year of fund availability.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."tsym_end_period" IS 'TSYM End Period';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."tsym_main_account" IS 'TRFR_MAIN_ACCT - Transfer Treasury Symbol: Main Account.  Identifies the appropriation.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."tsym_sub_account" IS 'TRFR_SUB_ACCT - Transfer Treasury Symbol: Sub Account';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."cust_tsym_agy_identifier" IS 'XTRN_AID - Customer Treasury Symbol: Agency Identifier.  The three-digit agency identifier.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."cust_tsym_alloc_trans_agy" IS 'XTRN_ATA - Customer Treasury Symbol: Allocation Transfer Agency.  The three-digit Agency Identifier of the agency receiving funds through an allocation transfer.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."cust_tsym_avail_type" IS 'XTRN_AVAL_TYP - Customer Treasury Symbol: Availability Type.  For funding that is not fiscal year based, identifies no-year accounts "X", clearing/suspense accounts "F", and Treasurys central summary general ledger accounts "A".';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."cust_tsym_begin_period" IS 'XTRN_BPOA - Customer Treasury Symbol: Beginning Period of Availability.  For fiscal year-based funding. First fiscal year of fund availability.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."cust_tsym_end_period" IS 'XTRN_EPOA - Customer Treasury Symbol: Ending Period of Availability.  For fiscal year-based funding.  Last fiscal year of fund availability.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."cust_tsym_main_acct" IS 'XTRN_MAIN_ACCT - Customer Treasury Symbol: Main Account.  Identifies the appropriation.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."cust_tsym_sub_acct" IS 'XTRN_SUB_ACCT - Customer Treasury Symbol: Sub Account';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_acct_line"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."accrual_income_acct_line" IS 'Income Accrual Accounting Lines to send to BAAR';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.accrual_income_header
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."accrual_income_header" (
    "id" bigint NOT NULL,
    "transmittal_id" bigint,
    "transmittal_status_cd" character varying(20) NOT NULL,
    "accrual_income_dist_id" bigint,
    "activity_address_cd" character varying(6),
    "ui_description" character varying(400),
    "act_number" character varying(9),
    "batch_type_indicator" character varying(2) NOT NULL,
    "information_indicator" character varying(1) NOT NULL,
    "accounting_period" character varying(7) NOT NULL,
    "auto_reverse" character varying(1) NOT NULL,
    "correction_flag" character varying(8) NOT NULL,
    "document_num" character varying(30),
    "document_status" character varying(20) NOT NULL,
    "document_type" character varying(10) NOT NULL,
    "reset_document_date_flag" character varying(1) NOT NULL,
    "reversed_flag" character varying(1) NOT NULL,
    "reverse_after_periods" numeric NOT NULL,
    "security_organization" character varying(10) NOT NULL,
    "title" character varying(50),
    "external_system_doc_num" character varying(30) NOT NULL,
    "external_system_id" character varying(10) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "external_accrual_income_id" bigint,
    "accrual_income_dist_summary_id" bigint,
    CONSTRAINT "pk_accrual_income_header" PRIMARY KEY (id),
    CONSTRAINT "fk_acrl_inc_hrd_aac" FOREIGN KEY (activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd),
    CONSTRAINT "fk_acrl_inc_hrd_acrlincdist_id" FOREIGN KEY (accrual_income_dist_id) REFERENCES aasbs.accrual_income_dist(id),
    CONSTRAINT "fk_acrl_inc_hrd_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acrl_inc_hrd_ext_acrl_inc_id" FOREIGN KEY (external_accrual_income_id) REFERENCES accrual.accrual_income(accrual_income_id),
    CONSTRAINT "fk_acrl_inc_hrd_stat_cd" FOREIGN KEY (transmittal_status_cd) REFERENCES aasbs_transmit.lu_transmittal_status(cd),
    CONSTRAINT "fk_acrl_inc_hrd_transmit_id" FOREIGN KEY (transmittal_id) REFERENCES aasbs_transmit.transmittal(id),
    CONSTRAINT "fk_acrl_inc_hrd_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_acrl_incds_aids_id" FOREIGN KEY (accrual_income_dist_summary_id) REFERENCES aasbs.accrual_income_dist_summary(id)
);

CREATE INDEX ix_acrl_inc_hrd_aac ON aasbs_transmit.accrual_income_header USING btree (activity_address_cd);
CREATE INDEX ix_acrl_inc_hrd_acrlincdist_id ON aasbs_transmit.accrual_income_header USING btree (accrual_income_dist_id);
CREATE INDEX ix_acrl_inc_hrd_act_num ON aasbs_transmit.accrual_income_header USING btree (act_number);
CREATE INDEX ix_acrl_inc_hrd_cbui ON aasbs_transmit.accrual_income_header USING btree (created_by_user_id);
CREATE INDEX ix_acrl_inc_hrd_extincid ON aasbs_transmit.accrual_income_header USING btree (external_accrual_income_id);
CREATE INDEX ix_acrl_inc_hrd_trans_stat_cd ON aasbs_transmit.accrual_income_header USING btree (transmittal_status_cd);
CREATE INDEX ix_acrl_inc_hrd_transmit_id ON aasbs_transmit.accrual_income_header USING btree (transmittal_id);
CREATE INDEX ix_acrl_inc_hrd_ubui ON aasbs_transmit.accrual_income_header USING btree (updated_by_user_id);
CREATE INDEX ix_acrl_incds_aids_id ON aasbs_transmit.accrual_income_header USING btree (accrual_income_dist_summary_id);

CREATE TRIGGER accrual_income_header_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.accrual_income_header FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_accrual_income_header_hst();

COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS_TRANSMIT.ACCRUAL_INCOME_HEADER_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."transmittal_id" IS 'Foreign key to AASBS_TRANSMIT.TRANSMITTAL.ID.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."transmittal_status_cd" IS 'Foreign key to AASBS_TRANSMIT.LU_TRANSMITTAL_STATUS';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."accrual_income_dist_id" IS 'Foreign key to AASBS.ACCRUAL_INCOME_DIST';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."activity_address_cd" IS 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."ui_description" IS 'User Entered Description';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."act_number" IS 'The order ACT number.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."batch_type_indicator" IS 'Identifies the Batch Type indicator for AAS Revenue Accruals';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."information_indicator" IS 'Information Indicator.  Identifies the line as a Header.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."accounting_period" IS 'ACPD - Accounting Period.  Fiscal month and Fiscal year to which the accrual applies.  Format: MM/YYYY';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."auto_reverse" IS 'AUTM_RVER - Indicates that the voucher should be automatically reversed';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."correction_flag" IS 'CORR_FL - Indicates that the form represents a correction to an existing document. (T = correction, X = cancellation, F = new form)';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."document_num" IS 'DOC_NUM_CH - Document Number, which is unique in Pegasys.  DTYP value + YYYYMMDD document date + ##### sequential number within document type and date';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."document_status" IS 'DOC_STUS - Document Status';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."document_type" IS 'DTYP - Document Type.  Based on Program Code.';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."reset_document_date_flag" IS 'RSET_DOC_DT_FL - Indicates that original document date should be reset to the date on this form';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."reversed_flag" IS 'RVRD_FL - Indicates whether the voucher has been reversed';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."reverse_after_periods" IS 'RVRE_AFTR_PRDS - Reverse after this number of accounting periods';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."security_organization" IS 'SCTY_ORGN - Security Organization';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."title" IS 'TITL - Title of the transaction.  ASSIST will populate with IA PIID';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."external_system_doc_num" IS 'XSYS_DOC_NUM_CH - External System Document Number';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."external_system_id" IS 'XSYS_ID - ID of the external system in which the transaction originated';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."external_accrual_income_id" IS 'FK to ACCRUAL.ACCRUAL_INCOME.ACCRUAL_INCOME_ID';
COMMENT ON COLUMN "aasbs_transmit"."accrual_income_header"."accrual_income_dist_summary_id" IS 'FK to AASBS.ACCRUAL_INCOME_DIST_SUMMARY.ID';

COMMENT ON TABLE "aasbs_transmit"."accrual_income_header" IS 'Income Accrual Header to send to BAAR';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.billing_summary
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."billing_summary" (
    "id" bigint NOT NULL,
    "transmittal_id" bigint,
    "bill_item_id" bigint NOT NULL,
    "transmittal_status_cd" character varying(20) NOT NULL,
    "acct_classification_code" character varying(8),
    "acct_classification_ref_num" character varying(12),
    "activity_address_cd" character varying(6),
    "activity_code" character varying(5),
    "additional_text" character varying(50),
    "advance_indicator" character varying(1),
    "agency_code" character varying(3),
    "agreement_line_num" character varying(5),
    "agreement_num" character varying(20),
    "bbfy" character varying(4),
    "billing_amount" numeric(28,2),
    "billing_record_id" character varying(18),
    "budget_fy" integer,
    "generated_dt" timestamp(6) without time zone,
    "credit_indicator" character varying(1),
    "cust_sublevel_prefix" character varying(2),
    "customer_fund" character varying(4),
    "description" character varying(130),
    "description_of_services" character varying(40),
    "designated_agent_code" character varying(6),
    "duns" character varying(9),
    "duns_plus_four" character varying(13),
    "external_system_doc_num" character varying(8),
    "fiscal_station_num" integer,
    "fund" character varying(4),
    "funding_doc_num" character varying(20),
    "group_number" character varying(10),
    "interfund_indicator" character varying(1),
    "org_code" character varying(8),
    "pop_end" date,
    "pop_start" date,
    "program_code" character varying(4),
    "quantity" integer,
    "reference_doc_line_num" character varying(2),
    "reference_doc_num" character varying(20),
    "reference_doc_type" character varying(2),
    "region_division" character varying(3),
    "related_statement_num" character varying(10),
    "revenue_source" character varying(4),
    "statement_num" character varying(8),
    "system_type" character varying(10),
    "title" character varying(50),
    "tysm_agency_identifier" character varying(3),
    "tysm_allocation_trans_agency" character varying(3),
    "tysm_avalability_type" character varying(1),
    "tysm_begin_period" character varying(4),
    "tysm_end_period" character varying(4),
    "tysm_main_acct" character varying(4),
    "tysm_sub_acct" character varying(3),
    "unit_of_issue" character varying(2),
    "vendor_customer_code" character varying(6),
    "version_num" character varying(3),
    "billing_id" bigint,
    "loa_subtask" character varying(4),
    "ui_description" character varying(300),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "uei" character varying(16),
    "uei_plus4" character varying(8),
    "ginv_ref_performance_num" character varying(20),
    "ginv_ref_performance_num_detail" character varying(6),
    "vuei" character varying(16) GENERATED ALWAYS AS (
CASE
    WHEN (uei IS NULL) THEN duns
    ELSE uei
END) STORED,
    "vuei_plus4" character varying(13) GENERATED ALWAYS AS (
CASE
    WHEN (uei_plus4 IS NULL) THEN duns_plus_four
    ELSE uei_plus4
END) STORED,
    CONSTRAINT "pk_billing_summary" PRIMARY KEY (id),
    CONSTRAINT "fk_billing_sumry_aac" FOREIGN KEY (activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd),
    CONSTRAINT "fk_billing_sumry_agreement_num" FOREIGN KEY (agreement_num) REFERENCES agreement.agreement(agreement_num),
    CONSTRAINT "fk_billing_sumry_billing_id" FOREIGN KEY (billing_id) REFERENCES billing.billing(billing_id),
    CONSTRAINT "fk_billing_sumry_billitem_id" FOREIGN KEY (bill_item_id) REFERENCES aasbs.bill_item(id),
    CONSTRAINT "fk_billing_sumry_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_billing_sumry_transm_id" FOREIGN KEY (transmittal_id) REFERENCES aasbs_transmit.transmittal(id),
    CONSTRAINT "fk_billing_sumry_transm_stat" FOREIGN KEY (transmittal_status_cd) REFERENCES aasbs_transmit.lu_transmittal_status(cd),
    CONSTRAINT "fk_billing_sumry_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX indx_bill_item_id ON aasbs_transmit.billing_summary USING btree (bill_item_id);
CREATE INDEX indx_statement_number ON aasbs_transmit.billing_summary USING btree (statement_num);
CREATE INDEX ix_billing_sumry_aac ON aasbs_transmit.billing_summary USING btree (activity_address_cd);
CREATE INDEX ix_billing_sumry_agreement_num ON aasbs_transmit.billing_summary USING btree (agreement_num);
CREATE INDEX ix_billing_sumry_billing_id ON aasbs_transmit.billing_summary USING btree (billing_id);
CREATE INDEX ix_billing_sumry_cbui ON aasbs_transmit.billing_summary USING btree (created_by_user_id);
CREATE INDEX ix_billing_sumry_transm_id ON aasbs_transmit.billing_summary USING btree (transmittal_id);
CREATE INDEX ix_billing_sumry_transm_stat ON aasbs_transmit.billing_summary USING btree (transmittal_status_cd);
CREATE INDEX ix_billing_sumry_ubui ON aasbs_transmit.billing_summary USING btree (updated_by_user_id);

CREATE TRIGGER billing_summary_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.billing_summary FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_billing_summary_hst();

COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."id" IS 'Primary key. Set by sequence AASBS.PO_SUMMARY_SEQ.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."transmittal_id" IS 'Foreign key to AASBS.TRANSMITTAL.ID.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."bill_item_id" IS 'Foreign Key to table AASBS_TRANSMIT.BILL_ITEM';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."transmittal_status_cd" IS 'Foreign Key to LU_FLAT_FILE_TRANSMIT_STATUS.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."acct_classification_code" IS 'The code that represents the 6-digits provided by FTW accounting office. Previosly known as BOAC1_CODE.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."acct_classification_ref_num" IS 'Task Order/Cust. ID + Subtask. Subtask is not used for NBA, so this field will contain only Task Order/Cust. ID for NBA';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."activity_address_cd" IS 'Foreign Key to table AASBS_TRANSMIT.LU_ACTIVITY_ADDRESS';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."activity_code" IS 'OneFund ACTIVITY for this region. RBA-Purchase Order Function Code: F11->AF127 F31->AF123 F51->AF151 F81->AF121 F99->AF120';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."additional_text" IS 'ASSIST2: Foreign key to AASBS.LOA_LEDGER.ID. NBA: Foreign key to OMIS.BID_BAAR.BID_BAAR_ID ~ .BID_ID ~ .AGREEMENT_NUM. For RBA, the FRONT_END_ORDER_ID.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."advance_indicator" IS 'T for advance (a bill PDF will be created), O for advance offset (reate a null-posting bill that provides the amount that the Finance Center should use to offset the advance. No bill PDF will be created.), F (regular bill) otherwise';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."agency_code" IS 'The requesting agency.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."agreement_line_num" IS 'All line numbers are currently 1';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."agreement_num" IS 'The agreement number on an LOA. Foreign key to AGREEMENT.AGREEMENT.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."bbfy" IS 'The beginning budget fiscal year.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."billing_amount" IS 'The billing amount.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."billing_record_id" IS 'RNB + MMDDYYYY#######, where MMDDYYYY is the date, and ####### is a sequence number within the date.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."budget_fy" IS 'Budget fiscal year';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."generated_dt" IS 'Date Billing Summary was generated';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."credit_indicator" IS 'Required for credits: 'C' for credits, null otherwise.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."cust_sublevel_prefix" IS 'TYSM/Custome Sublevel Prefix.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."customer_fund" IS 'Customer Fund assigned from the funding doc, and used to determine Interfund Indicator and Activity Code. Different from Fund.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."description" IS 'Currently being set to the LOA.LINE_OF_ACCOUNTING: formerly the OMIS.MIPRS.ACCT_DATA or TABLE_MASTER.FUNDING_CITATION.FUND_CITE_CODE.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."description_of_services" IS 'Task order or charge description.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."designated_agent_code" IS 'LOA BOAC1: formerly found on the RBA citation or the NBA funding.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."duns" IS 'Intended to hold GSA Duns, which we do not capture, so we stopped populating this around May 2018. Formerly, set to DUNS number.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."duns_plus_four" IS 'Intended to hold GSA Duns, which we do not capture, so we stopped populating this around May 2018. Formerly, set to DUNS Plus Four number.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."external_system_doc_num" IS 'Act number without the leading 'A'. NBA-TASK ORDER or RBA-CUSTOMER ID. Required. The XSYS_DOC_NUM field is mapped to the ACT_NO on the FMIS VAT.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."fiscal_station_num" IS 'All records are currently being set to '0'.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."fund" IS 'All records are currently being set to 285F, NBA pre-BAAR was set to 295X or 299X.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."funding_doc_num" IS 'The funding num.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."group_number" IS 'May be populated for situations where multiple sequential records should be loaded as one unit of work (all are loaded or none are loaded). This field must be left blank unless there are 2 or more sequential records in the file with the same value. Note, at present, June 2019, always null.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."interfund_indicator" IS 'If Transaction Type is N, set to F. If Transaction Type is Y, set to A for intrafund transactions or T for interfund transactions';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."org_code" IS 'New Organization Code, which is an 8-character code where positions two and three are equal to the two-digit Region Code. AKA, onefund organization.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."pop_end" IS 'Period of performance end date. Set to the last day of the month corresponding to the Service Month/Year.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."pop_start" IS 'Period of performance start date. Set to the first day of the month corresponding to the Service Month/Year';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."program_code" IS 'Budget Act Code or Onefund Program: B3->IT23 F1->AA20 F2->AA10 FL->IT31 FQ->IT14 FR->GS14 P1->AA20 P2->AA10.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."quantity" IS 'Task order quantity.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."reference_doc_line_num" IS 'Formerly known as the MDL or CUST_MDL.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."reference_doc_num" IS 'Formerly know as the CUST_ACT or CUST_ACT_NUM.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."reference_doc_type" IS 'For internal clients, set to 'IX', otherwise null.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."region_division" IS 'Region or organization_id.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."related_statement_num" IS 'Identifies the related Pegasys statement number for credits and rebills. At present, June 2019, always null.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."revenue_source" IS 'NBA1 if the Program Code is AA10 or IT31, RBA1 if the Program Code is AA20, GS13, IT14, or IT23 (i.e. NBA1 for NBA, RBA1 for RBA).';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."statement_num" IS 'Alphanumeric unique identifier for grouping of bills. Set depending on the budget act code by a call to BILLING.GET_STATEMENT_NUM (p_budg_act_code VARCHAR2).';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."system_type" IS 'Set to either 'NBA' or 'RBA'.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."title" IS 'RBA Funding Document-IAA Number NBA-OMIS Funding Doc-IA.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."tysm_agency_identifier" IS 'Customer Treasury Symbol: Agency Identifier. Formerly known as the Customer Agency Id (Cust_AID) or Requesting_Agency_Id.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."tysm_allocation_trans_agency" IS 'Customer Treasury Symbol: Allocation Transfer Agency';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."tysm_avalability_type" IS 'Customer Treasury Symbol: Availability Type. Formerly known as the Cust_Avail_Type or Loa_Availability_Type.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."tysm_begin_period" IS 'Customer Treasury Symbol: Beginning Period of Availability. Formerly known as the Cust_BPOA or LOA_Period_First.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."tysm_end_period" IS 'Customer Treasury Symbol: Ending Period of Availability. Formerly known as the Cust_EPOA or LOA_Period_Last.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."tysm_main_acct" IS 'Customer Treasury Symbol: Main Account. Formerly know as the Cust_Main_Account or the Appropriation_LOA.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."tysm_sub_acct" IS 'This will be a new field on the Funding Citation. Customer Treasury Symbol: Sub Account. Formerly known as the Cust_Sub_Account or Treasury_Subaccount.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."unit_of_issue" IS 'Default to EA for Each. Presently, June 2019, EA is the only distinct value stored in the db for this column.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."vendor_customer_code" IS 'Formerly known as BOAC2.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."version_num" IS 'The version used to generate the flat file.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."billing_id" IS 'Foreign key to BILLING.BILLING.BILLING_ID.';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."loa_subtask" IS 'Unique Identifier for a combination of Line Item and an LOA that funds it';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."ui_description" IS 'Detailed, Formatted description that displays in the Transmit module UI';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."uei" IS 'Unique Entity Identifier (UEI) - replaces DUNS';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."uei_plus4" IS 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."ginv_ref_performance_num" IS 'G-Invoicing Ref Performance Number (ginv_ref_performance_num)';
COMMENT ON COLUMN "aasbs_transmit"."billing_summary"."ginv_ref_performance_num_detail" IS 'G-Invoicing Ref Performance Number Detail (ginv_ref_performance_num_detail)';

COMMENT ON TABLE "aasbs_transmit"."billing_summary" IS 'Billing Summary of External Billing transmissions';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.inv_detail
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."inv_detail" (
    "id" bigint NOT NULL,
    "inv_summary_id" bigint NOT NULL,
    "act_number" character varying(9) NOT NULL,
    "delivery_dt" timestamp(6) without time zone,
    "description" character varying(48),
    "invoice_number" character varying(30) NOT NULL,
    "item_num" character varying(8),
    "quantity" bigint NOT NULL,
    "service_period_beginning" timestamp(6) without time zone,
    "service_period_ending" timestamp(6) without time zone,
    "shipment_dt" timestamp(6) without time zone,
    "total_detail_amount" numeric(15,2) NOT NULL,
    "unit_of_issue" character varying(2) NOT NULL,
    "unit_price" numeric(14,2) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_inv_detail" PRIMARY KEY (id),
    CONSTRAINT "fk_inv_detail_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_inv_detail_inv_summary_id" FOREIGN KEY (inv_summary_id) REFERENCES aasbs_transmit.inv_summary(id),
    CONSTRAINT "fk_inv_detail_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_inv_detail_cbui ON aasbs_transmit.inv_detail USING btree (created_by_user_id);
CREATE INDEX ix_inv_detail_inv_summary_id ON aasbs_transmit.inv_detail USING btree (inv_summary_id);
CREATE INDEX ix_inv_detail_ubui ON aasbs_transmit.inv_detail USING btree (updated_by_user_id);

CREATE TRIGGER inv_detail_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.inv_detail FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_inv_detail_hst();

COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS_TRANSMIT.INV_DETAIL_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."inv_summary_id" IS 'Foreign Key to table AASBS_TRANSMIT.INV_SUMMARY';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."act_number" IS 'ACT Number from the award document purchase order form';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."delivery_dt" IS 'Task item delivery date.';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."description" IS 'Populated with INVOICE_ITEM.DESCRIPTION';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."invoice_number" IS 'Populated with INVOICE.INVOICE_NUM';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."item_num" IS 'Populated with INVOICE_ITEM.ITEM_NUM';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."quantity" IS 'Populated with INVOICE_ITEM.QUANTITY';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."service_period_beginning" IS 'Populated with INVOICE_ITEM.SERVICE_PERIOD_BEGINNING';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."service_period_ending" IS 'INVOICE_ITEM.SERVICE_PERIOD_ENDING';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."shipment_dt" IS 'Populated with INVOICE_ITEM.SHIPMENT_DATE';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."total_detail_amount" IS 'Sum of all task items invoice amount.';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."unit_of_issue" IS 'Populated with INVOICE_ITEM.UNIT_OF_ISSUE';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."unit_price" IS 'Populated with INVOICE_ITEM.UNIT_PRICE';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."inv_detail"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."inv_detail" IS 'Billing Invoice Details';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.inv_summary
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."inv_summary" (
    "id" bigint NOT NULL,
    "transmittal_id" bigint,
    "transmittal_status_cd" character varying(20) NOT NULL,
    "invoice_id" bigint,
    "aba_routing_num" character varying(9),
    "account_num" character varying(25),
    "account_type" character varying(1),
    "act_number" character varying(9) NOT NULL,
    "company_name" character varying(30) NOT NULL,
    "companys_phone_num" character varying(30) NOT NULL,
    "contact_name" character varying(35),
    "contract_num" character varying(30),
    "discount_days_due" integer,
    "discount_percent" numeric(6,3),
    "invoice_dt" timestamp(6) without time zone NOT NULL,
    "invoice_generation_dt" timestamp(6) without time zone,
    "invoice_receipt_dt" timestamp(6) without time zone,
    "invoice_transmitted_dt" timestamp(6) without time zone,
    "invoice_number" character varying(30) NOT NULL,
    "misc_charges_and_credits" numeric(14,2),
    "net_days" integer,
    "number_of_detail_lines" bigint NOT NULL,
    "pegasys_doc_num" character varying(20),
    "purchase_order_number" character varying(22),
    "rem_vendor_address1" character varying(55),
    "rem_vendor_address2" character varying(40) NOT NULL,
    "rem_vendor_address3" character varying(40),
    "rem_vendor_city" character varying(30) NOT NULL,
    "rem_vendor_name" character varying(60),
    "rem_vendor_state" character varying(2) NOT NULL,
    "rem_vendor_zip_code" character varying(9) NOT NULL,
    "shipping_amount" numeric(14,2),
    "system" character varying(256) NOT NULL,
    "tax_amount" numeric(14,2),
    "tax_id_num" character varying(9),
    "tax_id_qualifier" character varying(1),
    "total_invoice_amount" numeric(14,2) NOT NULL,
    "activity_address_cd" character varying(6) NOT NULL,
    "specification_version" character varying(3),
    "description" character varying(250),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_inv_summary" PRIMARY KEY (id),
    CONSTRAINT "nn_inv_sumry_invoice_recpt_dt" CHECK (invoice_receipt_dt IS NOT NULL),
    CONSTRAINT "fk_inv_sumry_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_inv_sumry_invoice_id" FOREIGN KEY (invoice_id) REFERENCES aasbs.invoice(id),
    CONSTRAINT "fk_inv_sumry_trans_status_cd" FOREIGN KEY (transmittal_status_cd) REFERENCES aasbs_transmit.lu_transmittal_status(cd),
    CONSTRAINT "fk_inv_sumry_transmittal_id" FOREIGN KEY (transmittal_id) REFERENCES aasbs_transmit.transmittal(id),
    CONSTRAINT "fk_inv_sumry_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_inv_sumry_act_number ON aasbs_transmit.inv_summary USING btree (act_number);
CREATE INDEX ix_inv_sumry_cbui ON aasbs_transmit.inv_summary USING btree (created_by_user_id);
CREATE INDEX ix_inv_sumry_invoice_id ON aasbs_transmit.inv_summary USING btree (invoice_id);
CREATE INDEX ix_inv_sumry_trans_status_cd ON aasbs_transmit.inv_summary USING btree (transmittal_status_cd);
CREATE INDEX ix_inv_sumry_transmittal_id ON aasbs_transmit.inv_summary USING btree (transmittal_id);
CREATE INDEX ix_inv_sumry_ubui ON aasbs_transmit.inv_summary USING btree (updated_by_user_id);

CREATE TRIGGER inv_summary_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.inv_summary FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_inv_summary_hst();

COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."id" IS 'Primary Key. Set by sequence AASBS_TRANSMIT.INV_SUMMARY_SEQ.';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."transmittal_id" IS 'Foreign Key to AASBS_TRANSMIT.TRANSMITTAL.ID.';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."transmittal_status_cd" IS 'Foreign Key to LU_transmittal_STATUS.';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."invoice_id" IS 'Foreign Key to AASBS.INVOICE';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."aba_routing_num" IS 'ABA Routing Number';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."account_num" IS 'Account Number';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."account_type" IS 'Account Type';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."act_number" IS 'Populated with INVOICE.ACT_NUM';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."company_name" IS 'Company Name. Populated with INVOICE_DETAILS. CONTRACTING_ORG_ID or INVOICE_DETAILS. REMIT_COMPANY';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."companys_phone_num" IS 'Company Phone Number';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."contact_name" IS 'Contact Name';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."contract_num" IS 'Contract Number. Populated with INVOICE_DETAILS.CONTRACT_NUM';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."discount_days_due" IS 'Discount Days Due. Populated with INVOICE. DISCOUNT_DAYS_DUE';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."discount_percent" IS 'Discount Percentage. Populated with INVOICE. DISCOUNT_PERCENTAGE';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."invoice_dt" IS 'Invoice Date';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."invoice_generation_dt" IS 'Date the Invoice was generated';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."invoice_receipt_dt" IS 'Invoice Receipt Date. Populated with INVOICE.INVOICE_DATE';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."invoice_transmitted_dt" IS 'Date the Invoice was transmitted';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."invoice_number" IS 'Populated with INVOICE. INVOICE_NUM';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."misc_charges_and_credits" IS 'Misc. Charges and Credits';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."net_days" IS 'Net Days';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."number_of_detail_lines" IS 'Number of Detail Lines';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."pegasys_doc_num" IS 'Pegasys Document Number';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."purchase_order_number" IS 'Purchase Order Number / PIID';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."rem_vendor_address1" IS 'REMITTANCE VENDOR LINE 1. Per INV FF spec, optional second line for remittance vendor name (despite poor name)';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."rem_vendor_address2" IS 'Remittance Vendor Address2. Per INV FF spec, manditory address line. Populated with INVOICE_DETAILS.REMIT_STREET1';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."rem_vendor_address3" IS 'Remittance Vendor Address3. Per INV FF spec, optional address line. Populated with INVOICE_DETAILS.REMIT_STREET2';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."rem_vendor_city" IS 'Remittance Vendor City. Populated with INVOICE_DETAILS.REMIT_CITY';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."rem_vendor_name" IS 'Remittance Vendor Name.';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."rem_vendor_state" IS 'Remittance Vendor State. Populated with INVOICE_DETAILS.REMIT_STATE';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."rem_vendor_zip_code" IS 'Remittance Vendor Zip Code. Populated with INVOICE_DETAILS.REMIT_ZIP';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."shipping_amount" IS 'Shipping Amount';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."system" IS 'To Be determined';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."tax_amount" IS 'Tax Amount';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."tax_id_num" IS 'Tax Identification Number';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."tax_id_qualifier" IS 'Tax Identificaiton Qualifier';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."total_invoice_amount" IS 'Total Invoice Amount. Populated with Sum of all INVOICE_ITEM.INVOICE_ITEM_AMT';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."activity_address_cd" IS 'Region or National program creator. Foreign Key to table AASBS_TRANSMIT.LU_ACTIVITY_ADDRESS';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."specification_version" IS 'The version used to generate the Transmittal.';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."description" IS 'The order description.';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."inv_summary"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."inv_summary" IS 'Invoice Summary';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.lu_transmittal_record_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."lu_transmittal_record_type" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_transmittal_record_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_trns_rec_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_transmittal_record_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.lu_transmittal_record_type FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_lu_transmittal_record_type_hst();

COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_record_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_record_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_record_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_record_type"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_record_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_record_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_record_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_record_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."lu_transmittal_record_type" IS 'Lookup table - All Transmittal Record Types';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.lu_transmittal_stage
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."lu_transmittal_stage" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_transmittal_stage" PRIMARY KEY (cd),
    CONSTRAINT "ck_trans_stage_active" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_transmittal_stage_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.lu_transmittal_stage FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_lu_transmittal_stage_hst();

COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_stage"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_stage"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_stage"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_stage"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_stage"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_stage"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_stage"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."lu_transmittal_stage" IS 'Lookup Table - Stages of releasing a Transmision to Pegasys';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.lu_transmittal_status
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."lu_transmittal_status" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(200) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_transmit_status" PRIMARY KEY (cd),
    CONSTRAINT "ck_transmit_status_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE INDEX idx_lu_transmittal_status_description ON aasbs_transmit.lu_transmittal_status USING btree (description);

CREATE TRIGGER lu_transmittal_status_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.lu_transmittal_status FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_lu_transmittal_status_hst();

COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_status"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_status"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_status"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_status"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_status"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_status"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_status"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_status"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."lu_transmittal_status" IS 'Lookup table - All Transmission Statuses';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.lu_transmittal_type
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."lu_transmittal_type" (
    "cd" character varying(20) NOT NULL,
    "description" character varying(100) NOT NULL,
    "flatfile_cd" character varying(2) NOT NULL,
    "active_yn" character(1) DEFAULT 'Y'::character(1) NOT NULL,
    "sort_order" bigint DEFAULT (0)::bigint NOT NULL,
    "created_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "created_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    "updated_by_user_name" character varying(30) DEFAULT SESSION_USER NOT NULL,
    "updated_dt" timestamp(6) without time zone DEFAULT now() NOT NULL,
    CONSTRAINT "pk_lu_transmittal_type" PRIMARY KEY (cd),
    CONSTRAINT "ck_trns_type_actyn" CHECK (active_yn = ANY (ARRAY['Y'::bpchar, 'N'::bpchar]))
);

CREATE TRIGGER lu_transmittal_type_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.lu_transmittal_type FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_lu_transmittal_type_hst();

COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_type"."cd" IS 'Alphanumeric Primary Key for this table';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_type"."description" IS 'Description of the lookup value';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_type"."flatfile_cd" IS '2 char code sent as a transmittal type identifier to pegasys';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_type"."active_yn" IS 'Flag indicating whether a record is active (=Y) or not (=N)';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_type"."sort_order" IS 'Sort order used for UI Display';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_type"."created_by_user_name" IS 'Oracle Login Username who created this database table record';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_type"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_type"."updated_by_user_name" IS 'Oracle Login Username who most recently updated this database table record';
COMMENT ON COLUMN "aasbs_transmit"."lu_transmittal_type"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."lu_transmittal_type" IS 'Lookup table - All Transmittal Types';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.po_accounting
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."po_accounting" (
    "id" bigint NOT NULL,
    "po_summary_id" bigint NOT NULL,
    "appropriation_code" character varying(4) NOT NULL,
    "budget_activity_code" character varying(4) NOT NULL,
    "ss_cost_ctr_a_work_authoriz" character varying(7),
    "ss_cost_ctr_b_building_number" character varying(8),
    "cost_element" character varying(3) NOT NULL,
    "ss_craft_code_plant_number" character varying(2),
    "ss_federal_indicator" character varying(1),
    "function_code" character varying(5) NOT NULL,
    "mdl" character varying(2) NOT NULL,
    "mdl_amt" numeric(15,2) NOT NULL,
    "object_class" character varying(2) NOT NULL,
    "organization_code" character varying(8) NOT NULL,
    "ss_project_number" character varying(8),
    "ss_document_number" character varying(8),
    "ss_labor_hours" character varying(8),
    "ss_work_item_location_code" character varying(3),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_po_accounting" PRIMARY KEY (id),
    CONSTRAINT "fk_po_acctng_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_po_acctng_po_summary_id" FOREIGN KEY (po_summary_id) REFERENCES aasbs_transmit.po_summary(id),
    CONSTRAINT "fk_po_acctng_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_po_acctng_cbui ON aasbs_transmit.po_accounting USING btree (created_by_user_id);
CREATE INDEX ix_po_acctng_po_summary_id ON aasbs_transmit.po_accounting USING btree (po_summary_id);
CREATE INDEX ix_po_acctng_ubui ON aasbs_transmit.po_accounting USING btree (updated_by_user_id);

CREATE TRIGGER po_accounting_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.po_accounting FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_po_accounting_hst();

COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."id" IS 'Primary key. Set by sequence AASBS_TRANSMIT.PO_ACCOUNTING_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."po_summary_id" IS 'Foreign key to AASBS.PO_SUMMARY.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."appropriation_code" IS 'The order appropriation code.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."budget_activity_code" IS 'After OneFund activation: OneFund PROGRAM code. Before OneFund activation: BUDGET_ACT_CODE.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."ss_cost_ctr_a_work_authoriz" IS 'The cost center A.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."ss_cost_ctr_b_building_number" IS 'The cost center B.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."cost_element" IS 'The order cost element code.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."ss_craft_code_plant_number" IS 'The craft code.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."ss_federal_indicator" IS 'The federal indicator.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."function_code" IS 'After OneFund activation: OneFund ACTIVITY code. Before OneFund activation: FUNCTION_CODE.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."mdl" IS 'The multiple distribution lines for GSA clients.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."mdl_amt" IS 'The amount on the MDL.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."object_class" IS 'The order object class.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."organization_code" IS 'The order organization code.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."ss_project_number" IS 'The project prospectus number.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."ss_document_number" IS 'The SS document number.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."ss_labor_hours" IS 'The SS labor hours.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."ss_work_item_location_code" IS 'The work item.';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."po_accounting"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."po_accounting" IS 'Purchase Order Accounting';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.po_line
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."po_line" (
    "id" bigint NOT NULL,
    "po_summary_id" bigint NOT NULL,
    "description_stock_number" character varying(30),
    "quantity" bigint NOT NULL,
    "total_line_amt" numeric(15,2) NOT NULL,
    "unit_of_issue" character varying(2) NOT NULL,
    "unit_price" numeric(14,4) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_po_line_id" PRIMARY KEY (id),
    CONSTRAINT "fk_po_line_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_po_line_po_summary_id" FOREIGN KEY (po_summary_id) REFERENCES aasbs_transmit.po_summary(id),
    CONSTRAINT "fk_po_line_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_po_line_cbui ON aasbs_transmit.po_line USING btree (created_by_user_id);
CREATE INDEX ix_po_line_po_summary_id ON aasbs_transmit.po_line USING btree (po_summary_id);
CREATE INDEX ix_po_line_ubui ON aasbs_transmit.po_line USING btree (updated_by_user_id);

CREATE TRIGGER po_line_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.po_line FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_po_line_hst();

COMMENT ON COLUMN "aasbs_transmit"."po_line"."id" IS 'Primary key. Set by sequence AASBS_TRANSMIT.PO_LINE_SEQ.';
COMMENT ON COLUMN "aasbs_transmit"."po_line"."po_summary_id" IS 'Foreign key to AASBS_TRANSMIT.PO_SUMMARY.';
COMMENT ON COLUMN "aasbs_transmit"."po_line"."description_stock_number" IS 'Description Stock Number';
COMMENT ON COLUMN "aasbs_transmit"."po_line"."quantity" IS 'Quantity of items (units)';
COMMENT ON COLUMN "aasbs_transmit"."po_line"."total_line_amt" IS 'Total_Line_Amount = Quantity * Unit_Price';
COMMENT ON COLUMN "aasbs_transmit"."po_line"."unit_of_issue" IS 'Units of Measure for this ';
COMMENT ON COLUMN "aasbs_transmit"."po_line"."unit_price" IS 'Line Item Unit Price';
COMMENT ON COLUMN "aasbs_transmit"."po_line"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."po_line"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."po_line"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."po_line"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."po_line" IS 'Purchase Order Line Item';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.po_summary
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."po_summary" (
    "id" bigint NOT NULL,
    "transmittal_id" bigint,
    "transmittal_status_cd" character varying(20) NOT NULL,
    "activity_address_cd" character varying(6) NOT NULL,
    "acceptance_terms" numeric,
    "number_of_accounting_lines" bigint,
    "act_number" character varying(9) NOT NULL,
    "budget_fy" integer,
    "generated_dt" timestamp(6) without time zone,
    "billing_office_city" character varying(100) NOT NULL,
    "billing_office_name" character varying(100) NOT NULL,
    "billing_office_state" character varying(2) NOT NULL,
    "billing_office_street_1" character varying(100) NOT NULL,
    "billing_office_street_2" character varying(100),
    "billing_office_zip" character varying(16) NOT NULL,
    "contract_number" character varying(30),
    "contract_officer_name" character varying(75) NOT NULL,
    "contract_officer_phone" character varying(30) NOT NULL,
    "dba_indicator" character varying(1),
    "number_of_detail_lines" bigint,
    "discount_days" bigint,
    "discount_percent" numeric(6,3),
    "duns_number" character varying(13),
    "fob_shipment_method" character varying(2) NOT NULL,
    "ipartner_city" character varying(30) NOT NULL,
    "tax_id_number" bigint,
    "ipartner_name" character varying(120) NOT NULL,
    "ipartner_name_2" character varying(55),
    "ipartner_phone" character varying(30) NOT NULL,
    "ipartner_state" character varying(2) NOT NULL,
    "ipartner_street_1" character varying(75) NOT NULL,
    "ipartner_street_2" character varying(75),
    "ipartner_zip" character varying(10) NOT NULL,
    "mod_number" character varying(5) NOT NULL,
    "net_days" bigint NOT NULL,
    "nish_nib_ind" character varying(1),
    "order_dt" timestamp(6) without time zone NOT NULL,
    "purchase_order_number" character varying(22) NOT NULL,
    "po_amount" numeric(15,2) NOT NULL,
    "prepayment_authorized_flag" character varying(2),
    "recv_office_name" character varying(75) NOT NULL,
    "recv_office_phone" character varying(30) NOT NULL,
    "rem_vendor_city" character varying(30) NOT NULL,
    "rem_vendor_name" character varying(60) NOT NULL,
    "rem_vendor_name_2" character varying(55),
    "rem_vendor_state" character varying(2) NOT NULL,
    "rem_vendor_address_1" character varying(40),
    "rem_vendor_address_2" character varying(75) NOT NULL,
    "rem_vendor_address_3" character varying(75),
    "rem_vendor_zip_code" character varying(10) NOT NULL,
    "rem_vendor_number" bigint,
    "business_classification_code" character varying(1) NOT NULL,
    "tax_id_qualifier" character varying(1),
    "type_of_modification_code_ind" character varying(1),
    "version_num" character varying(3),
    "description" character varying(250),
    "award_mod_id" bigint,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    "uei" character varying(16),
    "vendor_code" character varying(16),
    "vendor_addr_code" character varying(16),
    "vuei" character varying(16) GENERATED ALWAYS AS (
CASE
    WHEN (uei IS NULL) THEN duns_number
    ELSE uei
END) STORED,
    CONSTRAINT "pk_po_summary" PRIMARY KEY (id),
    CONSTRAINT "fk_po_sumry_activity_addr_cd" FOREIGN KEY (activity_address_cd) REFERENCES aasbs.lu_activity_address_code(cd),
    CONSTRAINT "fk_po_sumry_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_po_sumry_trans_status_cd" FOREIGN KEY (transmittal_status_cd) REFERENCES aasbs_transmit.lu_transmittal_status(cd),
    CONSTRAINT "fk_po_sumry_transmittal_id" FOREIGN KEY (transmittal_id) REFERENCES aasbs_transmit.transmittal(id),
    CONSTRAINT "fk_po_sumry_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_po_sumry_act_number ON aasbs_transmit.po_summary USING btree (act_number);
CREATE INDEX ix_po_sumry_activity_addr_cd ON aasbs_transmit.po_summary USING btree (activity_address_cd);
CREATE INDEX ix_po_sumry_cbui ON aasbs_transmit.po_summary USING btree (created_by_user_id);
CREATE INDEX ix_po_sumry_trans_status_cd ON aasbs_transmit.po_summary USING btree (transmittal_status_cd);
CREATE INDEX ix_po_sumry_transmittal_id ON aasbs_transmit.po_summary USING btree (transmittal_id);
CREATE INDEX ix_po_sumry_ubui ON aasbs_transmit.po_summary USING btree (updated_by_user_id);

CREATE TRIGGER po_summary_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.po_summary FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_po_summary_hst();

COMMENT ON COLUMN "aasbs_transmit"."po_summary"."id" IS 'Numeric Primary Key for this table - fill using sequence: AASBS_TRANSMIT.PO_SUMMARY_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."transmittal_id" IS 'Foreign Key to AASBS_TRANSMIT.TRANSMITTAL';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."transmittal_status_cd" IS 'Foreign Key to AASBS_TRANSMIT.LU_transmittal_STATUS.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."activity_address_cd" IS 'Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS_CODE';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."acceptance_terms" IS 'The acceptance terms and conditions.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."number_of_accounting_lines" IS 'The number of accounting lines in NEAR_PO_ACCOUNTINGS.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."act_number" IS 'The order ACT number.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."budget_fy" IS 'Budget fiscal year';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."generated_dt" IS 'Date Purchase Order summary was generated';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."billing_office_city" IS 'The city line of address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."billing_office_name" IS 'The name of the billing office.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."billing_office_state" IS 'The state line of address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."billing_office_street_1" IS 'The first line in address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."billing_office_street_2" IS 'The second line in address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."billing_office_zip" IS 'The zipcode of address. The first 5 numbers are required.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."contract_number" IS 'The contract number.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."contract_officer_name" IS 'The name of the Contract Officer.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."contract_officer_phone" IS 'The phone number of Contract Officer.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."dba_indicator" IS 'The DBA indicator.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."number_of_detail_lines" IS 'The number of detail lines in NEAR_PO_DETAILS.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."discount_days" IS 'The days to receive discount.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."discount_percent" IS 'The discount percentage offered for prompt payment.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."duns_number" IS 'The industry partner DUNS number.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."fob_shipment_method" IS 'The shipment method.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."ipartner_city" IS 'The city line of address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."tax_id_number" IS 'The tax ID number.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."ipartner_name" IS 'The name of company or organization.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."ipartner_name_2" IS 'The overflow of company name.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."ipartner_phone" IS 'The phone number of company.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."ipartner_state" IS 'The state line of address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."ipartner_street_1" IS 'The first line in address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."ipartner_street_2" IS 'The second line in address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."ipartner_zip" IS 'The zipcode line of address. The first 5 numbers are required.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."mod_number" IS 'The modification number.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."net_days" IS 'The net days to pay invoice.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."nish_nib_ind" IS 'The NISH NIB indicator.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."order_dt" IS 'The calendar date that denotes the date of order.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."purchase_order_number" IS 'The order number/order PIID.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."po_amount" IS 'The total value of purchase order.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."prepayment_authorized_flag" IS 'The identifier that indicates whether prepayment is authorized.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."recv_office_name" IS 'The Client receiving office name.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."recv_office_phone" IS 'The Client receiving office phone number.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."rem_vendor_city" IS 'The city line of remittance address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."rem_vendor_name" IS 'The name of company or organization.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."rem_vendor_name_2" IS 'The overflow for vendor name.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."rem_vendor_state" IS 'The state line of remittance address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."rem_vendor_address_1" IS 'REMITTANCE VENDOR LINE 1. Per INV FF spec, optional second line for remittance vendor name';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."rem_vendor_address_2" IS 'The first line of remittance address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."rem_vendor_address_3" IS 'The second line of remittance address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."rem_vendor_zip_code" IS 'The zipcode line of remittance address.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."rem_vendor_number" IS 'Pegasys vendor number';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."business_classification_code" IS 'The small business classification.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."tax_id_qualifier" IS 'The tax ID qualifier.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."type_of_modification_code_ind" IS 'The type of modification.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."version_num" IS 'The version used to generate the Transmittal.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."description" IS 'The order description.';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."award_mod_id" IS 'Foreign Key to AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."updated_dt" IS 'Most recent date this database record was updated';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."uei" IS 'Unique Entity Identifier (UEI) - replaces DUNS';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."vendor_code" IS 'Vendor Code of the Contractor to which the PO is awarded';
COMMENT ON COLUMN "aasbs_transmit"."po_summary"."vendor_addr_code" IS 'Vendor Address Code of the Contractor to which the PO is awarded';

COMMENT ON TABLE "aasbs_transmit"."po_summary" IS 'Purchase Order Summary';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.rr_detail
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."rr_detail" (
    "id" bigint NOT NULL,
    "rr_summary_id" bigint NOT NULL,
    "deduction_amt" numeric(15,2) NOT NULL,
    "description_stock_num" character varying(30) NOT NULL,
    "mdl" character varying(2),
    "quantity" numeric(10,2) NOT NULL,
    "total_mdl_amount" numeric(15,2) NOT NULL,
    "unit_price" numeric(15,4) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_rr_detail" PRIMARY KEY (id),
    CONSTRAINT "fk_rr_detail_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_rr_detail_rr_summary_id" FOREIGN KEY (rr_summary_id) REFERENCES aasbs_transmit.rr_summary(id),
    CONSTRAINT "fk_rr_detail_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_rr_detail_cbui ON aasbs_transmit.rr_detail USING btree (created_by_user_id);
CREATE INDEX ix_rr_detail_rr_summary_id ON aasbs_transmit.rr_detail USING btree (rr_summary_id);
CREATE INDEX ix_rr_detail_ubui ON aasbs_transmit.rr_detail USING btree (updated_by_user_id);

CREATE TRIGGER rr_detail_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.rr_detail FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_rr_detail_hst();

COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."id" IS 'Primary key. Set by sequence AASBS_TRANSMIT.RR_DETAIL_SEQ.';
COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."rr_summary_id" IS 'Foreign key to AASBS_TRANSMIT.RR_SUMMARY.ID.';
COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."deduction_amt" IS 'The amount deducted from line total amount.';
COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."description_stock_num" IS 'The text description and ID number of item.';
COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."mdl" IS 'The GSA client MDL.';
COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."quantity" IS 'The quantity ordered.';
COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."total_mdl_amount" IS 'The sum of quantity * unit price.';
COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."unit_price" IS 'The unit price of the item.';
COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."rr_detail"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."rr_detail" IS 'Receiving Report details';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.rr_summary
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."rr_summary" (
    "id" bigint NOT NULL,
    "transmittal_id" bigint,
    "transmittal_status_cd" character varying(20) NOT NULL,
    "accepted_dt" timestamp(6) without time zone NOT NULL,
    "act_number" character varying(9) NOT NULL,
    "amendment_ind" character varying(1),
    "contract_number" character varying(30),
    "deduction_amt" numeric(15,2) NOT NULL,
    "description" character varying(50),
    "number_of_detail_lines" bigint,
    "invoice_dt" timestamp(6) without time zone,
    "pegasys_invoice_number" character varying(30) NOT NULL,
    "acceptance_id" bigint NOT NULL,
    "invoice_id" bigint NOT NULL,
    "max_approved_amt" numeric(15,2) NOT NULL,
    "max_payment_amt" numeric(15,2) NOT NULL,
    "purchase_order_number" character varying(22),
    "received_dt" timestamp(6) without time zone NOT NULL,
    "recv_office_symbol" character varying(8) NOT NULL,
    "recv_person_name" character varying(60),
    "recv_person_phone" character varying(30),
    "rem_vendor_name" character varying(35) NOT NULL,
    "rem_office_symbol_2" character varying(8),
    "second_recv_name" character varying(35),
    "second_recv_phone" character varying(13),
    "type_of_delivery" character varying(1) NOT NULL,
    "activity_address_cd" character varying(6) NOT NULL,
    "version_num" character varying(3),
    "generated_dt" timestamp(6) without time zone,
    "ui_description" character varying(250),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_rr_summary" PRIMARY KEY (id),
    CONSTRAINT "fk_rr_sumry_acceptance_id" FOREIGN KEY (acceptance_id) REFERENCES aasbs.acceptance(id),
    CONSTRAINT "fk_rr_sumry_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_rr_sumry_invoice_id" FOREIGN KEY (invoice_id) REFERENCES aasbs.invoice(id),
    CONSTRAINT "fk_rr_sumry_trans_status_cd" FOREIGN KEY (transmittal_status_cd) REFERENCES aasbs_transmit.lu_transmittal_status(cd),
    CONSTRAINT "fk_rr_sumry_transmittal_id" FOREIGN KEY (transmittal_id) REFERENCES aasbs_transmit.transmittal(id),
    CONSTRAINT "fk_rr_sumry_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX fk_rr_sumry_cbui ON aasbs_transmit.rr_summary USING btree (created_by_user_id);
CREATE INDEX fk_rr_sumry_ubui ON aasbs_transmit.rr_summary USING btree (updated_by_user_id);
CREATE INDEX idx_rr_summary_activity_address_cd ON aasbs_transmit.rr_summary USING btree (activity_address_cd);
CREATE INDEX idx_rr_summary_contract_number ON aasbs_transmit.rr_summary USING btree (contract_number);
CREATE INDEX idx_rr_summary_generated_dt ON aasbs_transmit.rr_summary USING btree (generated_dt);
CREATE INDEX idx_rr_summary_generated_dt_status ON aasbs_transmit.rr_summary USING btree (generated_dt, transmittal_status_cd);
CREATE INDEX idx_rr_summary_purchase_order_number ON aasbs_transmit.rr_summary USING btree (purchase_order_number);
CREATE INDEX ix_rr_sumry_acceptance_id ON aasbs_transmit.rr_summary USING btree (acceptance_id);
CREATE INDEX ix_rr_sumry_act_number ON aasbs_transmit.rr_summary USING btree (act_number);
CREATE INDEX ix_rr_sumry_invoice_id ON aasbs_transmit.rr_summary USING btree (invoice_id);
CREATE INDEX ix_rr_sumry_peg_inv_num ON aasbs_transmit.rr_summary USING btree (pegasys_invoice_number);
CREATE INDEX ix_rr_sumry_trans_status_cd ON aasbs_transmit.rr_summary USING btree (transmittal_status_cd);
CREATE INDEX ix_rr_sumry_transmittal_id ON aasbs_transmit.rr_summary USING btree (transmittal_id);

CREATE TRIGGER rr_summary_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.rr_summary FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_rr_summary_hst();

COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."id" IS 'Primary key. Set by sequence AASBS_TRANSMIT.RR_SUMMARY_SEQ.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."transmittal_id" IS 'Foreign key to AASBS_TRANSMIT.TRANSMITTAL.ID.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."transmittal_status_cd" IS 'Foreign Key to AASBS_TRANSMIT.LU_TRANSMITTAL_STATUS.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."accepted_dt" IS 'The date the items were accepted.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."act_number" IS 'The order ACT number.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."amendment_ind" IS 'The identifier that indicates if amendment.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."contract_number" IS 'The order contract number.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."deduction_amt" IS 'The deductions for non-performance/non-receipt.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."description" IS 'The order description.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."number_of_detail_lines" IS 'The number of detail lines.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."invoice_dt" IS 'The date on invoice.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."pegasys_invoice_number" IS 'The pegasys version of the invoice number for receiving report.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."acceptance_id" IS 'Foreign Key to table AASBS.ACCEPTANCE';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."invoice_id" IS 'Foreign Key to table AASBS.INVOICE';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."max_approved_amt" IS 'The maximum amount approved for payment.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."max_payment_amt" IS 'The maximum payment amount.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."purchase_order_number" IS 'The Purchase Order number/piid.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."received_dt" IS 'The date the items were received.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."recv_office_symbol" IS 'The office symbol of receiving person.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."recv_person_name" IS 'The name of person signing receiving report.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."recv_person_phone" IS 'The phone number of receiving person.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."rem_vendor_name" IS 'The remittance vendor''s name.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."rem_office_symbol_2" IS 'The office symbol of second receiver.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."second_recv_name" IS 'The name of the second certifier, if required.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."second_recv_phone" IS 'The phone number of second receiver.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."type_of_delivery" IS 'The type of delivery.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."activity_address_cd" IS 'Region or National program creator. Foreign Key to table AASBS.LU_ACTIVITY_ADDRESS';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."version_num" IS 'The version used to generate the flat file.';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."generated_dt" IS 'Date the Summary Report was Generated';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."ui_description" IS 'Description of the Summary Report to be displayed to the User';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."rr_summary"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."rr_summary" IS 'Receving Report Summary';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.transmit_error
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."transmit_error" (
    "id" bigint NOT NULL,
    "transmittal_status_cd" character varying(20) NOT NULL,
    "transmittal_stage_cd" character varying(20) NOT NULL,
    "transmittal_type_cd" character varying(20) NOT NULL,
    "source_table_name_cd" character varying(30) NOT NULL,
    "source_record_id" bigint NOT NULL,
    "retry_cnt" integer,
    "error_description" character varying(1000) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_transmit_error" PRIMARY KEY (id),
    CONSTRAINT "fk_trns_err_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_trns_err_source_table_name_cd" FOREIGN KEY (source_table_name_cd) REFERENCES aasbs.lu_table_name(cd),
    CONSTRAINT "fk_trns_err_stage" FOREIGN KEY (transmittal_stage_cd) REFERENCES aasbs_transmit.lu_transmittal_stage(cd),
    CONSTRAINT "fk_trns_err_status" FOREIGN KEY (transmittal_status_cd) REFERENCES aasbs_transmit.lu_transmittal_status(cd),
    CONSTRAINT "fk_trns_err_tran_type_cd" FOREIGN KEY (transmittal_type_cd) REFERENCES aasbs_transmit.lu_transmittal_type(cd),
    CONSTRAINT "fk_trns_err_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_transmital_err_cbui ON aasbs_transmit.transmit_error USING btree (created_by_user_id);
CREATE INDEX ix_transmital_err_src_rec_id ON aasbs_transmit.transmit_error USING btree (source_record_id);
CREATE INDEX ix_transmital_err_src_table ON aasbs_transmit.transmit_error USING btree (source_table_name_cd);
CREATE INDEX ix_transmital_err_trans_stage ON aasbs_transmit.transmit_error USING btree (transmittal_stage_cd);
CREATE INDEX ix_transmital_err_trans_stat ON aasbs_transmit.transmit_error USING btree (transmittal_status_cd);
CREATE INDEX ix_transmital_err_trans_type ON aasbs_transmit.transmit_error USING btree (transmittal_type_cd);
CREATE INDEX ix_transmital_err_ubui ON aasbs_transmit.transmit_error USING btree (updated_by_user_id);

CREATE TRIGGER transmit_error_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.transmit_error FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_transmit_error_hst();

COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."id" IS 'Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."transmittal_status_cd" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."transmittal_stage_cd" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."transmittal_type_cd" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."source_table_name_cd" IS 'Foreign Key to table AASBS.AWARD_MOD';
COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."source_record_id" IS 'Primary key ID of the record "SOURCE_TABLE_NAME_CD" table assoicated with this error';
COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."retry_cnt" IS 'Number of times retransmission was attempted';
COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."error_description" IS 'Error description';
COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."transmit_error"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."transmit_error" IS 'Log of Errors generatred while transmitting files to external systems (e.g., Pegasys)';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.transmittal
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."transmittal" (
    "id" bigint NOT NULL,
    "transmittal_name" character varying(200) NOT NULL,
    "transmittal_type_cd" character varying(20) NOT NULL,
    "batch_id" character varying(20) NOT NULL,
    "sent_dt" timestamp(6) without time zone NOT NULL,
    "batch_dt" date NOT NULL,
    "batch_sequence" bigint DEFAULT (1)::bigint NOT NULL,
    "release_count" bigint DEFAULT (1)::bigint NOT NULL,
    "transmit_return_msg" character varying(500),
    "transmit_status" character varying(20),
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_transmittal" PRIMARY KEY (id),
    CONSTRAINT "fk_transmital_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_transmital_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_transmital_batchdt ON aasbs_transmit.transmittal USING btree (batch_dt);
CREATE INDEX ix_transmital_cbui ON aasbs_transmit.transmittal USING btree (created_by_user_id);
CREATE INDEX ix_transmital_name ON aasbs_transmit.transmittal USING btree (transmittal_name);
CREATE INDEX ix_transmital_sentdate ON aasbs_transmit.transmittal USING btree (sent_dt);
CREATE INDEX ix_transmital_type ON aasbs_transmit.transmittal USING btree (transmittal_type_cd);
CREATE INDEX ix_transmital_ubui ON aasbs_transmit.transmittal USING btree (updated_by_user_id);

CREATE TRIGGER transmittal_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.transmittal FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_transmittal_hst();

COMMENT ON COLUMN "aasbs_transmit"."transmittal"."id" IS 'Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."transmittal_name" IS 'Name of Transmittal / Flat File';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."transmittal_type_cd" IS 'Foreign Key to aasbs_transmit.LU_TRANSMITTAL_TYPE';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."batch_id" IS 'Id specified in batch header of flat file';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."sent_dt" IS 'Date Transmittal was sent';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."batch_dt" IS 'Date Only version of sent_dt used for getting batch sequence';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."batch_sequence" IS 'Unique by sent_dt, 3 digit sequence number used for file naming';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."release_count" IS 'The number of main/summary records (i.e. po_summary, inv_summary) included in the batch';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."transmit_return_msg" IS 'Returned message';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."transmit_status" IS 'Status for transmitted batch. Values: ERROR, CONNECTION_ERROR';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."transmittal" IS 'Transmittal / Flat File parent table';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.transmittal_envelope
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."transmittal_envelope" (
    "id" bigint NOT NULL,
    "envelope_version" character varying(3) NOT NULL,
    "file_header_indicator" character varying(4) NOT NULL,
    "file_trailer_indicator" character varying(4) NOT NULL,
    "batch_header_indicator" character varying(4) NOT NULL,
    "batch_trailer_indicator" character varying(4) NOT NULL,
    "file_poc_id" bigint NOT NULL,
    "transmittal_prefix" character varying(20) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_transmittal_envelope" PRIMARY KEY (id),
    CONSTRAINT "fk_trns_envlop_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_trns_envlop_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_trns_envlop_cbui ON aasbs_transmit.transmittal_envelope USING btree (created_by_user_id);
CREATE INDEX ix_trns_envlop_ubui ON aasbs_transmit.transmittal_envelope USING btree (updated_by_user_id);

CREATE TRIGGER transmittal_envelope_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.transmittal_envelope FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_transmittal_envelope_hst();

COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."id" IS 'Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_ENVELOPE_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."envelope_version" IS 'Transmittal Envelope Version';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."file_header_indicator" IS 'Transmittal file header indicator';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."file_trailer_indicator" IS 'Transmittal file trailer indicator';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."batch_header_indicator" IS 'Transmittal batch header indicator';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."batch_trailer_indicator" IS 'Transmittal batch trailer indicator';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."file_poc_id" IS 'Foreign key to TRANSMITTAL_PO';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."transmittal_prefix" IS 'Envelope Prefix - FEDS';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_envelope"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."transmittal_envelope" IS 'Contains Transmittal Envelope records';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.transmittal_poc
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."transmittal_poc" (
    "id" bigint NOT NULL,
    "poc_name" character varying(80) NOT NULL,
    "poc_phone" character varying(30) NOT NULL,
    "poc_email" character varying(100) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_transmittal_poc" PRIMARY KEY (id),
    CONSTRAINT "fk_trns_poc_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_trns_poc_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_trns_poc_cbui ON aasbs_transmit.transmittal_poc USING btree (created_by_user_id);
CREATE INDEX ix_trns_poc_ubui ON aasbs_transmit.transmittal_poc USING btree (updated_by_user_id);

CREATE TRIGGER transmittal_poc_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.transmittal_poc FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_transmittal_poc_hst();

COMMENT ON COLUMN "aasbs_transmit"."transmittal_poc"."id" IS 'Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_POC_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_poc"."poc_name" IS 'Point of Contact name';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_poc"."poc_phone" IS 'Point of Contact phone number';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_poc"."poc_email" IS 'Point of Contact email address';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_poc"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_poc"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_poc"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_poc"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."transmittal_poc" IS 'Contains Transmittal Envelope records';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.transmittal_spec_version
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."transmittal_spec_version" (
    "id" bigint NOT NULL,
    "transmittal_type_cd" character varying(20) NOT NULL,
    "specification_version" character varying(20) NOT NULL,
    "transmit_file_prefix" character varying(10) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_transmittal_spec_version" PRIMARY KEY (id),
    CONSTRAINT "fk_spec_ver_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_spec_ver_tran_type_cd" FOREIGN KEY (transmittal_type_cd) REFERENCES aasbs_transmit.lu_transmittal_type(cd),
    CONSTRAINT "fk_spec_ver_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_spec_ver_cbui ON aasbs_transmit.transmittal_spec_version USING btree (created_by_user_id);
CREATE INDEX ix_spec_ver_tran_type ON aasbs_transmit.transmittal_spec_version USING btree (transmittal_type_cd);
CREATE INDEX ix_spec_ver_ubui ON aasbs_transmit.transmittal_spec_version USING btree (updated_by_user_id);

CREATE TRIGGER transmittal_spec_version_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.transmittal_spec_version FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_transmittal_spec_version_hst();

COMMENT ON COLUMN "aasbs_transmit"."transmittal_spec_version"."id" IS 'Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_SPEC_VERSION_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_spec_version"."transmittal_type_cd" IS 'Foreign Key to table AASBS_TRANSMIT.LU_TRANSMITTAL_TYPE';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_spec_version"."specification_version" IS 'Version of Transmittal Specification';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_spec_version"."transmit_file_prefix" IS 'Transmit File Prefix';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_spec_version"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_spec_version"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_spec_version"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_spec_version"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."transmittal_spec_version" IS 'Transmittal / provides the current specification version per transmittal type.';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.transmittal_specification
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."transmittal_specification" (
    "id" bigint NOT NULL,
    "specification_version" character varying(20) NOT NULL,
    "transmittal_type_cd" character varying(20) NOT NULL,
    "transmittal_record_type_cd" character varying(20) NOT NULL,
    "begin_position" bigint NOT NULL,
    "end_position" bigint NOT NULL,
    "field_length" bigint NOT NULL,
    "pad_direction" character varying(10),
    "pad_char" character(1) DEFAULT ' '::bpchar,
    "format_mask" character varying(20),
    "source_table_name" character varying(61) NOT NULL,
    "source_column_name" character varying(30) NOT NULL,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_transmittal_specification" PRIMARY KEY (id),
    CONSTRAINT "fk_trns_spec_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_trns_spec_rec_type_cd" FOREIGN KEY (transmittal_record_type_cd) REFERENCES aasbs_transmit.lu_transmittal_record_type(cd),
    CONSTRAINT "fk_trns_spec_tran_type_cd" FOREIGN KEY (transmittal_type_cd) REFERENCES aasbs_transmit.lu_transmittal_type(cd),
    CONSTRAINT "fk_trns_spec_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_trns_spec_cbui ON aasbs_transmit.transmittal_specification USING btree (created_by_user_id);
CREATE INDEX ix_trns_spec_rec_type_cd ON aasbs_transmit.transmittal_specification USING btree (transmittal_record_type_cd);
CREATE INDEX ix_trns_spec_tran_type_cd ON aasbs_transmit.transmittal_specification USING btree (transmittal_type_cd);
CREATE INDEX ix_trns_spec_ubui ON aasbs_transmit.transmittal_specification USING btree (updated_by_user_id);

CREATE TRIGGER transmittal_specification_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.transmittal_specification FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_transmittal_specification_hst();

COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."id" IS 'Numeric Primary Key for this table - fill using sequence: aasbs_transmit.TRANSMITTAL_SPECIFICATION_SEQ';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."specification_version" IS 'AAC envelope version';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."transmittal_type_cd" IS 'Foreign Key to table AASBS_TRANSMIT.LU_TRANSMITTAL_TYPE - Transmittal (batch) type';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."transmittal_record_type_cd" IS 'Foreign Key to table AASBS_TRANSMIT.LU_TRANSMITTAL_RECORD_TYPE - Transmittal Record Type specifier';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."begin_position" IS 'Starting column within a flat file of a field of data';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."end_position" IS 'Ending column within a flat file of a field of data';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."field_length" IS 'Length of the field of data';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."pad_direction" IS 'Left justify vs right justify indicator';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."pad_char" IS 'Padding character that fills extra space in the field of data';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."format_mask" IS 'Format mask for formatting dates, currency, and zip codes';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."source_table_name" IS 'Name of table from which the data is pulled into this field';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."source_column_name" IS 'Name of column within "source_table_name" from which the data is pulled into this field';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_specification"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."transmittal_specification" IS 'Billing Transmittal Specification';

-- ------------------------------------------------------------
-- Table: aasbs_transmit.transmittal_suspension
-- ------------------------------------------------------------

CREATE TABLE "aasbs_transmit"."transmittal_suspension" (
    "id" bigint NOT NULL,
    "transmittal_type_cd" character varying(20) NOT NULL,
    "suspend_start_dt" timestamp(6) without time zone,
    "suspend_end_dt" timestamp(6) without time zone,
    "created_by_user_id" character varying(16) NOT NULL,
    "created_dt" timestamp(6) without time zone NOT NULL,
    "updated_by_user_id" character varying(16) NOT NULL,
    "updated_dt" timestamp(6) without time zone NOT NULL,
    CONSTRAINT "pk_transmittal_suspension" PRIMARY KEY (id),
    CONSTRAINT "fk_transmt_suspnd_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_transmt_suspnd_trans_type" FOREIGN KEY (transmittal_type_cd) REFERENCES aasbs_transmit.lu_transmittal_type(cd),
    CONSTRAINT "fk_transmt_suspnd_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_transmt_suspnd_cbui ON aasbs_transmit.transmittal_suspension USING btree (created_by_user_id);
CREATE INDEX ix_transmt_suspnd_trans_type ON aasbs_transmit.transmittal_suspension USING btree (transmittal_type_cd);
CREATE INDEX ix_transmt_suspnd_ubui ON aasbs_transmit.transmittal_suspension USING btree (updated_by_user_id);

CREATE TRIGGER transmittal_suspension_trig BEFORE INSERT OR DELETE OR UPDATE ON aasbs_transmit.transmittal_suspension FOR EACH ROW EXECUTE FUNCTION aasbs_transmit.trg_transmittal_suspension_hst();

COMMENT ON COLUMN "aasbs_transmit"."transmittal_suspension"."id" IS 'Primary key. Set by sequence AASBS_TRANSMIT.TRANSMITTAL_SUSPENSION_SEQ.';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_suspension"."transmittal_type_cd" IS 'Foreign key to AASBS_TRANSMIT.LU_TRANSMITTAL_TYPE';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_suspension"."suspend_start_dt" IS 'Date to stop transmitting';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_suspension"."suspend_end_dt" IS 'Date to resume transmitting';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_suspension"."created_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that created this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_suspension"."created_dt" IS 'Date this database record was created';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_suspension"."updated_by_user_id" IS 'Foreign Key to table ASSIST.USERS - Application user that most recently updated this record';
COMMENT ON COLUMN "aasbs_transmit"."transmittal_suspension"."updated_dt" IS 'Most recent date this database record was updated';

COMMENT ON TABLE "aasbs_transmit"."transmittal_suspension" IS 'Indicates suspension of transmission of specific Transmittal Types';

-- ------------------------------------------------------------
-- Table: accrual.accrual_batch
-- ------------------------------------------------------------

CREATE TABLE "accrual"."accrual_batch" (
    "accrual_batch_id" bigint NOT NULL,
    "accrual_file_id" bigint NOT NULL,
    "batch_header_indicator" character varying(4) DEFAULT 'BHDR'::character varying,
    "batch_type" character varying(2) DEFAULT 'RV'::character varying,
    "batch_id" character varying(10),
    "batch_date" timestamp(0) without time zone,
    "batch_trailer" character varying(40),
    CONSTRAINT "accrual_batch_pk" PRIMARY KEY (accrual_batch_id),
    CONSTRAINT "accrual_file_id_fk" FOREIGN KEY (accrual_file_id) REFERENCES accrual.accrual_file(accrual_file_id)
);

CREATE INDEX ix_ab_accrual_file_id ON accrual.accrual_batch USING btree (accrual_file_id);

CREATE TRIGGER "accrual_batch$audtrg" AFTER INSERT OR DELETE OR UPDATE ON accrual.accrual_batch FOR EACH ROW EXECUTE FUNCTION accrual."accrual_batch$audtrg$accrual_batch"();

COMMENT ON COLUMN "accrual"."accrual_batch"."accrual_batch_id" IS 'UI Element: None. Primary key for the table';
COMMENT ON COLUMN "accrual"."accrual_batch"."accrual_file_id" IS 'UI Element: None. Foreign key to ACCRUAL_FILE table.';
COMMENT ON COLUMN "accrual"."accrual_batch"."batch_header_indicator" IS 'UI Element: None. This indicates the line is the batch header. Value: BHDR';
COMMENT ON COLUMN "accrual"."accrual_batch"."batch_type" IS 'UI Element: None. Refers to the configured Batch Type. Note the Batch Type value will be validated against FMESB and Pegasys reference tables (BATCH_TYPE and GSA_INCG_BATC_TYP, respectively). Value: RV';
COMMENT ON COLUMN "accrual"."accrual_batch"."batch_id" IS 'UI Element: None. Used with Batch Type to determine batch sequence or batch duplication. The format is XXXYDDD### where XXX = Batch ID Prefix, YDDD = Date and ### = Batch Sequence Number. For AAS the batch id prefix is RRV, therefore format is RRV + YDDD + ###';
COMMENT ON COLUMN "accrual"."accrual_batch"."batch_date" IS 'UI Element: None. Date of batch YYYYMMDD';
COMMENT ON COLUMN "accrual"."accrual_batch"."batch_trailer" IS 'UI Element: None. The trailer string for this batch envelope. Format: BTRLRV RRVYDDD###[#lines in 6 digits][$ value of batch with two digit decimal point in 15 digits. For negative values the first digit must be a minus sign].';

-- ------------------------------------------------------------
-- Table: accrual.accrual_header
-- ------------------------------------------------------------

CREATE TABLE "accrual"."accrual_header" (
    "accrual_header_id" bigint NOT NULL,
    "accrual_batch_id" bigint NOT NULL,
    "batch_type_indicator" character varying(2) DEFAULT 'V'::character varying,
    "information_indicator" character varying(1) DEFAULT '1'::character varying,
    "auto_reverse" character varying(1) DEFAULT 'F'::character varying,
    "correction_flag" character varying(1) DEFAULT 'F'::character varying,
    "document_number" character varying(30),
    "document_status" character varying(20) DEFAULT 'NEW'::character varying,
    "reset_doc_date_flag" character varying(1) DEFAULT 'F'::character varying,
    "reversed_flag" character varying(1) DEFAULT 'F'::character varying,
    "security_organization" character varying(10) DEFAULT 'GSA'::character varying,
    "external_document_number" character varying(30),
    "document_type" character varying(10),
    "system_id" character varying(10),
    "accounting_period" character varying(7),
    "created_by_user_id" character varying(16),
    "created_dt" timestamp(6) without time zone,
    "updated_by_user_id" character varying(16),
    "updated_dt" timestamp(6) without time zone,
    "title" character varying(100),
    CONSTRAINT "accrual_header_pk" PRIMARY KEY (accrual_header_id),
    CONSTRAINT "accrual_batch_id_fk" FOREIGN KEY (accrual_batch_id) REFERENCES accrual.accrual_batch(accrual_batch_id),
    CONSTRAINT "fk_accheader_cbui" FOREIGN KEY (created_by_user_id) REFERENCES assist.users(id),
    CONSTRAINT "fk_accheader_ubui" FOREIGN KEY (updated_by_user_id) REFERENCES assist.users(id)
);

CREATE INDEX ix_ah_accrual_batch_id ON accrual.accrual_header USING btree (accrual_batch_id);
CREATE INDEX ix_ah_created_by_user_id ON accrual.accrual_header USING btree (created_by_user_id);
CREATE INDEX ix_ah_updated_by_user_id ON accrual.accrual_header USING btree (updated_by_user_id);

CREATE TRIGGER "accrual_header$audtrg" AFTER INSERT OR DELETE OR UPDATE ON accrual.accrual_header FOR EACH ROW EXECUTE FUNCTION accrual."accrual_header$audtrg$accrual_header"();

COMMENT ON COLUMN "accrual"."accrual_header"."accrual_header_id" IS 'UI Element: None. Primary key for the table';
COMMENT ON COLUMN "accrual"."accrual_header"."accrual_batch_id" IS 'UI Element: None. Foreign key to ACCRUAL_BATCH table.';
COMMENT ON COLUMN "accrual"."accrual_header"."batch_type_indicator" IS 'UI Element: None. Identifies the Batch Type of the record. Batch Type indicator for AAS Revenue Accruals is RV.';
COMMENT ON COLUMN "accrual"."accrual_header"."information_indicator" IS 'UI Element: None. Identifies the line as a Header. Should always be the value 1';
COMMENT ON COLUMN "accrual"."accrual_header"."auto_reverse" IS 'UI Element: None. Indicates that the voucher should be automatically reversed. T or F.';
COMMENT ON COLUMN "accrual"."accrual_header"."correction_flag" IS 'UI Element: None. Indicates that the form represents a correction to an existing document. (T, X, F). Values: T (if a correction), X (if a cancellation), or F (if a new form)';
COMMENT ON COLUMN "accrual"."accrual_header"."document_number" IS 'UI Element: None. Document Number, which is unique in Pegasys. Format: DTYP + YYYYMMDD + #####. Where DTYP is the value from DOCUMENT_TYPE column, YYYYMMDD = Document date, #####= sequential number within document type and date';
COMMENT ON COLUMN "accrual"."accrual_header"."document_status" IS 'UI Element: None. AAS will always use NEW';
COMMENT ON COLUMN "accrual"."accrual_header"."reset_doc_date_flag" IS 'UI Element: None. Indicates that original document date should be reset to the date on this form. AAS will always use F.';
COMMENT ON COLUMN "accrual"."accrual_header"."reversed_flag" IS 'UI Element: None. Indicates whether the voucher has been reversed. AAS will always use F.';
COMMENT ON COLUMN "accrual"."accrual_header"."security_organization" IS 'UI Element: None. AAS will always use GSA.';
COMMENT ON COLUMN "accrual"."accrual_header"."external_document_number" IS 'UI Element: None. Act No/Task Number';
COMMENT ON COLUMN "accrual"."accrual_header"."document_type" IS 'UI Element: None. The document type is based on the Program code from the agreement (citation), as follows: AA10=SDA, AA20=EDA, GS14=KDA, IT14=NDA, IT23=XDA, IT31=HDA';
COMMENT ON COLUMN "accrual"."accrual_header"."system_id" IS 'UI Element: None. ID of the external system in which the transaction originated. Example: RBA';
COMMENT ON COLUMN "accrual"."accrual_header"."accounting_period" IS 'UI Element: None. Fiscal month and Fiscal year to which the accrual applies. Example: Accruals for February 2012 would have Accounting Period of 05/2012';

-- ------------------------------------------------------------
-- Table: accrual.accrual_income_line
-- ------------------------------------------------------------

CREATE TABLE "accrual"."accrual_income_line" (
    "accrual_income_line_id" bigint NOT NULL,
    "accrual_header_id" bigint NOT NULL,
    "accrual_income_id" bigint NOT NULL,
    CONSTRAINT "accrual_income_line_pk" PRIMARY KEY (accrual_income_line_id),
    CONSTRAINT "accrual_header_id_fk" FOREIGN KEY (accrual_header_id) REFERENCES accrual.accrual_header(accrual_header_id) NOT VALID,
    CONSTRAINT "accrual_income_id_fk" FOREIGN KEY (accrual_income_id) REFERENCES accrual.accrual_income(accrual_income_id) NOT VALID
);

CREATE INDEX ix_acrl_incl_incomeid ON accrual.accrual_income_line USING btree (accrual_income_id);
CREATE INDEX ix_ail_accrual_header_id ON accrual.accrual_income_line USING btree (accrual_header_id);

CREATE TRIGGER "accrual_income_line$audtrg" AFTER INSERT OR DELETE OR UPDATE ON accrual.accrual_income_line FOR EACH ROW EXECUTE FUNCTION accrual."accrual_income_line$audtrg$accrual_income_line"();

COMMENT ON COLUMN "accrual"."accrual_income_line"."accrual_income_line_id" IS 'UI Element: None. Primary key for the table';
COMMENT ON COLUMN "accrual"."accrual_income_line"."accrual_header_id" IS 'UI Element: None. Foreign key to ACCRUAL_HEADER table.';
COMMENT ON COLUMN "accrual"."accrual_income_line"."accrual_income_id" IS 'UI Element: None. Foreign key to ACCRUAL_INCOME table.';

-- ------------------------------------------------------------
-- Table: accrual.accrual_response_detail
-- ------------------------------------------------------------

CREATE TABLE "accrual"."accrual_response_detail" (
    "accrual_response_det_id" bigint NOT NULL,
    "accrual_response_header_id" bigint NOT NULL,
    "accrual_doc_num" character varying(30) NOT NULL,
    "accrual_response_detail" character varying(68),
    "accepted_status" character varying(8),
    CONSTRAINT "accrual_response_det_pk" PRIMARY KEY (accrual_response_det_id),
    CONSTRAINT "accrual_accepted_status" CHECK (accepted_status::text = ANY (ARRAY['ACCEPTED'::character varying::text, 'ERROR'::character varying::text, 'DELETE'::character varying::text])),
    CONSTRAINT "accrual_info_header_fk" FOREIGN KEY (accrual_response_header_id) REFERENCES accrual.accrual_response_header(accrual_response_header_id)
);

CREATE INDEX ix_ard_accrual_response_header_id ON accrual.accrual_response_detail USING btree (accrual_response_header_id);

CREATE TRIGGER "accrual_response_detail$audtrg" AFTER INSERT OR DELETE OR UPDATE ON accrual.accrual_response_detail FOR EACH ROW EXECUTE FUNCTION accrual."accrual_response_detail$audtrg$accrual_response_detail"();

COMMENT ON COLUMN "accrual"."accrual_response_detail"."accrual_response_det_id" IS 'Primary Key';
COMMENT ON COLUMN "accrual"."accrual_response_detail"."accrual_response_header_id" IS 'Foreign Key to the ACCRUAL response header';
COMMENT ON COLUMN "accrual"."accrual_response_detail"."accrual_doc_num" IS 'Uniquely identifies this bill detail record in the Pegasys Detail ACCRUAL Record table; this will reflect the value from the input file';
COMMENT ON COLUMN "accrual"."accrual_response_detail"."accrual_response_detail" IS 'The raw response';
COMMENT ON COLUMN "accrual"."accrual_response_detail"."accepted_status" IS 'Valid values ACCEPTED and ERROR, whether or not this record was accepted by Pegasys or rejected with an error';

COMMENT ON TABLE "accrual"."accrual_response_detail" IS 'This table logs the informational reponses from ACCRUAL transmission.  How do we tie this record back to a ACCRUAL entry, though?!';

-- ------------------------------------------------------------
-- Table: accrual.accrual_response_header
-- ------------------------------------------------------------

CREATE TABLE "accrual"."accrual_response_header" (
    "accrual_response_header_id" bigint NOT NULL,
    "pegasys_load_num" numeric(19,2),
    "start_date" timestamp(0) without time zone,
    "accrual_response_header" character varying(30),
    "accrual_batch_id" bigint,
    "response_file_name" character varying(260),
    "accrual_response_trailer" character varying(60),
    "total_num_of_records" numeric(19,2),
    "end_date" timestamp(0) without time zone,
    CONSTRAINT "accrual_response_header_pk" PRIMARY KEY (accrual_response_header_id),
    CONSTRAINT "accrual_response_header_fk" FOREIGN KEY (accrual_batch_id) REFERENCES accrual.accrual_batch(accrual_batch_id)
);

CREATE INDEX ix_arh_accrual_batch_id ON accrual.accrual_response_header USING btree (accrual_batch_id);

CREATE TRIGGER "accrual_response_header$audtrg" AFTER INSERT OR DELETE OR UPDATE ON accrual.accrual_response_header FOR EACH ROW EXECUTE FUNCTION accrual."accrual_response_header$audtrg$accrual_response_header"();

COMMENT ON COLUMN "accrual"."accrual_response_header"."accrual_response_header_id" IS 'Primary Key';
COMMENT ON COLUMN "accrual"."accrual_response_header"."pegasys_load_num" IS 'There is no Pegasys load num for accrual reposnses.';
COMMENT ON COLUMN "accrual"."accrual_response_header"."start_date" IS 'Date processed finished in Pegasys.  The sixteen digit string starting at position 15 in the response trailer.  TO_DATE(SUBSTR(ACCRUAL_RESPONSE_TRAILER,15,16), 'MMDDYYYYHHMISS')';
COMMENT ON COLUMN "accrual"."accrual_response_header"."accrual_response_header" IS 'The raw header value returned from Pegasys.';
COMMENT ON COLUMN "accrual"."accrual_response_header"."accrual_batch_id" IS 'Foreign Key back to the ACCRUAL batch header that was sent.';
COMMENT ON COLUMN "accrual"."accrual_response_header"."response_file_name" IS 'Name of the response file on disk';
COMMENT ON COLUMN "accrual"."accrual_response_header"."accrual_response_trailer" IS 'The raw trailer value returned from Pegasys.';
COMMENT ON COLUMN "accrual"."accrual_response_header"."total_num_of_records" IS 'The total number of records returned by the trailer';

COMMENT ON TABLE "accrual"."accrual_response_header" IS 'This table logs the reponse header and trailer from ACCRUAL transmission';

-- ------------------------------------------------------------
-- Table: accrual.accrual_response_message
-- ------------------------------------------------------------

CREATE TABLE "accrual"."accrual_response_message" (
    "accrual_response_msg_id" bigint NOT NULL,
    "accrual_response_det_id" bigint NOT NULL,
    "accrual_response_msg" character varying(556),
    "error_code" character varying(7),
    "error_label" character varying(256),
    "error_text" character varying(256),
    CONSTRAINT "accrual_response_msg_pk" PRIMARY KEY (accrual_response_msg_id),
    CONSTRAINT "accrual_response_msg_fk" FOREIGN KEY (accrual_response_det_id) REFERENCES accrual.accrual_response_detail(accrual_response_det_id)
);

CREATE INDEX ix_arm_accrual_response_det_id ON accrual.accrual_response_message USING btree (accrual_response_det_id);

CREATE TRIGGER "accrual_response_msg$audtrg" AFTER INSERT OR DELETE OR UPDATE ON accrual.accrual_response_message FOR EACH ROW EXECUTE FUNCTION accrual."accrual_response_msg$audtrg$accrual_response_message"();

COMMENT ON COLUMN "accrual"."accrual_response_message"."accrual_response_msg_id" IS 'Primary Key';
COMMENT ON COLUMN "accrual"."accrual_response_message"."accrual_response_det_id" IS 'This is a foreign key to ACCRUAL_RESPONSE_DETAIL to tie this Message to the row that was processed previously';
COMMENT ON COLUMN "accrual"."accrual_response_message"."accrual_response_msg" IS 'The raw error value returned from Pegasys.';
COMMENT ON COLUMN "accrual"."accrual_response_message"."error_code" IS 'The seven digit error code from Pegasys';
COMMENT ON COLUMN "accrual"."accrual_response_message"."error_label" IS 'Pegasys Error label';
COMMENT ON COLUMN "accrual"."accrual_response_message"."error_text" IS 'Pegasys Error text';

COMMENT ON TABLE "accrual"."accrual_response_message" IS 'This table logs the message rows for each record in the rejected and accepted files';

-- ------------------------------------------------------------
-- Table: agreement.agreement
-- ------------------------------------------------------------

CREATE TABLE "agreement"."agreement" (
    "agreement_id" bigint NOT NULL,
    "agreement_num" character varying(20) NOT NULL,
    "agreement_name" character varying(60),
    "agreement_end_date" timestamp(0) without time zone,
    "active_status" character varying(1),
    "funding_status" character varying(10),
    "document_type" character varying(4) DEFAULT 'UED'::character varying,
    "security_org" character varying(10),
    "boac_code" character varying(13),
    "addr_code" character varying(13),
    "title" character varying(50),
    "obligations_avail_amt" character varying(1),
    "commitments_avail_amt" character varying(1),
    "agreement_prepared_date" timestamp(0) without time zone DEFAULT now(),
    "acct_period" character varying(7),
    "suppress_printing" character varying(1),
    "funding_source" character varying(1),
    "maximum_agreement_amt" numeric(38,2),
    "transmission_status" character varying(19),
    "system_type" character varying(10),
    "doctype_defined_header_field" character varying(10),
    "novation_date" timestamp(0) without time zone,
    "customer_novation_code" character varying(10),
    "customer_novation_addr_code" character varying(13),
    "created_by" character varying(30) DEFAULT SESSION_USER,
    "creation_date" timestamp(0) without time zone DEFAULT now(),
    "last_updated_by" character varying(30) DEFAULT SESSION_USER,
    "last_update_date" timestamp(0) without time zone DEFAULT now(),
    "reimbursable" character varying(255),
    "sub_allocation" character varying(4),
    "agency_location_code" character varying(10),
    CONSTRAINT "agreement_pk" PRIMARY KEY (agreement_id),
    CONSTRAINT "agreement_caa_chk" CHECK (commitments_avail_amt::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "agreement_fund_src_chk" CHECK (funding_source::text = ANY (ARRAY['N'::character varying::text, 'F'::character varying::text])),
    CONSTRAINT "agreement_fund_status_chk" CHECK (funding_status::text = ANY (ARRAY['ESTIMATED'::character varying::text, 'ACTUAL'::character varying::text])),
    CONSTRAINT "agreement_oaa_chk" CHECK (obligations_avail_amt::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "agreement_print_chk" CHECK (suppress_printing::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "agreement_status_chk" CHECK (active_status::text = ANY (ARRAY['Y'::character varying::text, 'N'::character varying::text])),
    CONSTRAINT "agreement_sys_type_chk" CHECK (system_type::text = ANY (ARRAY['AABS'::character varying::text, 'NBA'::character varying::text, 'RBA'::character varying::text])),
    CONSTRAINT "agreement_trans_status_chk" CHECK (transmission_status::text = ANY (ARRAY['PENDING_CONVERSION'::character varying::text, 'IN_PROCESS'::character varying::text, 'PENDING'::character varying::text, 'PENDING_TRANSMIT'::character varying::text, 'ERROR'::character varying::text, 'PROCESSING_ERROR'::character varying::text, 'SUCCESS'::character varying::text]))
);

CREATE INDEX agreement_name_idx ON agreement.agreement USING btree (agreement_name);

CREATE TRIGGER "agreement$audtrg" AFTER INSERT OR DELETE OR UPDATE ON agreement.agreement FOR EACH ROW EXECUTE FUNCTION agreement."agreement$audtrg$agreement"();
CREATE TRIGGER agreement_b_i_u BEFORE INSERT OR UPDATE ON agreement.agreement FOR EACH ROW EXECUTE FUNCTION agreement."agreement_b_i_u$agreement"();

COMMENT ON COLUMN "agreement"."agreement"."agreement_id" IS 'UI Element: None. Primary key with sequence';
COMMENT ON COLUMN "agreement"."agreement"."agreement_num" IS 'UI Element: AGREEMENT_NUM. Agreement numbers are generated by AAS and must be unique. RBA agreement numbers will begin with X and NBA agreement numbers will begin with Z. Note: different agreement numbers are needed for each combination of Funding Document, BOAC2 and Citation/LOA (reflecting the customer supplied funding).';
COMMENT ON COLUMN "agreement"."agreement"."agreement_name" IS 'UI Element: Funding Doc Number NBA-OMIS Funding Document-Funding Doc #. Populate with the funding doc number';
COMMENT ON COLUMN "agreement"."agreement"."agreement_end_date" IS 'UI Element: AGREEMENT_END_DATE. Populate with the period of performance end date, citation expiration date, or appropriation expiration date from the funding document. If no specific date is available, then populate with the prepared date from the funding document plus a reasonable interval.';
COMMENT ON COLUMN "agreement"."agreement"."active_status" IS 'UI Element: None. For the purposes of this interface this field will always be true (active agreement).';
COMMENT ON COLUMN "agreement"."agreement"."funding_status" IS 'UI Element: None. Indicates the status of the funding for this agreement. Funds can be marked as ESTIMATED or ACTUAL.';
COMMENT ON COLUMN "agreement"."agreement"."document_type" IS 'UI Element: None. The ED document type for AAS is UED.';
COMMENT ON COLUMN "agreement"."agreement"."security_org" IS 'UI Element: None. GSA';
COMMENT ON COLUMN "agreement"."agreement"."boac_code" IS 'UI Element: RBA-Funding Document-BOAC2 NBA-OMIS Funding Document-BOAC2';
COMMENT ON COLUMN "agreement"."agreement"."addr_code" IS 'UI Element: RBA-Funding Document-BOAC2 NBA-OMIS Funding Document-BOAC2';
COMMENT ON COLUMN "agreement"."agreement"."title" IS 'UI Element: NBA-OMIS Funding Document-IA. IAA Number';
COMMENT ON COLUMN "agreement"."agreement"."obligations_avail_amt" IS 'UI Element: None. True';
COMMENT ON COLUMN "agreement"."agreement"."commitments_avail_amt" IS 'UI Element: None. False';
COMMENT ON COLUMN "agreement"."agreement"."agreement_prepared_date" IS 'UI Element: RBA-Funding Citation Submitted Date  NBA-. If this document is being submitted to establish a new agreement, populate this field with the Prepared Date on the Funding Document.
			If this document is being submitted to update an existing agreement, then populate with the current date. The date of the document being processed. There can be multiple documents processed against an agreement entity. The first agreement document, used to create the agreement, will use the document date to set the Agreement Start Date. The Document Date of subsequent agreement documents will update the Last Agreement Date on the Agreement.';
COMMENT ON COLUMN "agreement"."agreement"."acct_period" IS 'UI Element: RBA-Funding Citation Fiscal Year. Populate with the current fiscal year accounting period. MM/YYYY';
COMMENT ON COLUMN "agreement"."agreement"."suppress_printing" IS 'UI Element: Suppress Printing (checkbox). False';
COMMENT ON COLUMN "agreement"."agreement"."funding_source" IS 'UI Element: Populate with F or N for Federal or Non-Federal.';
COMMENT ON COLUMN "agreement"."agreement"."maximum_agreement_amt" IS 'UI Element: Maximum Agreement Amount';
COMMENT ON COLUMN "agreement"."agreement"."transmission_status" IS 'UI Element: None. The success or failure of transmission to BAAR.';
COMMENT ON COLUMN "agreement"."agreement"."system_type" IS 'UI Element: None. RBA or NBA.';
COMMENT ON COLUMN "agreement"."agreement"."doctype_defined_header_field" IS 'UI Element: None. TBD';
COMMENT ON COLUMN "agreement"."agreement"."novation_date" IS 'UI Element: RBA-Funding Document-BOAC2 NBA-OMIS Funding Document-BOAC2. Populate with effective date for BOAC2 (vendor) change.
			NOTE: Novation Date, Customer Novation Code and Customer Novation Address Code are ONLY populated when AAS wishes to associate an existing agreement with a different BOAC2 code. Used to update the vendor on an existing Agreement.
			To change the vendor (BOAC2 value) on an existing agreement, these fields (NVT_DT_CH, NVT_VEND_CD and NVT_VEND_ADDR_CD) are populated with the date and new vendor codes. Once processed, new transactions should use the new vendor code.';
COMMENT ON COLUMN "agreement"."agreement"."customer_novation_code" IS 'UI Element: RBA-Funding Document-BOAC2 NBA-OMIS Funding Document-BOAC2. Populate with new BOAC2 code only if the vendor on an existing agreement needs to be changed.';
COMMENT ON COLUMN "agreement"."agreement"."customer_novation_addr_code" IS 'UI Element: RBA-Funding Document-BOAC2 NBA-OMIS Funding Document-BOAC2. Populate with new BOAC2 code only if the vendor on an existing agreement needs to be changed.';
COMMENT ON COLUMN "agreement"."agreement"."sub_allocation" IS 'LOA embedded data that facilitates DoD monthly reconciliations';
COMMENT ON COLUMN "agreement"."agreement"."agency_location_code" IS 'Identifying code of the paying office for treasury when an agency is using OPAC billing';

-- ------------------------------------------------------------
-- Table: agreement.agreement_line
-- ------------------------------------------------------------

CREATE TABLE "agreement"."agreement_line" (
    "agreement_line_id" bigint NOT NULL,
    "agreement_id" bigint NOT NULL,
    "agreement_line_num" bigint DEFAULT (1)::bigint NOT NULL,
    "agreement_line_amt" numeric(38,2) NOT NULL,
    "agreement_line_state" character varying(1),
    "task_order_id" character varying(10),
    "transaction_type" character varying(4),
    "actual_cost_flag" character varying(1),
    "revenue_control" character varying(1),
    "spending_control" character varying(1),
    "agreement_charge_flag" character varying(1),
    "prior_year_adjustment" character varying(1),
    "customer_bbfy" character varying(4),
    "customer_fund" character varying(20),
    "referenced_doc_num" character varying(30),
    "referenced_doc_type" character varying(4),
    "partial_final_flag" character varying(1),
    "customer_vendor_code" character varying(10),
    "customer_vendor_address_code" character varying(13),
    "novation_date" timestamp(0) without time zone,
    "novation_vendor_code" character varying(10),
    "novation_vendor_address_code" character varying(13),
    "bbfy" character varying(4),
    "fund" character varying(20),
    "region_division" character varying(3),
    "org_code" character varying(20),
    "program_code" character varying(20),
    "activity_code" character varying(20),
    "revenue_source" character varying(20),
    "billing_start_date" timestamp(0) without time zone,
    "billing_end_date" timestamp(0) without time zone,
    "bill_type" character varying(24),
    "bill_print" character varying(10),
    "billing_control" character varying(1),
    "bill_cycle" character varying(29),
    "cust_long_line_of_acct" character varying(255),
    "created_by" character varying(30) DEFAULT SESSION_USER,
    "creation_date" timestamp(0) without time zone DEFAULT now(),
    "last_updated_by" character varying(30) DEFAULT SESSION_USER,
    "last_update_date" timestamp(0) without time zone DEFAULT now(),
    "referenced_line_num" integer,
    "arts_srvs_dscr" character varying(130),
    "tsym_agency_identifier" character varying(3),
    "tsym_begin_period" character varying(4),
    "tsym_end_period" character varying(4),
    "tsym_availability_type" character varying(1),
    "tsym_main_acct" character varying(4),
    "tsym_sub_acct" character varying(3),
    "tsym_allocation_trans_agency" character varying(3),
    "tsym_sublevel_prefix" character varying(2),
    "trfr_aid" character varying(3),
    "trfr_ata" character varying(3),
    "trfr_aval_typ" character varying(1),
    "trfr_bpoa" character varying(4),
    "trfr_epoa" character varying(4),
    "trfr_main_acct" character varying(4),
    "trfr_sub_acct" character varying(3),
    "ginv_order_line_num" integer,
    "ginv_order_num" character varying(20),
    "ginv_order_sched_num" integer,
    "ginv_rqst_svcg_type" character varying(1),
    CONSTRAINT "agreement_line_pk" PRIMARY KEY (agreement_line_id),
    CONSTRAINT "agreement_line_state_chk" CHECK (agreement_line_state::text = ANY (ARRAY['C'::character varying::text, 'O'::character varying::text])),
    CONSTRAINT "agreement_revenue_source_chk" CHECK (revenue_source::text = ANY (ARRAY['AABS'::character varying::text, 'NBA1'::character varying::text, 'RBA1'::character varying::text])),
    CONSTRAINT "agreement_id_fk" FOREIGN KEY (agreement_id) REFERENCES agreement.agreement(agreement_id)
);

CREATE INDEX agreement_line_fk_idx ON agreement.agreement_line USING btree (agreement_id);
CREATE INDEX agreement_line_num_idx ON agreement.agreement_line USING btree (agreement_line_num);

CREATE TRIGGER "agreement_line$audtrg" AFTER INSERT OR DELETE OR UPDATE ON agreement.agreement_line FOR EACH ROW EXECUTE FUNCTION agreement."agreement_line$audtrg$agreement_line"();
CREATE TRIGGER agreement_line_b_i_u BEFORE INSERT OR UPDATE ON agreement.agreement_line FOR EACH ROW EXECUTE FUNCTION agreement."agreement_line_b_i_u$agreement_line"();

COMMENT ON COLUMN "agreement"."agreement_line"."agreement_line_id" IS 'UI Element: None. Primary key with sequence';
COMMENT ON COLUMN "agreement"."agreement_line"."agreement_id" IS 'UI Element: None. Foreign key the AGREEMENT table';
COMMENT ON COLUMN "agreement"."agreement_line"."agreement_line_num" IS 'UI Element: None. This value represents the document line number. Each new ED or ID document can have only one line so this value will always be 1.';
COMMENT ON COLUMN "agreement"."agreement_line"."agreement_line_amt" IS 'UI Element: AGREEMENT_LINE_AMT. For a new agreement, supply the agreement amount. When changing the line amount on an existing agreement, supply the value the agreement is to be increased or decreased by (delta). The submission of a negative value will cause the supplied (negative) amount to be subtracted from the existing Agreements line¿s balance.';
COMMENT ON COLUMN "agreement"."agreement_line"."agreement_line_state" IS 'UI Element: None. Manages the state of an Agreement line (open or closed). Valid values are O for open or C for closed. AAS will always send O for this field.';
COMMENT ON COLUMN "agreement"."agreement_line"."task_order_id" IS 'UI Element: None. This field will be used for the NBA Task ID and will not be populated for RBA agreements. In the DES this field is identified as Doctype Defined Accounting Line Field #6';
COMMENT ON COLUMN "agreement"."agreement_line"."transaction_type" IS 'UI Element: None. Populate with 01 for federal and 02 for non-federal.';
COMMENT ON COLUMN "agreement"."agreement_line"."actual_cost_flag" IS 'UI Element: None. Establishes actual cost as the basis of billing. Pegasys requires a value here, but it won¿t be used because we¿re not billing.';
COMMENT ON COLUMN "agreement"."agreement_line"."revenue_control" IS 'UI Element: None. Refers to the level of an error the system will return if an agreement line is referenced for a total receivables line that exceeds the total spending amount. Use N (none) for all AAS agreements.';
COMMENT ON COLUMN "agreement"."agreement_line"."spending_control" IS 'UI Element: None. Spending control errors are off for AAS Agreements.';
COMMENT ON COLUMN "agreement"."agreement_line"."agreement_charge_flag" IS 'UI Element: None. True.';
COMMENT ON COLUMN "agreement"."agreement_line"."prior_year_adjustment" IS 'UI Element: None. Indicates whether the line is a prior year adjustment. AAS will use the Not a Prior Year Adjustment (X) value.';
COMMENT ON COLUMN "agreement"."agreement_line"."customer_bbfy" IS 'UI Element: None. agreement start date four digit year';
COMMENT ON COLUMN "agreement"."agreement_line"."customer_fund" IS 'UI Element: None. Customer fund code from obligating document (i.e., 192X, 455F, etc.).';
COMMENT ON COLUMN "agreement"."agreement_line"."referenced_doc_num" IS 'UI Element: None. This is the Pegasys document number of the referenced, obligating document.
			This field is not used since PCAS will not be generating NV documents for AAS. However, AAS can supply the
			obligating document number for informational purposes. If a document number is supplied, the document must
			actually exist in Pegasys. This field is used in conjunction with Referenced Document Type and Referenced Accounting Line Number.';
COMMENT ON COLUMN "agreement"."agreement_line"."referenced_doc_type" IS 'UI Element: None. Pegasys document type of referenced obligating document.';
COMMENT ON COLUMN "agreement"."agreement_line"."partial_final_flag" IS 'UI Element: None. Always set to ¿P¿ for partial. Setting this flag to final indicates that the document is the last in the chain and liquidates the chains funding.';
COMMENT ON COLUMN "agreement"."agreement_line"."customer_vendor_code" IS 'UI Element: NBA-OMIS Funding Document-BOAC2. BOAC2 code of the customer.';
COMMENT ON COLUMN "agreement"."agreement_line"."customer_vendor_address_code" IS 'UI Element: NBA-OMIS Funding Document-BOAC2. BOAC2 code of the customer.';
COMMENT ON COLUMN "agreement"."agreement_line"."novation_date" IS 'UI Element: None. The effective date for Customer vendor code change. ';
COMMENT ON COLUMN "agreement"."agreement_line"."novation_vendor_code" IS 'UI Element: NBA-OMIS Funding Document-BOAC2. The new Customer vendor code (BOAC2). ';
COMMENT ON COLUMN "agreement"."agreement_line"."novation_vendor_address_code" IS 'UI Element: NBA-OMIS Funding Document-BOAC2. The new Customer vendor code (BOAC2). ';
COMMENT ON COLUMN "agreement"."agreement_line"."bbfy" IS 'UI Element: None. agreement start date four digit year';
COMMENT ON COLUMN "agreement"."agreement_line"."fund" IS 'UI Element: None. 285F?!';
COMMENT ON COLUMN "agreement"."agreement_line"."region_division" IS 'UI Element: None. Two-digit Region Code. Valid values are 00 through 11.';
COMMENT ON COLUMN "agreement"."agreement_line"."org_code" IS 'UI Element: None. New Organization Code, which is an 8-character code where positions two and three are equal to the two-digit Region Code.';
COMMENT ON COLUMN "agreement"."agreement_line"."program_code" IS 'UI Element: None. New Program Code, which can be derived from the old two-character Budget Activity Code';
COMMENT ON COLUMN "agreement"."agreement_line"."activity_code" IS 'UI Element: None. New Activity Code, which can be derived from the old three-character Function Code';
COMMENT ON COLUMN "agreement"."agreement_line"."revenue_source" IS 'UI Element: None. NBA1 if the Program Code is AA10 or IT31.  RBA1 if the Program Code is AA20, GS13, IT14, or IT23';
COMMENT ON COLUMN "agreement"."agreement_line"."billing_start_date" IS 'UI Element: RBA-Funding Citation Pop start Date NBA-OMIS Funding Document-POP Start. Populate with Agreement start date.';
COMMENT ON COLUMN "agreement"."agreement_line"."billing_end_date" IS 'UI Element: RBA-Funding Citation pop end Date NBA-OMIS Funding Document-POP END. Populate with the agreement end date plus 5 years. For no-year funds, populate with agreement end date plus 30 years.';
COMMENT ON COLUMN "agreement"."agreement_line"."bill_type" IS 'UI Element: None. Set to MANUAL to prevent the generation of bills by PCAS.';
COMMENT ON COLUMN "agreement"."agreement_line"."bill_print" IS 'UI Element: None. set to value: NO';
COMMENT ON COLUMN "agreement"."agreement_line"."billing_control" IS 'UI Element: None. set to: N';
COMMENT ON COLUMN "agreement"."agreement_line"."bill_cycle" IS 'UI Element: None. Set to: AT COMPLETION. Determines how frequently the billing process should consider billing the customer on this agreement. This field is not used for AAS agreements but must be populated with a valid value.';
COMMENT ON COLUMN "agreement"."agreement_line"."cust_long_line_of_acct" IS 'UI Element: RBA Funding Citation Customer long line of accounting  NBA-OMIS Funding Document-Accounting Data. Populate with the cust long line of accounting.';
COMMENT ON COLUMN "agreement"."agreement_line"."arts_srvs_dscr" IS 'UI Element: RBA-Funding Citation, Citation Code, populate with funding_citations.fund_cite_code. NBA-OMIS-Funding Summary, Accounting Data. Populate with MIPRS.ACCT_DATA. Do not populate for Agency 047.';
COMMENT ON COLUMN "agreement"."agreement_line"."tsym_agency_identifier" IS 'UI Element: RBA-Funding Citation, Requesting Agency, populate with funding_citations.requesting_agency_id. NBA-OMIS-Funding Summary, Request Agy Code, populate with MIPRS.REQUEST_AGENCY. Do not populate for UIDs';
COMMENT ON COLUMN "agreement"."agreement_line"."tsym_begin_period" IS 'UI Element: RBA-Funding Citation, First FY Available, populate with funding_citations.loa_period_first. NBA-OMIS-Funding Summary, Available FY From, populate with MIPRS.FY_FIRST_AVAIL. Do not populate for UIDs';
COMMENT ON COLUMN "agreement"."agreement_line"."tsym_end_period" IS 'UI Element: RBA-Funding Citation, Last FY Available, populate with funding_citations.loa_period_last. NBA-OMIS-Funding Summary, Available FY To, populate with MIPRS.FY_LAST_AVAIL. Do not populate for UIDs';
COMMENT ON COLUMN "agreement"."agreement_line"."tsym_availability_type" IS 'UI Element: None. Only populate for non-UIDs; if funding type = 'No Year' then 'X' otherwise blank.  RBA-populate with funding_citations.loa_availability_type(Funding Type is funding_citations.loa_type). NBA-OMIS-Funding Type is MIPRS.AVAILABILITY_TYPE ('N' = No Year).';
COMMENT ON COLUMN "agreement"."agreement_line"."tsym_main_acct" IS 'UI Element: RBA-Funding Citation, Appropriation, populate with funding_citations.appropriation_loa. NBA-OMIS-Funding Summary, Appropriation, populate with MIPRS.APPROPRIATION. Do not populate for UIDs';
COMMENT ON COLUMN "agreement"."agreement_line"."tsym_sub_acct" IS 'UI Element: RBA-Funding Citation, Treasury Sub Account, populate with funding_citations.treasury_subaccount. NBA-OMIS-Funding Summary, Sub-Acct, populate with MIPRS.TREASURY_SUB_ACCOUNT.';
COMMENT ON COLUMN "agreement"."agreement_line"."tsym_sublevel_prefix" IS 'UI Element: RBA-Funding Citation, TSYM Sublevel Prefix. NBA-OMIS Funding TSYM Sublevel Prefix';
COMMENT ON COLUMN "agreement"."agreement_line"."trfr_aid" IS 'Transfer Agency Identifier';
COMMENT ON COLUMN "agreement"."agreement_line"."trfr_ata" IS 'Transfer Allocation Transfer Agency';
COMMENT ON COLUMN "agreement"."agreement_line"."trfr_aval_typ" IS 'Transfer Availability Type';
COMMENT ON COLUMN "agreement"."agreement_line"."trfr_bpoa" IS 'Transfer Beginning Period of Availability';
COMMENT ON COLUMN "agreement"."agreement_line"."trfr_epoa" IS 'Transfer Ending Period of Availability';
COMMENT ON COLUMN "agreement"."agreement_line"."trfr_main_acct" IS 'Transfer Main Account';
COMMENT ON COLUMN "agreement"."agreement_line"."trfr_sub_acct" IS 'Transfer Sub Account';
COMMENT ON COLUMN "agreement"."agreement_line"."ginv_order_line_num" IS 'ginv_order_line_num From G-INVOICING System';
COMMENT ON COLUMN "agreement"."agreement_line"."ginv_order_num" IS 'ginv_order_num From G-INVOICING System';
COMMENT ON COLUMN "agreement"."agreement_line"."ginv_order_sched_num" IS 'ginv_order_sched_num From G-INVOICING System';
COMMENT ON COLUMN "agreement"."agreement_line"."ginv_rqst_svcg_type" IS 'ginv_rqst_svcg_type From G-INVOICING System';

-- ------------------------------------------------------------
-- Table: agreement.agreement_log
-- ------------------------------------------------------------

CREATE TABLE "agreement"."agreement_log" (
    "agreement_log_id" bigint NOT NULL,
    "document_num" character varying(30) NOT NULL,
    "agreement_id" bigint NOT NULL,
    "request_date" timestamp(0) without time zone,
    "response_date" timestamp(0) without time zone,
    "created_by" character varying(30) DEFAULT SESSION_USER,
    "creation_date" timestamp(0) without time zone DEFAULT now(),
    "last_updated_by" character varying(30) DEFAULT SESSION_USER,
    "last_update_date" timestamp(0) without time zone DEFAULT now(),
    "transmission_status" character varying(19),
    "request" text,
    "response" text,
    CONSTRAINT "agreement_log_pk" PRIMARY KEY (agreement_log_id),
    CONSTRAINT "agreement_log_agreement_id_fk" FOREIGN KEY (agreement_id) REFERENCES agreement.agreement(agreement_id)
);

CREATE INDEX agreement_log_fk_idx ON agreement.agreement_log USING btree (agreement_id);

CREATE TRIGGER "agreement_log$audtrg" AFTER INSERT OR DELETE OR UPDATE ON agreement.agreement_log FOR EACH ROW EXECUTE FUNCTION agreement."agreement_log$audtrg$agreement_log"();
CREATE TRIGGER agreement_log_b_i_u BEFORE INSERT OR UPDATE ON agreement.agreement_log FOR EACH ROW EXECUTE FUNCTION agreement."agreement_log_b_i_u$agreement_log"();

COMMENT ON COLUMN "agreement"."agreement_log"."agreement_log_id" IS 'UI Element: None. Primary key with sequence';
COMMENT ON COLUMN "agreement"."agreement_log"."document_num" IS 'UI Element: None. This will be generated by Assist. Document number format:UEDYYYYMMDD####
            UED is the ED document type, #### is a 4-digit sequential number beginning with 0000 (can start at 0000 for each new date)
            For example, the document number for the first external direct document created on 12/16/2014 would be UED201412160001';
COMMENT ON COLUMN "agreement"."agreement_log"."agreement_id" IS 'UI Element: None. Links to Agreement table PK';
COMMENT ON COLUMN "agreement"."agreement_log"."request_date" IS 'UI Element: None. This is the date-time that the SOAP request message was made.';
COMMENT ON COLUMN "agreement"."agreement_log"."response_date" IS 'UI Element: None. This is the date-time that the SOAP response message was made.';

-- ------------------------------------------------------------
-- Table: billing.billing
-- ------------------------------------------------------------

CREATE TABLE "billing"."billing" (
    "billing_id" bigint NOT NULL,
    "agreement_num" character varying(20) NOT NULL,
    "agreement_line_num" character varying(5),
    "system_type" character varying(10),
    "billing_amount" numeric(28,2),
    "vendor_customer_code" character varying(6),
    "designated_agent_code" character varying(6),
    "agency_code" character varying(3),
    "title" character varying(50),
    "external_system_doc_num" character varying(8),
    "activity_code" character varying(5),
    "fund" character varying(4),
    "region_division" character varying(3),
    "org_code" character varying(8),
    "program_code" character varying(4),
    "revenue_source" character varying(4),
    "credit_indicator" character varying(1),
    "advance_indicator" character varying(1),
    "pop_start" timestamp(0) without time zone,
    "pop_end" timestamp(0) without time zone,
    "related_statement_num" character varying(10),
    "interfund_indicator" character varying(1),
    "reference_doc_type" character varying(2),
    "reference_doc_num" character varying(20),
    "reference_doc_line_num" character varying(2),
    "acct_classification_code" character varying(8),
    "acct_classification_ref_num" character varying(12),
    "fiscal_station_num" integer,
    "description" character varying(130),
    "cust_sublevel_prefix" character varying(2),
    "tysm_allocation_trans_agency" character varying(3),
    "tysm_agency_identifier" character varying(3),
    "tysm_begin_period" character varying(4),
    "tysm_end_period" character varying(4),
    "tysm_avalability_type" character varying(1),
    "tysm_main_acct" character varying(4),
    "tysm_sub_acct" character varying(3),
    "duns" character varying(9),
    "duns_plus_four" character varying(13),
    "funding_doc_num" character varying(20),
    "quantity" integer,
    "unit_of_issue" character varying(2),
    "description_of_services" character varying(40),
    "group_number" character varying(10),
    "additional_text" character varying(50),
    "created_by" character varying(30) DEFAULT SESSION_USER,
    "creation_date" timestamp(0) without time zone DEFAULT now(),
    "last_updated_by" character varying(30) DEFAULT SESSION_USER,
    "last_update_date" timestamp(0) without time zone DEFAULT now(),
    "bbfy" character varying(4),
    "assist_ext_billing_id" bigint,
    "statement_num" character varying(8),
    "customer_fund" character varying(4),
    "agcy_uei" character varying(12),
    "uei" character varying(16),
    "uei_plus4" character varying(12),
    "ginv_ref_performance_num" character varying(20),
    "ginv_ref_performance_num_detail" character varying(6),
    "vuei" character varying(16) GENERATED ALWAYS AS (
CASE
    WHEN (uei IS NULL) THEN duns
    ELSE uei
END) STORED,
    "vuei_plus4" character varying(13) GENERATED ALWAYS AS (
CASE
    WHEN (uei_plus4 IS NULL) THEN duns_plus_four
    ELSE uei_plus4
END) STORED,
    "billing_summary_id" bigint,
    CONSTRAINT "billing_pk" PRIMARY KEY (billing_id),
    CONSTRAINT "billing_uk_bill_sum_id" UNIQUE (billing_summary_id),
    CONSTRAINT "billing_advance_chk" CHECK (advance_indicator::text = ANY (ARRAY['T'::character varying::text, 'O'::character varying::text, 'F'::character varying::text])),
    CONSTRAINT "billing_interfund_chk" CHECK (interfund_indicator::text = ANY (ARRAY['T'::character varying::text, 'A'::character varying::text, 'F'::character varying::text])),
    CONSTRAINT "billing_fk_bill_sum_id" FOREIGN KEY (billing_summary_id) REFERENCES aasbs_transmit.billing_summary(id)
);

CREATE INDEX assist_ext_billing_id_idx ON billing.billing USING btree (assist_ext_billing_id);
CREATE INDEX billing_agreement_num_idx ON billing.billing USING btree (agreement_num);

CREATE TRIGGER "billing$audtrg" AFTER INSERT OR DELETE OR UPDATE ON billing.billing FOR EACH ROW EXECUTE FUNCTION billing."billing$audtrg$billing"();
CREATE TRIGGER billing_b_i BEFORE INSERT ON billing.billing FOR EACH ROW EXECUTE FUNCTION billing."billing_b_i$billing"();
CREATE TRIGGER billing_b_i_u BEFORE INSERT OR UPDATE ON billing.billing FOR EACH ROW EXECUTE FUNCTION billing."billing_b_i_u$billing"();

COMMENT ON COLUMN "billing"."billing"."billing_id" IS 'UI Element: None. Primary key with sequence';
COMMENT ON COLUMN "billing"."billing"."agreement_num" IS 'UI Element: AGREEMENT_NUM  This is the foreign key to the AGREEMENT table';
COMMENT ON COLUMN "billing"."billing"."agreement_line_num" IS 'UI Element: NONE.  All line numbers are currently 1';
COMMENT ON COLUMN "billing"."billing"."system_type" IS 'UI Element: NONE.  RBA or NBA';
COMMENT ON COLUMN "billing"."billing"."billing_amount" IS 'UI Element: ITOMS - multiple fields.  Billing amount';
COMMENT ON COLUMN "billing"."billing"."vendor_customer_code" IS 'UI Element: RBA-Funding Citation-BOAC2 NBA-OMIS Funding Doc-BOAC 2.  BOAC2';
COMMENT ON COLUMN "billing"."billing"."designated_agent_code" IS 'UI Element: RBA-Funding Citation-BOAC1 NBA-OMIS Funding Doc-BOAC 1';
COMMENT ON COLUMN "billing"."billing"."agency_code" IS 'UI Element: NONE. Agency code.';
COMMENT ON COLUMN "billing"."billing"."title" IS 'UI Element: Funding Document-IAA Number NBA-OMIS Funding Doc-IA';
COMMENT ON COLUMN "billing"."billing"."external_system_doc_num" IS 'UI Element: NBA-TASK ORDER or RBA-CUSTOMER ID.  Required. The XSYS_DOC_NUM field is mapped to the ACT_NO on the FMIS VAT.';
COMMENT ON COLUMN "billing"."billing"."activity_code" IS 'UI Element: RBA-Purchase Order Function Code.  F11->AF127 F31->AF123 F51->AF151 F81->AF121 F99->AF120';
COMMENT ON COLUMN "billing"."billing"."fund" IS 'UI Element: Fund. 285F?!';
COMMENT ON COLUMN "billing"."billing"."region_division" IS 'UI Element: RBA-COI Organization Code NBA-Not visible.  Required. New Organization Codes are implemented as part of the FAS One-Fund reorganization. Examples:
			If Region is 03, Program Code is AA20, and Organization Code is A03VR110, then set the new Organization Code to Q03FA000.
			If Region is 30, Program Code is AA20, and Organization Code is A03VR114, then set the new Organization Code to Q03FA100.';
COMMENT ON COLUMN "billing"."billing"."org_code" IS 'UI Element: RBA-Funding Citation NBA-Not visible - Budget Activity Code. New Organization Code, which is an 8-character code where positions two and three are equal to the two-digit Region Code.';
COMMENT ON COLUMN "billing"."billing"."program_code" IS 'UI Element: NONE.  B3->IT23 F1->AA20 F2->AA10 FL->IT31 FQ->IT14 FR->GS14 P1->AA20 P2->AA10';
COMMENT ON COLUMN "billing"."billing"."revenue_source" IS 'UI Element: NONE.  NBA1 if the Program Code (row 15 in this table) is AA10 or IT31, RBA1 if the Program Code (row 15 in this table) is AA20, GS13, IT14, or IT23';
COMMENT ON COLUMN "billing"."billing"."credit_indicator" IS 'UI Element: NONE.  Required for credits.';
COMMENT ON COLUMN "billing"."billing"."advance_indicator" IS 'UI Element: NONE.  T for advance (a bill PDF will be created), O for advance offset (reate a null-posting bill that provides the amount that the Finance Center should use to offset the advance. No bill PDF will be created.), F (regular bill) otherwise';
COMMENT ON COLUMN "billing"."billing"."pop_start" IS 'UI Element: RBA-Purchase Order Pop start date NBA-OMIS POP Start.  Set to the first day of the month corresponding to the Service Month/Year';
COMMENT ON COLUMN "billing"."billing"."pop_end" IS 'UI Element: RBA-Purchase Order Pop End Date NBA-OMIS POP End.  Set to the last day of the month corresponding to the Service Month/Year';
COMMENT ON COLUMN "billing"."billing"."related_statement_num" IS 'UI Element: NONE.  Identifies the related Pegasys statement number for credits and rebills.';
COMMENT ON COLUMN "billing"."billing"."interfund_indicator" IS 'UI Element: NONE.  If Transaction Type is N, set to F If Transaction Type is Y, set to A for intrafund transactions or T for interfund transactions';
COMMENT ON COLUMN "billing"."billing"."reference_doc_type" IS 'UI Element: NONE.  If the Interfund Indicator is T or A, set to the Document type of the Pegasys obligating document that provides Buyer-side accounting dimensions, Otherwise, leave blank';
COMMENT ON COLUMN "billing"."billing"."reference_doc_num" IS 'UI Element: NONE.  If the Interfund Indicator is T or A, set to the Document Number of the Pegasys obligating document that provides Buyer-side accounting dimensions, Otherwise, leave blank';
COMMENT ON COLUMN "billing"."billing"."reference_doc_line_num" IS 'UI Element: NONE.  If the Interfund Indicator (row 24 in this table) is T or A, set to the accounting line number of the Pegasys obligating document that provides Buyer-side accounting dimensions, Otherwise, leave blank';
COMMENT ON COLUMN "billing"."billing"."acct_classification_code" IS 'UI Element: NONE.  This may be not needed if this will just be BOAC1';
COMMENT ON COLUMN "billing"."billing"."acct_classification_ref_num" IS 'UI Element: NONE.  Task Order/Cust. ID + Subtask. Subtask is not used for NBA, so this field will contain only Task Order/Cust. ID for NBA';
COMMENT ON COLUMN "billing"."billing"."fiscal_station_num" IS 'UI Element: NONE.  This is defaulted to 0';
COMMENT ON COLUMN "billing"."billing"."description" IS 'UI Element: NONE.  If the Interfund Indicator is F, set to Customer Purchase Order, otherwise, leave blank';
COMMENT ON COLUMN "billing"."billing"."cust_sublevel_prefix" IS 'UI Element: NONE.  Customer Sublevel Prefix';
COMMENT ON COLUMN "billing"."billing"."tysm_allocation_trans_agency" IS 'UI Element: NONE.  Customer Treasury Symbol: Allocation Transfer Agency';
COMMENT ON COLUMN "billing"."billing"."tysm_agency_identifier" IS 'UI Element: NONE.  Customer Treasury Symbol: Agency Identifier';
COMMENT ON COLUMN "billing"."billing"."tysm_begin_period" IS 'UI Element: NONE.  Customer Treasury Symbol: Beginning Period of Availability';
COMMENT ON COLUMN "billing"."billing"."tysm_end_period" IS 'UI Element: NONE.  Customer Treasury Symbol: Ending Period of Availability';
COMMENT ON COLUMN "billing"."billing"."tysm_avalability_type" IS 'UI Element: NONE.  Customer Treasury Symbol: Availability Type';
COMMENT ON COLUMN "billing"."billing"."tysm_main_acct" IS 'UI Element: NONE.  Customer Treasury Symbol: Main Account';
COMMENT ON COLUMN "billing"."billing"."tysm_sub_acct" IS 'UI Element: This will be a new field on the Funding Citation.  Customer Treasury Symbol: Sub Account';
COMMENT ON COLUMN "billing"."billing"."duns" IS 'UI Element: RBA-Contractor DUNS NBA-OMIS Funding Document-Client DUNS#.  Agency DUNS';
COMMENT ON COLUMN "billing"."billing"."duns_plus_four" IS 'UI Element: RBA-Contractor DUNS+4 NBA-OMIS Funding Document-Client DUNS#.  Agency DUNS+4';
COMMENT ON COLUMN "billing"."billing"."funding_doc_num" IS 'UI Element: RBA-Fundings Funding Document Number NBA-Funding Document Screen-Funding Doc#. FNDG_DOCSRC_NUM';
COMMENT ON COLUMN "billing"."billing"."quantity" IS 'UI Element: RBA-Task Order Quantity';
COMMENT ON COLUMN "billing"."billing"."unit_of_issue" IS 'UI Element: RBA-Task Order Unit. Default to EA for EAch';
COMMENT ON COLUMN "billing"."billing"."description_of_services" IS 'UI Element: RBA-Task Order Description';
COMMENT ON COLUMN "billing"."billing"."group_number" IS 'UI Element: NONE. May be populated for situations where multiple sequential records should be loaded as one unit of work (all are loaded or none are loaded). This field must be left blank unless there are 2 or more sequential records in the file with the same value.';
COMMENT ON COLUMN "billing"."billing"."additional_text" IS 'ASSIST2:  Foreign key to AASBS.LOA_LEDGER.ID.  NBA:  Foreign key to OMIS.BID_BAAR.BID_BAAR_ID ~ .BID_ID ~ .AGREEMENT_NUM';
COMMENT ON COLUMN "billing"."billing"."statement_num" IS 'Alphanumeric unique identifier for grouping of bills.';
COMMENT ON COLUMN "billing"."billing"."customer_fund" IS 'Customer Fund assigned from the funding doc, and used to determine Interfund Indicator and Activity Code. Different from Fund.';
COMMENT ON COLUMN "billing"."billing"."agcy_uei" IS 'GSA UEI - Unique Entity Identifier';
COMMENT ON COLUMN "billing"."billing"."uei" IS 'Unique Entity Identifier (UEI) - replaces DUNS';
COMMENT ON COLUMN "billing"."billing"."uei_plus4" IS 'Electronic Funds Transfer Indicator, generally paired with UEI, replaces DUNS Plus 4';
COMMENT ON COLUMN "billing"."billing"."ginv_ref_performance_num" IS 'G-Invoice Performance Number';
COMMENT ON COLUMN "billing"."billing"."ginv_ref_performance_num_detail" IS 'G-Invoice Performance Number Detail';
COMMENT ON COLUMN "billing"."billing"."billing_summary_id" IS 'Foreign Key to AASBS_TRANSMIT.BILLING_SUMMARY.ID';

-- ------------------------------------------------------------
-- Table: billing.billing_batch_detail
-- ------------------------------------------------------------

CREATE TABLE "billing"."billing_batch_detail" (
    "billing_batch_detail_id" bigint NOT NULL,
    "billing_id" bigint NOT NULL,
    "batch_id" character varying(10) NOT NULL,
    "billing_record_id" character varying(18) NOT NULL,
    "detail_trailer" character varying(60),
    "batch_trailer" character varying(60),
    CONSTRAINT "billing_batch_detail_fk" FOREIGN KEY (batch_id) REFERENCES billing.billing_batch_header(batch_id),
    CONSTRAINT "billing_det_billing_fk" FOREIGN KEY (billing_id) REFERENCES billing.billing(billing_id)
);

CREATE INDEX batch_id_idx ON billing.billing_batch_detail USING btree (batch_id);
CREATE INDEX billing_id_idx ON billing.billing_batch_detail USING btree (billing_id);

CREATE TRIGGER "billing_batch_detail$audtrg" AFTER INSERT OR DELETE OR UPDATE ON billing.billing_batch_detail FOR EACH ROW EXECUTE FUNCTION billing."billing_batch_detail$audtrg$billing_batch_detail"();

COMMENT ON COLUMN "billing"."billing_batch_detail"."billing_batch_detail_id" IS 'UI Element: None. PK for this table';
COMMENT ON COLUMN "billing"."billing_batch_detail"."billing_id" IS 'UI Element: None. FK link to BILLING table';
COMMENT ON COLUMN "billing"."billing_batch_detail"."batch_id" IS 'UI Element: None. FK link to BILLING_BATCH_HEADER';
COMMENT ON COLUMN "billing"."billing_batch_detail"."billing_record_id" IS 'UI Element: None. RNB + MMDDYYYY#######, where MMDDYYYY is the date, and ####### is a sequence number within the date.';
COMMENT ON COLUMN "billing"."billing_batch_detail"."detail_trailer" IS 'UI Element: None. Trailer string for the detail records';
COMMENT ON COLUMN "billing"."billing_batch_detail"."batch_trailer" IS 'UI Element: None. Trailer string for this batch envelope';

COMMENT ON TABLE "billing"."billing_batch_detail" IS 'UI Element: None. This table links details about a batch transmission with the specific billing item sent. See BAAR 31b AAS Billing DES-Final, exhibit 5.1 and 5.7 for file/batch/detail structure';

-- ------------------------------------------------------------
-- Table: billing.billing_response_info
-- ------------------------------------------------------------

CREATE TABLE "billing"."billing_response_info" (
    "billing_response_info_id" bigint NOT NULL,
    "billing_response_header_id" bigint NOT NULL,
    "billing_record_id" character varying(28) NOT NULL,
    "billing_response_info" character varying(556),
    "accepted_status" character varying(8),
    CONSTRAINT "billing_response_info_pk" PRIMARY KEY (billing_response_info_id),
    CONSTRAINT "billing_accepted_status" CHECK (accepted_status::text = ANY (ARRAY['ACCEPTED'::character varying::text, 'ERROR'::character varying::text, 'INVALID'::character varying::text, 'DELETE'::character varying::text, 'HOLD'::character varying::text])),
    CONSTRAINT "billing_info_header_fk" FOREIGN KEY (billing_response_header_id) REFERENCES billing.billing_response_header(billing_response_header_id)
);

CREATE INDEX billing_record_id_idx ON billing.billing_response_info USING btree (billing_record_id);
CREATE INDEX ix_bri_billing_response_header_id ON billing.billing_response_info USING btree (billing_response_header_id);

CREATE TRIGGER "billing_response_info$audtrg" AFTER INSERT OR DELETE OR UPDATE ON billing.billing_response_info FOR EACH ROW EXECUTE FUNCTION billing."billing_response_info$audtrg$billing_response_info"();

COMMENT ON COLUMN "billing"."billing_response_info"."billing_response_info_id" IS 'Primary Key';
COMMENT ON COLUMN "billing"."billing_response_info"."billing_response_header_id" IS 'Foreign Key to the billing response header';
COMMENT ON COLUMN "billing"."billing_response_info"."billing_record_id" IS 'Uniquely identifies this bill detail record in the Pegasys Detail Billing Record table; this will reflect the value from the input file TLC + MMDDYYYY + #######';
COMMENT ON COLUMN "billing"."billing_response_info"."billing_response_info" IS 'The raw information response';
COMMENT ON COLUMN "billing"."billing_response_info"."accepted_status" IS 'Valid values: ACCEPTED=Pegasys verified ERROR=Pegasys rejected INVALID,DELETE=Do not send HOLD=temporarily hold to release later';

COMMENT ON TABLE "billing"."billing_response_info" IS 'This table logs the informational reponses from billing transmission.  How do we tie this record back to a BILLING entry, though?!';