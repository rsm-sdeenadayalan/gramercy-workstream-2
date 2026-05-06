-- CII — Compute Infrastructure Index (database: cii)
-- Apply with: psql -h localhost -p 5433 -U <user> -d cii -f cii_schema.sql

BEGIN;

-- ── COLLECTION LAYER ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cii_facilities (
    id                    SERIAL PRIMARY KEY,
    country_iso           CHAR(2)          NOT NULL,
    facility_name         VARCHAR(500)     NOT NULL,
    operator              VARCHAR(200)     NOT NULL,
    capacity_mw           DOUBLE PRECISION,
    status                VARCHAR(30)      NOT NULL
        CHECK (status IN ('operational','permitted','under_construction','announced')),
    date_announced        DATE,
    date_operational      DATE,
    investment_value_usd  DOUBLE PRECISION,
    energy_source         VARCHAR(200),
    chip_type_if_known    VARCHAR(200),
    ownership_type        VARCHAR(20)
        CHECK (ownership_type IN ('domestic','foreign','joint_venture','unknown')),
    is_hyperscaler        BOOLEAN          NOT NULL DEFAULT FALSE,
    has_estimated_fields  BOOLEAN          NOT NULL DEFAULT FALSE,
    confidence_score      DOUBLE PRECISION,
    source_urls           TEXT[],
    source_count          INTEGER          DEFAULT 0,
    first_seen_run_id     UUID,
    last_updated_run_id   UUID,
    collected_at          TIMESTAMP        DEFAULT NOW(),
    UNIQUE (country_iso, facility_name, operator)
);

CREATE TABLE IF NOT EXISTS cii_quarterly_snapshots (
    id                              SERIAL PRIMARY KEY,
    country_iso                     CHAR(2)          NOT NULL,
    quarter                         CHAR(6)          NOT NULL,
    quarter_end_date                DATE             NOT NULL,
    installed_mw                    DOUBLE PRECISION,
    committed_mw                    DOUBLE PRECISION,
    committed_usd                   DOUBLE PRECISION,
    pipeline_multiplier             DOUBLE PRECISION,
    hyperscaler_commitments_count   INTEGER,
    qoq_installed_growth_rate       DOUBLE PRECISION,
    qoq_committed_mw_growth_rate    DOUBLE PRECISION,
    qoq_committed_usd_growth_rate   DOUBLE PRECISION,
    national_grid_mw                DOUBLE PRECISION,
    grid_strain_ratio               DOUBLE PRECISION,
    run_id                          UUID,
    computed_at                     TIMESTAMP DEFAULT NOW(),
    UNIQUE (country_iso, quarter)
);

CREATE TABLE IF NOT EXISTS cii_chip_access (
    id              SERIAL PRIMARY KEY,
    country_iso     CHAR(2)      NOT NULL,
    tier            INTEGER      NOT NULL CHECK (tier BETWEEN 1 AND 5),
    tier_label      VARCHAR(100),
    policy_basis    TEXT,
    effective_date  DATE         NOT NULL,
    review_date     DATE,
    run_id          UUID,
    UNIQUE (country_iso, effective_date)
);

CREATE TABLE IF NOT EXISTS cii_grid_reference (
    id                  SERIAL PRIMARY KEY,
    country_iso         CHAR(2)          NOT NULL,
    grid_capacity_mw    DOUBLE PRECISION NOT NULL,
    data_year           INTEGER          NOT NULL,
    source_name         VARCHAR(200),
    source_url          TEXT,
    run_id              UUID,
    UNIQUE (country_iso, data_year)
);

CREATE TABLE IF NOT EXISTS cii_raw_metrics (
    id               SERIAL PRIMARY KEY,
    country_iso      CHAR(2)          NOT NULL,
    country_name     VARCHAR(100),
    sub_index        VARCHAR(10)      NOT NULL,
    metric_key       VARCHAR(100)     NOT NULL,
    metric_value     DOUBLE PRECISION,
    unit             VARCHAR(50),
    data_date        DATE,
    source_name      VARCHAR(200),
    source_url       TEXT,
    confidence_score DOUBLE PRECISION,
    run_id           UUID,
    collected_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (country_iso, metric_key, data_date)
);

-- ── SCORING LAYER ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cii_score_methodology (
    id          SERIAL PRIMARY KEY,
    sub_index   VARCHAR(10)      NOT NULL,
    metric_key  VARCHAR(100)     NOT NULL,
    weight      DOUBLE PRECISION NOT NULL,
    invert      BOOLEAN          NOT NULL DEFAULT FALSE,
    notes       TEXT,
    UNIQUE (sub_index, metric_key),
    CHECK (weight >= 0 AND weight <= 1)
);

CREATE TABLE IF NOT EXISTS cii_subindex_weights (
    sub_index   VARCHAR(10) PRIMARY KEY,
    weight      DOUBLE PRECISION NOT NULL,
    label       VARCHAR(100),
    CHECK (weight >= 0 AND weight <= 1)
);

CREATE TABLE IF NOT EXISTS cii_score_metric_inputs (
    id           SERIAL PRIMARY KEY,
    run_id       UUID,
    country_iso  CHAR(2)          NOT NULL,
    sub_index    VARCHAR(10)      NOT NULL,
    metric_key   VARCHAR(100)     NOT NULL,
    raw_value    DOUBLE PRECISION,
    unit         VARCHAR(50),
    data_date    DATE,
    confidence   DOUBLE PRECISION,
    pulled_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE (run_id, country_iso, sub_index, metric_key)
);

CREATE TABLE IF NOT EXISTS cii_score_metric_normalized (
    id             SERIAL PRIMARY KEY,
    run_id         UUID,
    country_iso    CHAR(2)          NOT NULL,
    sub_index      VARCHAR(10)      NOT NULL,
    metric_key     VARCHAR(100)     NOT NULL,
    raw_value      DOUBLE PRECISION,
    normalized     DOUBLE PRECISION NOT NULL,
    inverted       BOOLEAN          NOT NULL DEFAULT FALSE,
    weight         DOUBLE PRECISION NOT NULL,
    weighted_score DOUBLE PRECISION NOT NULL,
    computed_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE (run_id, country_iso, sub_index, metric_key)
);

CREATE TABLE IF NOT EXISTS cii_score_subindex (
    id             SERIAL PRIMARY KEY,
    run_id         UUID,
    country_iso    CHAR(2)          NOT NULL,
    sub_index      VARCHAR(10)      NOT NULL,
    score          DOUBLE PRECISION NOT NULL,
    weight         DOUBLE PRECISION NOT NULL,
    weighted_score DOUBLE PRECISION NOT NULL,
    data_date_min  DATE,
    data_date_max  DATE,
    computed_at    TIMESTAMP DEFAULT NOW(),
    UNIQUE (run_id, country_iso, sub_index)
);

CREATE TABLE IF NOT EXISTS cii_score_final (
    id            SERIAL PRIMARY KEY,
    run_id        UUID,
    country_iso   CHAR(2)          NOT NULL,
    si1_capacity  DOUBLE PRECISION,
    si2_velocity  DOUBLE PRECISION,
    si3_quality   DOUBLE PRECISION,
    cii_score     DOUBLE PRECISION NOT NULL,
    rank          INTEGER,
    data_date_min DATE,
    data_date_max DATE,
    computed_at   TIMESTAMP DEFAULT NOW(),
    UNIQUE (run_id, country_iso)
);

-- ── OUTPUT LAYER ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cii_sc_gap (
    id               SERIAL PRIMARY KEY,
    run_id           UUID,
    country_iso      CHAR(2)          NOT NULL,
    sdi_score        DOUBLE PRECISION,
    sdi_normalized   DOUBLE PRECISION,
    cii_score        DOUBLE PRECISION,
    cii_normalized   DOUBLE PRECISION,
    sc_gap           DOUBLE PRECISION,
    interpretation   VARCHAR(30)
        CHECK (interpretation IN ('under_converting','near_parity','over_converting')),
    computed_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE (run_id, country_iso)
);

-- ── OPERATIONAL LAYER ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cii_collection_runs (
    run_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    started_at            TIMESTAMP DEFAULT NOW(),
    finished_at           TIMESTAMP,
    status                VARCHAR(20) DEFAULT 'running'
        CHECK (status IN ('running','completed','partial','failed')),
    facilities_discovered INTEGER DEFAULT 0,
    facilities_enriched   INTEGER DEFAULT 0,
    tavily_calls_used     INTEGER DEFAULT 0,
    notes                 TEXT
);

CREATE TABLE IF NOT EXISTS cii_collection_log (
    id            SERIAL PRIMARY KEY,
    run_id        UUID REFERENCES cii_collection_runs(run_id),
    country_iso   CHAR(2),
    facility_name VARCHAR(500),
    pass_type     VARCHAR(20)
        CHECK (pass_type IN ('discovery','enrichment','validation','si3','grid_ref','scoring')),
    query         TEXT,
    source_url    TEXT,
    status        VARCHAR(20)
        CHECK (status IN ('success','failed','skipped','gap')),
    confidence    DOUBLE PRECISION,
    elapsed_ms    INTEGER,
    error_msg     TEXT,
    logged_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cii_data_gaps (
    id                      SERIAL PRIMARY KEY,
    country_iso             CHAR(2),
    metric_key              VARCHAR(100),
    facility_name           VARCHAR(500),
    gap_type                VARCHAR(30)
        CHECK (gap_type IN ('facility_gap','field_gap','metric_gap','structural_unknown','structural_unobservable')),
    failure_reason          TEXT,
    collectors_tried        TEXT[],
    severity                VARCHAR(20)
        CHECK (severity IN ('critical','high','medium','low','structural')),
    manual_review_required  BOOLEAN DEFAULT FALSE,
    recommended_action      TEXT,
    first_detected          TIMESTAMP DEFAULT NOW(),
    last_attempted          TIMESTAMP DEFAULT NOW(),
    attempt_count           INTEGER DEFAULT 1,
    status                  VARCHAR(20) DEFAULT 'open',
    UNIQUE (country_iso, metric_key, facility_name)
);

-- ── VIEWS ────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW v_cii_facilities_summary AS
SELECT
    country_iso,
    COUNT(*)                                                              AS total_facilities,
    SUM(CASE WHEN status = 'operational' THEN capacity_mw ELSE 0 END)    AS installed_mw,
    SUM(CASE WHEN status != 'operational' THEN capacity_mw ELSE 0 END)   AS committed_mw,
    SUM(CASE WHEN status != 'operational' AND is_hyperscaler
              THEN investment_value_usd ELSE 0 END)                       AS committed_usd,
    ROUND(AVG(confidence_score)::numeric, 3)                             AS avg_confidence
FROM cii_facilities
GROUP BY country_iso;

CREATE OR REPLACE VIEW v_cii_latest_scores AS
SELECT
    f.country_iso,
    ROUND(f.si1_capacity::numeric, 2) AS capacity_score,
    ROUND(f.si2_velocity::numeric, 2) AS velocity_score,
    ROUND(f.si3_quality::numeric, 2)  AS quality_score,
    ROUND(f.cii_score::numeric, 2)    AS cii,
    f.rank,
    f.data_date_min AS as_of_oldest,
    f.data_date_max AS as_of_newest,
    f.computed_at
FROM cii_score_final f
WHERE f.run_id = (SELECT run_id FROM cii_collection_runs
                  WHERE status = 'completed' ORDER BY finished_at DESC LIMIT 1)
ORDER BY f.cii_score DESC NULLS LAST;

CREATE OR REPLACE VIEW v_cii_sc_gap_ranked AS
SELECT
    country_iso,
    ROUND(sdi_normalized::numeric, 3) AS sdi,
    ROUND(cii_normalized::numeric, 3) AS cii,
    ROUND(sc_gap::numeric, 3)         AS sc_gap,
    interpretation,
    RANK() OVER (ORDER BY sc_gap DESC) AS rank_under_converting,
    computed_at
FROM cii_sc_gap
WHERE run_id = (SELECT run_id FROM cii_collection_runs
                WHERE status = 'completed' ORDER BY finished_at DESC LIMIT 1)
ORDER BY sc_gap DESC;

-- ── SEED DATA ────────────────────────────────────────────────────

INSERT INTO cii_score_methodology (sub_index, metric_key, weight, invert, notes) VALUES
    ('SI1', 'installed_capacity_mw',         0.40, FALSE, 'Operational DC capacity'),
    ('SI1', 'committed_pipeline_mw',         0.40, FALSE, 'Permitted+under_construction+announced'),
    ('SI1', 'pipeline_multiplier',           0.20, FALSE, 'committed / installed; NULL when installed=0'),
    ('SI2', 'qoq_installed_growth_rate',     0.40, FALSE, 'QoQ growth of operational capacity'),
    ('SI2', 'qoq_committed_mw_growth_rate',  0.20, FALSE, 'QoQ growth of committed pipeline MW'),
    ('SI2', 'qoq_committed_usd_growth_rate', 0.10, FALSE, 'QoQ growth of committed pipeline USD'),
    ('SI2', 'new_hyperscaler_commitments',   0.20, FALSE, 'New hyperscaler announcements per quarter'),
    ('SI2', 'grid_strain_ratio',             0.10, TRUE,  'Inverted: higher strain = lower score'),
    ('SI3', 'chip_access_tier',              0.40, FALSE, '1=unrestricted latest-gen, 5=denied'),
    ('SI3', 'hyperscaler_count',             0.20, FALSE, 'Distinct hyperscaler operators'),
    ('SI3', 'hyperscaler_investment_usd',    0.15, FALSE, 'Total hyperscaler committed investment'),
    ('SI3', 'domestic_ownership_ratio',      0.15, FALSE, 'Domestic capacity / total capacity'),
    ('SI3', 'frontier_training_present',     0.10, FALSE, '1 if confirmed in-jurisdiction training')
ON CONFLICT (sub_index, metric_key) DO UPDATE SET
    weight = EXCLUDED.weight, invert = EXCLUDED.invert, notes = EXCLUDED.notes;

INSERT INTO cii_subindex_weights (sub_index, weight, label) VALUES
    ('SI1', 0.40, 'Installed and Committed Capacity'),
    ('SI2', 0.35, 'Growth Velocity'),
    ('SI3', 0.25, 'Compute Quality and Access')
ON CONFLICT (sub_index) DO UPDATE SET weight = EXCLUDED.weight, label = EXCLUDED.label;

INSERT INTO cii_chip_access (country_iso, tier, tier_label, policy_basis, effective_date, review_date) VALUES
    ('US', 1, 'unrestricted_latest_gen',  'Domestic producer — no export restrictions apply',                           '2024-01-01', '2027-01-01'),
    ('SG', 2, 'unrestricted_prior_gen',   'BIS Tier 1 ally — H100/H200 unrestricted, B200 under review',               '2024-10-07', '2026-10-07'),
    ('IN', 2, 'unrestricted_prior_gen',   'BIS Tier 2 — approved up to H100 class',                                    '2024-10-07', '2026-10-07'),
    ('BR', 2, 'unrestricted_prior_gen',   'BIS Tier 2 — approved up to H100 class',                                    '2024-10-07', '2026-10-07'),
    ('PH', 2, 'unrestricted_prior_gen',   'BIS Tier 2 — approved up to H100 class',                                    '2024-10-07', '2026-10-07'),
    ('AE', 3, 'restricted',               'BIS Entity List concern; NVIDIA-Microsoft Emirati AI deal 2025 conditional', '2025-01-01', '2025-10-01')
ON CONFLICT (country_iso, effective_date) DO UPDATE SET
    tier = EXCLUDED.tier, tier_label = EXCLUDED.tier_label,
    policy_basis = EXCLUDED.policy_basis, review_date = EXCLUDED.review_date;

COMMIT;
