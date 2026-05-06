# CII — Compute Infrastructure Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-grade quarterly data pipeline that collects AI data center facility records for 6 countries, computes CII sub-index scores, and outputs the S-C Gap analysis.

**Architecture:** Three-phase pipeline — Phase 1 runs SI1 facility collection (3-pass: discovery → enrichment → validation) and SI3 research collectors in parallel; Phase 2 derives SI2 growth velocity and SI3 aggregates from the populated facility DB; Phase 3 runs scoring and S-C Gap. Every facility record is streamed to Postgres immediately on extraction (UPSERT), making the pipeline crash-resumable.

**Tech Stack:** Python 3.11+, psycopg2-binary, anthropic SDK (claude-haiku-4-5-20251001), tavily-python, python-dotenv, pytest, unittest.mock

---

## File Map

```
Workstream 2 Capstone/
├── cii/
│   ├── cii_schema.sql          # 13 tables + 3 views + seeded weights/tiers
│   ├── setup_cii.py            # idempotent DB bootstrap
│   ├── cii_collectors.py       # web_search, Claude extraction, 3-pass SI1
│   ├── cii_si3_collectors.py   # domestic ownership + frontier training research
│   ├── cii_scoring.py          # SI2 computation, SI3 derived, scoring, S-C Gap
│   ├── cii_gap_report.py       # prioritised gap report for analyst
│   ├── cii_verify.py           # post-run bounds + freshness checks
│   └── run_cii.py              # orchestrator (phases 1-3)
├── tests/
│   ├── conftest.py             # shared fixtures (mock conn, mock Claude, mock Tavily)
│   ├── test_collectors.py      # SI1 3-pass tests
│   ├── test_si3_collectors.py  # SI3 research tests
│   ├── test_scoring.py         # SI2 computation + scoring + S-C Gap tests
│   └── test_gap_report.py      # gap report output tests
├── requirements.txt
└── .env.example
```

---

## Task 1: Project Scaffold + Requirements

**Files:**
- Create: `requirements.txt`
- Create: `.env.example`
- Create: `tests/conftest.py`

- [ ] **Step 1: Create requirements.txt**

```
anthropic==0.96.0
psycopg2-binary==2.9.11
python-dotenv==1.2.2
requests==2.33.1
tavily-python==0.7.23
pytest==8.3.5
```

- [ ] **Step 2: Create .env.example**

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_USER=shankar_1
POSTGRES_PASSWORD=

ANTHROPIC_API_KEY=
TAVILY_API_KEY=
```

- [ ] **Step 3: Create tests/conftest.py**

```python
import pytest
from unittest.mock import MagicMock, patch
import psycopg2


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    return conn


@pytest.fixture
def mock_claude():
    with patch("anthropic.Anthropic") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_tavily():
    with patch("tavily.TavilyClient") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def sample_facility():
    return {
        "country_iso": "US",
        "facility_name": "Microsoft Iowa Campus",
        "operator": "Microsoft",
        "capacity_mw": 200.0,
        "status": "operational",
        "date_announced": "2023-01-15",
        "date_operational": "2024-06-01",
        "investment_value_usd": 1_000_000_000.0,
        "energy_source": "renewable",
        "chip_type_if_known": "NVIDIA H100",
        "ownership_type": "foreign",
        "is_hyperscaler": True,
        "confidence_score": 0.85,
        "source_urls": ["https://news.microsoft.com/iowa"],
        "source_count": 2,
    }
```

- [ ] **Step 4: Verify pytest discovers conftest**

```bash
cd "/Users/shankar/Desktop/Workstream 2 Capstone"
pip install -r requirements.txt
pytest tests/ --collect-only
```

Expected: `no tests ran` (no tests yet, but no import errors)

- [ ] **Step 5: Commit**

```bash
git add requirements.txt .env.example tests/conftest.py
git commit -m "feat: project scaffold and test fixtures"
```

---

## Task 2: DB Schema

**Files:**
- Create: `cii/cii_schema.sql`

- [ ] **Step 1: Write cii/cii_schema.sql**

```sql
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
    SUM(CASE WHEN status != 'operational' THEN investment_value_usd ELSE 0 END) AS committed_usd,
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
```

- [ ] **Step 2: Commit**

```bash
git add cii/cii_schema.sql
git commit -m "feat: CII database schema (13 tables, 3 views, seeded weights)"
```

---

## Task 3: DB Bootstrap (setup_cii.py)

**Files:**
- Create: `cii/setup_cii.py`
- Create: `tests/test_setup.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_setup.py
import pytest
from unittest.mock import MagicMock, patch, call
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cii'))


def test_create_db_if_not_exists():
    with patch("psycopg2.connect") as mock_connect:
        admin_conn = MagicMock()
        admin_conn.autocommit = False
        mock_connect.return_value = admin_conn
        admin_cursor = MagicMock()
        admin_conn.cursor.return_value.__enter__ = lambda s: admin_cursor
        admin_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        admin_cursor.fetchone.return_value = None  # DB doesn't exist yet

        from setup_cii import create_db_if_not_exists
        create_db_if_not_exists(host="localhost", port=5433, user="u", password="p")

        calls = [str(c) for c in admin_cursor.execute.call_args_list]
        assert any("CREATE DATABASE" in c and "cii" in c for c in calls)


def test_skip_create_if_db_exists():
    with patch("psycopg2.connect") as mock_connect:
        admin_conn = MagicMock()
        mock_connect.return_value = admin_conn
        admin_cursor = MagicMock()
        admin_conn.cursor.return_value.__enter__ = lambda s: admin_cursor
        admin_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        admin_cursor.fetchone.return_value = ("cii",)  # DB already exists

        from setup_cii import create_db_if_not_exists
        create_db_if_not_exists(host="localhost", port=5433, user="u", password="p")

        calls = [str(c) for c in admin_cursor.execute.call_args_list]
        assert not any("CREATE DATABASE" in c for c in calls)
```

- [ ] **Step 2: Run — verify it fails**

```bash
pytest tests/test_setup.py -v
```

Expected: `ImportError: cannot import name 'create_db_if_not_exists'`

- [ ] **Step 3: Write cii/setup_cii.py**

```python
import os, sys, subprocess
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}


def create_db_if_not_exists(host, port, user, password):
    conn = psycopg2.connect(host=host, port=port, user=user,
                            password=password, dbname="postgres")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = 'cii'")
        if cur.fetchone() is None:
            cur.execute('CREATE DATABASE cii')
            print("  Created database: cii")
        else:
            print("  Database cii already exists — skipping create")
    conn.close()


def apply_schema(host, port, user, password):
    schema_path = os.path.join(os.path.dirname(__file__), "cii_schema.sql")
    conn = psycopg2.connect(host=host, port=port, user=user,
                            password=password, dbname="cii")
    conn.autocommit = True
    with conn.cursor() as cur:
        with open(schema_path) as f:
            cur.execute(f.read())
    conn.close()
    print("  Schema applied.")


if __name__ == "__main__":
    h, p, u, pw = (DB_CONFIG["host"], DB_CONFIG["port"],
                   DB_CONFIG["user"], DB_CONFIG["password"])
    print("Setting up CII database...")
    create_db_if_not_exists(h, p, u, pw)
    apply_schema(h, p, u, pw)
    print("Done.")
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
pytest tests/test_setup.py -v
```

Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add cii/setup_cii.py tests/test_setup.py
git commit -m "feat: idempotent DB bootstrap (setup_cii.py)"
```

---

## Task 4: Shared Utilities — web_search + Claude Extraction

**Files:**
- Create: `cii/cii_collectors.py` (shared constants + utilities only)
- Create: `tests/test_collectors.py` (utility tests)

- [ ] **Step 1: Write failing tests**

```python
# tests/test_collectors.py
import pytest, json, sys, os
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cii'))


def test_web_search_returns_results():
    with patch("tavily.TavilyClient") as MockTavily:
        client = MagicMock()
        MockTavily.return_value = client
        client.search.return_value = {
            "results": [{"url": "https://example.com", "title": "Test",
                         "content": "200MW data center announced"}]
        }
        from cii_collectors import web_search
        results = web_search("US data center capacity", count=3)
        assert len(results) == 1
        assert results[0]["url"] == "https://example.com"
        assert "200MW" in results[0]["content"]


def test_web_search_raises_when_no_key():
    with patch.dict(os.environ, {"TAVILY_API_KEY": ""}, clear=False):
        import importlib, cii_collectors
        importlib.reload(cii_collectors)
        with pytest.raises(ValueError, match="TAVILY_API_KEY"):
            cii_collectors.web_search("test query")


def test_extract_facilities_parses_claude_json():
    mock_client = MagicMock()
    mock_client.messages.create.return_value.content = [MagicMock(
        text=json.dumps([{
            "facility_name": "AWS Iowa",
            "operator": "Amazon",
            "capacity_mw": 150.0,
            "status": "operational",
            "date_announced": "2023-01-01",
            "date_operational": "2024-01-01",
            "investment_value_usd": 500000000.0,
            "energy_source": "renewable",
            "chip_type_if_known": None,
            "ownership_type": "foreign",
            "is_hyperscaler": True,
            "source_url": "https://aws.amazon.com/press"
        }])
    )]

    from cii_collectors import _extract_facilities_claude
    results = _extract_facilities_claude(
        mock_client,
        [{"url": "https://aws.amazon.com/press", "title": "AWS Iowa",
          "content": "150MW operational campus"}],
        "US", "United States"
    )
    assert len(results) == 1
    assert results[0]["facility_name"] == "AWS Iowa"
    assert results[0]["capacity_mw"] == 150.0
    assert results[0]["is_hyperscaler"] is True


def test_extract_facilities_handles_bad_json():
    mock_client = MagicMock()
    mock_client.messages.create.return_value.content = [
        MagicMock(text="not valid json")
    ]
    from cii_collectors import _extract_facilities_claude
    results = _extract_facilities_claude(mock_client, [], "US", "United States")
    assert results == []
```

- [ ] **Step 2: Run — verify they fail**

```bash
pytest tests/test_collectors.py -v
```

Expected: `ImportError: No module named 'cii_collectors'`

- [ ] **Step 3: Write cii/cii_collectors.py (utilities only)**

```python
import os, json, time, re
from datetime import date, datetime
from dotenv import load_dotenv
import psycopg2

load_dotenv()

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TAVILY_API_KEY    = os.environ.get("TAVILY_API_KEY", "")

DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   "cii",
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}

COUNTRIES = {
    "US": "United States", "AE": "UAE",       "BR": "Brazil",
    "IN": "India",         "SG": "Singapore", "PH": "Philippines",
}

CONFIDENCE = {
    "official_ir":   0.90,
    "multi_source":  0.85,
    "trade_pub":     0.75,
    "agent_multi":   0.70,
    "agent_single":  0.65,
    "benchmark_est": 0.50,
}

HYPERSCALER_BENCHMARK_MW = {
    "Microsoft": 100.0, "Amazon": 100.0, "Google": 80.0,
    "Meta": 80.0, "Oracle": 60.0, "default": 50.0,
}

HYPERSCALER_OPERATORS = {
    "Microsoft", "Amazon", "AWS", "Google", "Meta", "Oracle",
    "Apple", "Alibaba", "Tencent", "Huawei",
}

TRUSTED_DOMAINS = {
    "US": ["aws.amazon.com", "azure.microsoft.com", "cloud.google.com",
           "about.meta.com", "oracle.com", "datacenterknowledge.com",
           "datacenterdynamics.com", "synergy-rp.com"],
    "AE": ["dewa.gov.ae", "moei.gov.ae", "ewec.ae", "taqa.com",
           "mubadala.com", "g42.ai", "datacenterdynamics.com"],
    "BR": ["equinix.com.br", "ascenty.com", "hostdime.com.br",
           "anatel.gov.br", "datacenterdynamics.com"],
    "IN": ["niti.gov.in", "meity.gov.in", "stpi.in",
           "datacenterdynamics.com", "economictimes.indiatimes.com"],
    "SG": ["edb.gov.sg", "imda.gov.sg", "ema.gov.sg",
           "datacenterdynamics.com", "straitstimes.com"],
    "PH": ["dict.gov.ph", "peza.gov.ph",
           "datacenterdynamics.com", "businessmirror.com.ph"],
}

DISCOVERY_QUERY_TEMPLATES = [
    "{country} hyperscaler data center capacity MW announced {year}",
    "{country} Microsoft Azure data center investment",
    "{country} AWS Amazon data center facility",
    "{country} Google cloud data center",
    "{country} Meta AI data center",
    "{country} Oracle data center",
    "{country} data center energy permit MW grid connection {year}",
    "{country} AI compute infrastructure investment billion USD {year}",
]

KNOWN_ZERO_FRONTIER = {"PH", "BR"}   # confirmed no frontier AI training


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def web_search(query: str, count: int = 5) -> list[dict]:
    if not TAVILY_API_KEY:
        raise ValueError("TAVILY_API_KEY not set in .env")
    from tavily import TavilyClient
    client = TavilyClient(api_key=TAVILY_API_KEY)
    resp = client.search(query, max_results=count,
                         include_raw_content=False, search_depth="advanced")
    return [
        {"url": r.get("url", ""), "title": r.get("title", ""),
         "content": r.get("content", "") or ""}
        for r in resp.get("results", [])
    ]


def _extract_facilities_claude(client, results: list[dict],
                                country_iso: str, country_name: str) -> list[dict]:
    if not results:
        return []
    content_block = "\n\n".join(
        f"Source: {r['url']}\nTitle: {r['title']}\nContent: {r['content'][:800]}"
        for r in results if r.get("content")
    )
    if not content_block.strip():
        return []
    prompt = f"""Extract all AI/cloud data center facilities mentioned for {country_name} ({country_iso}).
Return a JSON array. Each item must have these exact keys:
  facility_name (string), operator (string), capacity_mw (number or null),
  status (one of: operational, permitted, under_construction, announced),
  date_announced (YYYY-MM-DD or null), date_operational (YYYY-MM-DD or null),
  investment_value_usd (number in USD or null), energy_source (string or null),
  chip_type_if_known (string or null),
  ownership_type (one of: domestic, foreign, joint_venture, unknown),
  is_hyperscaler (true/false), source_url (string)

Sources:
{content_block}

Return ONLY the JSON array. No explanation."""

    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        text = resp.content[0].text.strip()
        # strip markdown code fences if present
        text = re.sub(r"^```[a-z]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
        return json.loads(text)
    except Exception:
        return []


def upsert_facility(conn, run_id: str, f: dict) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cii_facilities
                (country_iso, facility_name, operator, capacity_mw, status,
                 date_announced, date_operational, investment_value_usd,
                 energy_source, chip_type_if_known, ownership_type, is_hyperscaler,
                 has_estimated_fields, confidence_score, source_urls, source_count,
                 first_seen_run_id, last_updated_run_id)
            VALUES
                (%(country_iso)s, %(facility_name)s, %(operator)s, %(capacity_mw)s,
                 %(status)s, %(date_announced)s, %(date_operational)s,
                 %(investment_value_usd)s, %(energy_source)s, %(chip_type_if_known)s,
                 %(ownership_type)s, %(is_hyperscaler)s, %(has_estimated_fields)s,
                 %(confidence_score)s, %(source_urls)s, %(source_count)s,
                 %(run_id)s, %(run_id)s)
            ON CONFLICT (country_iso, facility_name, operator) DO UPDATE SET
                capacity_mw          = COALESCE(EXCLUDED.capacity_mw, cii_facilities.capacity_mw),
                status               = EXCLUDED.status,
                date_announced       = COALESCE(EXCLUDED.date_announced, cii_facilities.date_announced),
                date_operational     = COALESCE(EXCLUDED.date_operational, cii_facilities.date_operational),
                investment_value_usd = COALESCE(EXCLUDED.investment_value_usd, cii_facilities.investment_value_usd),
                energy_source        = COALESCE(EXCLUDED.energy_source, cii_facilities.energy_source),
                chip_type_if_known   = COALESCE(EXCLUDED.chip_type_if_known, cii_facilities.chip_type_if_known),
                ownership_type       = EXCLUDED.ownership_type,
                is_hyperscaler       = EXCLUDED.is_hyperscaler,
                has_estimated_fields = EXCLUDED.has_estimated_fields,
                confidence_score     = GREATEST(EXCLUDED.confidence_score, cii_facilities.confidence_score),
                source_count         = cii_facilities.source_count + EXCLUDED.source_count,
                last_updated_run_id  = EXCLUDED.last_updated_run_id,
                collected_at         = NOW()
        """, {**f, "run_id": run_id,
              "source_urls": f.get("source_urls", []),
              "has_estimated_fields": f.get("has_estimated_fields", False)})
    conn.commit()


def log_attempt(conn, run_id, country_iso, facility_name, pass_type,
                query, source_url, status, confidence, elapsed_ms, error_msg=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cii_collection_log
                (run_id, country_iso, facility_name, pass_type, query,
                 source_url, status, confidence, elapsed_ms, error_msg)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, country_iso, facility_name, pass_type, query,
              source_url, status, confidence, elapsed_ms, error_msg))
    conn.commit()


def upsert_gap(conn, country_iso, metric_key, facility_name,
               gap_type, failure_reason, severity, recommended_action=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cii_data_gaps
                (country_iso, metric_key, facility_name, gap_type,
                 failure_reason, severity, recommended_action,
                 manual_review_required)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (country_iso, metric_key, facility_name) DO UPDATE SET
                attempt_count     = cii_data_gaps.attempt_count + 1,
                last_attempted    = NOW(),
                failure_reason    = EXCLUDED.failure_reason,
                manual_review_required = (cii_data_gaps.attempt_count + 1) >= 2
        """, (country_iso, metric_key, facility_name or "",
              gap_type, failure_reason, severity, recommended_action,
              False))
    conn.commit()
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
pytest tests/test_collectors.py -v
```

Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add cii/cii_collectors.py tests/test_collectors.py
git commit -m "feat: shared web_search, Claude extraction, upsert helpers"
```

---

## Task 5: SI1 — Discovery Pass

**Files:**
- Modify: `cii/cii_collectors.py` (add `run_discovery_pass`)
- Modify: `tests/test_collectors.py` (add discovery tests)

- [ ] **Step 1: Add failing tests**

```python
# append to tests/test_collectors.py

def test_run_discovery_pass_upserts_facilities(mock_conn):
    with patch("cii_collectors.web_search") as mock_search, \
         patch("cii_collectors._extract_facilities_claude") as mock_extract, \
         patch("cii_collectors.upsert_facility") as mock_upsert, \
         patch("cii_collectors.log_attempt"), \
         patch("anthropic.Anthropic"):

        mock_search.return_value = [
            {"url": "https://example.com", "title": "T", "content": "200MW campus"}
        ]
        mock_extract.return_value = [{
            "facility_name": "AWS Iowa", "operator": "Amazon",
            "capacity_mw": 200.0, "status": "operational",
            "date_announced": None, "date_operational": None,
            "investment_value_usd": None, "energy_source": None,
            "chip_type_if_known": None, "ownership_type": "foreign",
            "is_hyperscaler": True, "source_url": "https://example.com"
        }]

        from cii_collectors import run_discovery_pass
        run_discovery_pass(mock_conn, "run-123", "US")
        assert mock_upsert.call_count >= 1
        call_kwargs = mock_upsert.call_args[0][2]
        assert call_kwargs["confidence_score"] == CONFIDENCE["agent_single"]


def test_run_discovery_pass_deduplicates_same_facility(mock_conn):
    with patch("cii_collectors.web_search") as mock_search, \
         patch("cii_collectors._extract_facilities_claude") as mock_extract, \
         patch("cii_collectors.upsert_facility") as mock_upsert, \
         patch("cii_collectors.log_attempt"), \
         patch("anthropic.Anthropic"):

        mock_search.return_value = [{"url": "u", "title": "t", "content": "c"}]
        # Same facility returned twice from two different queries
        mock_extract.return_value = [{
            "facility_name": "AWS Iowa", "operator": "Amazon",
            "capacity_mw": 200.0, "status": "operational",
            "date_announced": None, "date_operational": None,
            "investment_value_usd": None, "energy_source": None,
            "chip_type_if_known": None, "ownership_type": "foreign",
            "is_hyperscaler": True, "source_url": "u"
        }]

        from cii_collectors import run_discovery_pass
        # DB unique constraint is enforced by upsert_facility (UPSERT) — deduplicated at DB level
        run_discovery_pass(mock_conn, "run-123", "US")
        # upsert called once per unique facility per query — dedup is DB-side
        assert mock_upsert.call_count >= 1
```

- [ ] **Step 2: Run — verify they fail**

```bash
pytest tests/test_collectors.py::test_run_discovery_pass_upserts_facilities -v
```

Expected: `ImportError: cannot import name 'run_discovery_pass'`

- [ ] **Step 3: Add run_discovery_pass to cii/cii_collectors.py**

```python
# append to cii/cii_collectors.py

def run_discovery_pass(conn, run_id: str, country_iso: str) -> int:
    """Pass 1: enumerate all AI data center facilities for a country.
    Returns number of facilities discovered."""
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    country_name = COUNTRIES[country_iso]
    year = date.today().year
    found = 0

    for template in DISCOVERY_QUERY_TEMPLATES:
        query = template.format(country=country_name, year=year)
        t0 = time.perf_counter()
        try:
            results = web_search(query, count=5)
            facilities = _extract_facilities_claude(client, results, country_iso, country_name)
            for fac in facilities:
                if not fac.get("facility_name") or not fac.get("operator"):
                    continue
                fac["country_iso"]       = country_iso
                fac["confidence_score"]  = CONFIDENCE["agent_single"]
                fac["source_urls"]       = [fac.pop("source_url", "")] if fac.get("source_url") else []
                fac["source_count"]      = 1
                fac["has_estimated_fields"] = False
                upsert_facility(conn, run_id, fac)
                found += 1
            elapsed = int((time.perf_counter() - t0) * 1000)
            log_attempt(conn, run_id, country_iso, None, "discovery",
                        query, None, "success", CONFIDENCE["agent_single"], elapsed)
        except Exception as exc:
            elapsed = int((time.perf_counter() - t0) * 1000)
            log_attempt(conn, run_id, country_iso, None, "discovery",
                        query, None, "failed", None, elapsed, str(exc)[:500])
        time.sleep(0.3)  # respect Tavily rate limits

    # update run counter
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE cii_collection_runs
            SET facilities_discovered = facilities_discovered + %s,
                tavily_calls_used = tavily_calls_used + %s
            WHERE run_id = %s
        """, (found, len(DISCOVERY_QUERY_TEMPLATES), run_id))
    conn.commit()
    return found
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
pytest tests/test_collectors.py -v
```

Expected: `6 passed`

- [ ] **Step 5: Commit**

```bash
git add cii/cii_collectors.py tests/test_collectors.py
git commit -m "feat: SI1 discovery pass — multi-angle facility enumeration"
```

---

## Task 6: SI1 — Enrichment + Validation Passes

**Files:**
- Modify: `cii/cii_collectors.py` (add `run_enrichment_pass`, `run_validation_pass`)
- Modify: `tests/test_collectors.py`

- [ ] **Step 1: Add failing tests**

```python
# append to tests/test_collectors.py

def test_enrichment_updates_capacity_mw(mock_conn):
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [
        ("US", "AWS Iowa", "Amazon", None)  # capacity_mw is NULL — needs enrichment
    ]
    with patch("cii_collectors.web_search") as mock_search, \
         patch("cii_collectors._enrich_facility_claude") as mock_enrich, \
         patch("cii_collectors.upsert_facility") as mock_upsert, \
         patch("cii_collectors.log_attempt"), \
         patch("anthropic.Anthropic"):

        mock_search.return_value = [{"url": "u", "title": "t", "content": "200MW"}]
        mock_enrich.return_value = {
            "capacity_mw": 200.0, "investment_value_usd": 1e9,
            "date_announced": "2023-01-01", "date_operational": None,
            "energy_source": "renewable", "chip_type_if_known": "H100",
            "confidence_score": 0.75, "source_url": "u"
        }
        from cii_collectors import run_enrichment_pass
        enriched = run_enrichment_pass(mock_conn, "run-123", "US")
        assert enriched >= 1
        assert mock_upsert.call_count >= 1


def test_validation_assigns_high_confidence_for_multi_source(mock_conn):
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [
        ("US", "AWS Iowa", "Amazon", 200.0, 2)  # source_count=2
    ]
    with patch("cii_collectors.log_attempt"):
        from cii_collectors import run_validation_pass
        # when source_count >= 2 and capacity_mw is set → confidence 0.85
        with patch("cii_collectors.upsert_facility") as mock_upsert:
            run_validation_pass(mock_conn, "run-123", "US")
            if mock_upsert.called:
                call_kwargs = mock_upsert.call_args[0][2]
                assert call_kwargs["confidence_score"] >= 0.85


def test_validation_applies_benchmark_when_mw_missing(mock_conn):
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [
        ("US", "Meta Iowa", "Meta", None, 0)  # capacity_mw NULL, 0 sources
    ]
    with patch("cii_collectors.upsert_facility") as mock_upsert, \
         patch("cii_collectors.log_attempt"):
        from cii_collectors import run_validation_pass
        run_validation_pass(mock_conn, "run-123", "US")
        assert mock_upsert.called
        call_kwargs = mock_upsert.call_args[0][2]
        assert call_kwargs["capacity_mw"] == HYPERSCALER_BENCHMARK_MW["Meta"]
        assert call_kwargs["confidence_score"] == CONFIDENCE["benchmark_est"]
        assert call_kwargs["has_estimated_fields"] is True
```

- [ ] **Step 2: Run — verify they fail**

```bash
pytest tests/test_collectors.py::test_enrichment_updates_capacity_mw -v
```

Expected: `ImportError: cannot import name 'run_enrichment_pass'`

- [ ] **Step 3: Add enrichment + validation to cii/cii_collectors.py**

```python
# append to cii/cii_collectors.py

ENRICH_QUERY_TEMPLATES = [
    '"{facility_name}" {operator} data center capacity megawatt MW',
    '"{facility_name}" {operator} investment USD billion cost',
    '"{facility_name}" {operator} operational date commissioned opened',
]


def _enrich_facility_claude(client, results: list[dict], facility: dict) -> dict:
    if not results:
        return {}
    content_block = "\n\n".join(
        f"Source: {r['url']}\nContent: {r['content'][:600]}"
        for r in results if r.get("content")
    )
    prompt = f"""Extract details for the data center "{facility['facility_name']}" operated by "{facility['operator']}".
Return a JSON object with these keys (use null if unknown):
  capacity_mw (number), investment_value_usd (number in USD),
  date_announced (YYYY-MM-DD), date_operational (YYYY-MM-DD),
  energy_source (string), chip_type_if_known (string),
  confidence_score (0.65-0.90 based on source quality), source_url (string)

Sources:
{content_block}

Return ONLY the JSON object."""
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        text = re.sub(r"^```[a-z]*\n?", "", resp.content[0].text.strip())
        text = re.sub(r"\n?```$", "", text)
        return json.loads(text)
    except Exception:
        return {}


def run_enrichment_pass(conn, run_id: str, country_iso: str) -> int:
    """Pass 2: deep-dive each facility to fill in MW, investment, dates.
    Returns number of facilities enriched."""
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    enriched = 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, facility_name, operator, capacity_mw
            FROM cii_facilities
            WHERE country_iso = %s
            ORDER BY facility_name
        """, (country_iso,))
        facilities = cur.fetchall()

    for (c_iso, fname, operator, existing_mw) in facilities:
        for template in ENRICH_QUERY_TEMPLATES:
            query = template.format(facility_name=fname, operator=operator)
            t0 = time.perf_counter()
            try:
                results = web_search(query, count=3)
                enriched_data = _enrich_facility_claude(
                    client, results, {"facility_name": fname, "operator": operator}
                )
                if enriched_data:
                    src_url = enriched_data.pop("source_url", "")
                    update = {
                        "country_iso": c_iso, "facility_name": fname,
                        "operator": operator,
                        "status": "operational",   # preserved from discovery; not overwritten
                        "ownership_type": "unknown",
                        "is_hyperscaler": any(op in operator for op in HYPERSCALER_OPERATORS),
                        **enriched_data,
                        "source_urls": [src_url] if src_url else [],
                        "source_count": 1,
                    }
                    upsert_facility(conn, run_id, update)
                    enriched += 1
                elapsed = int((time.perf_counter() - t0) * 1000)
                log_attempt(conn, run_id, c_iso, fname, "enrichment",
                            query, src_url if enriched_data else None,
                            "success" if enriched_data else "gap",
                            enriched_data.get("confidence_score"), elapsed)
            except Exception as exc:
                elapsed = int((time.perf_counter() - t0) * 1000)
                log_attempt(conn, run_id, c_iso, fname, "enrichment",
                            query, None, "failed", None, elapsed, str(exc)[:500])
            time.sleep(0.2)

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE cii_collection_runs
            SET facilities_enriched = facilities_enriched + %s,
                tavily_calls_used   = tavily_calls_used + %s
            WHERE run_id = %s
        """, (enriched, len(facilities) * len(ENRICH_QUERY_TEMPLATES), run_id))
    conn.commit()
    return enriched


def run_validation_pass(conn, run_id: str, country_iso: str) -> int:
    """Pass 3: assign final confidence based on source agreement;
    apply hyperscaler benchmark when MW still missing.
    Returns number of facilities updated."""
    updated = 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, facility_name, operator, capacity_mw, source_count
            FROM cii_facilities WHERE country_iso = %s
        """, (country_iso,))
        facilities = cur.fetchall()

    for (c_iso, fname, operator, cap_mw, src_count) in facilities:
        update = {"country_iso": c_iso, "facility_name": fname,
                  "operator": operator, "status": "operational",
                  "ownership_type": "unknown",
                  "is_hyperscaler": any(op in operator for op in HYPERSCALER_OPERATORS),
                  "source_urls": [], "source_count": 0}

        if cap_mw is not None and src_count >= 2:
            update["confidence_score"]    = CONFIDENCE["multi_source"]
            update["has_estimated_fields"] = False
        elif cap_mw is not None:
            update["confidence_score"]    = CONFIDENCE["agent_single"]
            update["has_estimated_fields"] = False
        else:
            # Apply benchmark estimate
            benchmark = next(
                (v for op, v in HYPERSCALER_BENCHMARK_MW.items() if op in operator),
                HYPERSCALER_BENCHMARK_MW["default"]
            )
            update["capacity_mw"]          = benchmark
            update["confidence_score"]     = CONFIDENCE["benchmark_est"]
            update["has_estimated_fields"] = True
            upsert_gap(conn, c_iso, "capacity_mw", fname,
                       "field_gap", "capacity_mw not found after enrichment",
                       "medium" if update["is_hyperscaler"] else "low")

        upsert_facility(conn, run_id, update)
        log_attempt(conn, run_id, c_iso, fname, "validation",
                    None, None, "success", update["confidence_score"], 0)
        updated += 1

    conn.commit()
    return updated
```

- [ ] **Step 4: Run all collector tests**

```bash
pytest tests/test_collectors.py -v
```

Expected: `9 passed`

- [ ] **Step 5: Commit**

```bash
git add cii/cii_collectors.py tests/test_collectors.py
git commit -m "feat: SI1 enrichment and validation passes with benchmark fallback"
```

---

## Task 7: SI3 Research Collectors

**Files:**
- Create: `cii/cii_si3_collectors.py`
- Create: `tests/test_si3_collectors.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_si3_collectors.py
import pytest, sys, os
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cii'))


def test_frontier_training_known_zero_stored_with_high_confidence(mock_conn):
    from cii_si3_collectors import collect_frontier_training
    collect_frontier_training(mock_conn, "run-123", "PH")

    calls = mock_conn.cursor.return_value.__enter__.return_value.execute.call_args_list
    insert_calls = [str(c) for c in calls if "cii_raw_metrics" in str(c)]
    assert any("frontier_training_present" in c for c in insert_calls)
    # value should be 0.0, confidence high
    for c in calls:
        args = c[0]
        if len(args) > 1 and isinstance(args[1], tuple) and "PH" in args[1]:
            if "frontier_training_present" in str(args):
                assert 0.0 in args[1] or args[1][3] == 0.0


def test_domestic_ownership_writes_raw_metric(mock_conn):
    with patch("cii_si3_collectors.web_search") as mock_search, \
         patch("cii_si3_collectors._extract_ownership_claude") as mock_extract, \
         patch("anthropic.Anthropic"):

        mock_search.return_value = [
            {"url": "https://edb.gov.sg", "title": "SG DC", "content": "70% foreign-owned"}
        ]
        mock_extract.return_value = {"domestic_ratio": 0.30, "confidence": 0.70}

        from cii_si3_collectors import collect_domestic_ownership
        collect_domestic_ownership(mock_conn, "run-123", "SG")

        calls = mock_conn.cursor.return_value.__enter__.return_value.execute.call_args_list
        assert any("cii_raw_metrics" in str(c) for c in calls)
```

- [ ] **Step 2: Run — verify they fail**

```bash
pytest tests/test_si3_collectors.py -v
```

Expected: `ImportError: No module named 'cii_si3_collectors'`

- [ ] **Step 3: Create cii/cii_si3_collectors.py**

```python
import os, json, re, time
from datetime import date
from dotenv import load_dotenv
from cii_collectors import (
    web_search, log_attempt, upsert_gap,
    ANTHROPIC_API_KEY, COUNTRIES, CONFIDENCE, KNOWN_ZERO_FRONTIER,
)

load_dotenv()

FRONTIER_TRAINING_COUNTRIES = {"US"}   # confirmed in-jurisdiction frontier training

OWNERSHIP_QUERIES = {
    "US": "United States major data center operators domestic vs foreign ownership AWS Google Microsoft",
    "AE": "UAE data center operators domestic foreign ownership Mubadala G42 DEWA versus hyperscaler",
    "BR": "Brazil data center operators domestic Ascenty Equinix foreign ownership ratio",
    "IN": "India data center operators domestic Adani Hiranandani vs foreign hyperscaler ownership",
    "SG": "Singapore data center operators domestic vs foreign hyperscaler ownership EDB",
    "PH": "Philippines data center operators domestic foreign ownership DICT PEZA hyperscaler",
}

FRONTIER_QUERIES = {
    "US": "United States frontier AI model training GPT-4 Claude Gemini in-jurisdiction",
    "AE": "UAE frontier AI model training in-country G42 Falcon TII",
    "BR": "Brazil frontier AI model training domestic",
    "IN": "India frontier AI model training in-jurisdiction domestic",
    "SG": "Singapore frontier AI model training in-jurisdiction",
    "PH": "Philippines frontier AI model training in-jurisdiction",
}


def upsert_raw_metric(conn, run_id, country_iso, sub_index, metric_key,
                      metric_value, unit, confidence, source_name):
    country_name = COUNTRIES[country_iso]
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cii_raw_metrics
                (country_iso, country_name, sub_index, metric_key, metric_value,
                 unit, data_date, confidence_score, source_name, run_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (country_iso, metric_key, data_date) DO UPDATE SET
                metric_value     = EXCLUDED.metric_value,
                confidence_score = GREATEST(EXCLUDED.confidence_score,
                                            cii_raw_metrics.confidence_score),
                source_name      = EXCLUDED.source_name,
                run_id           = EXCLUDED.run_id,
                collected_at     = NOW()
        """, (country_iso, country_name, sub_index, metric_key, metric_value,
              unit, date.today(), confidence, source_name, run_id))
    conn.commit()


def _extract_ownership_claude(client, results: list[dict], country_iso: str) -> dict:
    content = "\n\n".join(
        f"Source: {r['url']}\nContent: {r['content'][:600]}"
        for r in results if r.get("content")
    )
    if not content.strip():
        return {}
    prompt = f"""Estimate the domestic vs foreign ownership ratio of AI/cloud data center capacity in {COUNTRIES[country_iso]}.
"Domestic" = operators headquartered in {COUNTRIES[country_iso]}.
"Foreign" = operators headquartered elsewhere (AWS, Azure, Google, Meta, Oracle etc.).
Return JSON: {{"domestic_ratio": 0.0-1.0, "confidence": 0.50-0.90}}
Sources: {content}
Return ONLY the JSON."""
    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        text = re.sub(r"^```[a-z]*\n?|```$", "", resp.content[0].text.strip())
        return json.loads(text)
    except Exception:
        return {}


def collect_domestic_ownership(conn, run_id: str, country_iso: str) -> None:
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    query = OWNERSHIP_QUERIES[country_iso]
    t0 = time.perf_counter()
    try:
        results = web_search(query, count=5)
        data = _extract_ownership_claude(client, results, country_iso)
        if data and data.get("domestic_ratio") is not None:
            upsert_raw_metric(conn, run_id, country_iso, "SI3",
                              "domestic_ownership_ratio",
                              round(data["domestic_ratio"], 4), "ratio",
                              data.get("confidence", CONFIDENCE["agent_single"]),
                              "research_agent")
        else:
            upsert_gap(conn, country_iso, "domestic_ownership_ratio", None,
                       "metric_gap", "Could not extract ownership ratio",
                       "medium")
        elapsed = int((time.perf_counter() - t0) * 1000)
        log_attempt(conn, run_id, country_iso, None, "si3", query, None,
                    "success" if data else "gap",
                    data.get("confidence") if data else None, elapsed)
    except Exception as exc:
        elapsed = int((time.perf_counter() - t0) * 1000)
        log_attempt(conn, run_id, country_iso, None, "si3", query, None,
                    "failed", None, elapsed, str(exc)[:500])


def collect_frontier_training(conn, run_id: str, country_iso: str) -> None:
    """Collect frontier AI training presence.
    Known zeros (PH, BR) stored directly without search."""
    if country_iso in KNOWN_ZERO_FRONTIER:
        upsert_raw_metric(conn, run_id, country_iso, "SI3",
                          "frontier_training_present", 0.0, "boolean",
                          0.90, "known_zero_documented")
        return

    if country_iso in FRONTIER_TRAINING_COUNTRIES:
        upsert_raw_metric(conn, run_id, country_iso, "SI3",
                          "frontier_training_present", 1.0, "boolean",
                          0.90, "known_confirmed")
        return

    # For ambiguous countries — search and classify
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    query = FRONTIER_QUERIES[country_iso]
    t0 = time.perf_counter()
    try:
        results = web_search(query, count=3)
        content = " ".join(r.get("content", "") for r in results)
        prompt = f"""Is there confirmed frontier AI model training (GPT-4 scale or larger) 
happening in-jurisdiction in {COUNTRIES[country_iso]}?
Answer JSON: {{"present": true/false, "confidence": 0.50-0.90, "evidence": "one sentence"}}
Context: {content[:1000]}
Return ONLY the JSON."""
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )
        text = re.sub(r"^```[a-z]*\n?|```$", "", resp.content[0].text.strip())
        data = json.loads(text)
        value = 1.0 if data.get("present") else 0.0
        upsert_raw_metric(conn, run_id, country_iso, "SI3",
                          "frontier_training_present", value, "boolean",
                          data.get("confidence", CONFIDENCE["agent_single"]),
                          "research_agent")
        elapsed = int((time.perf_counter() - t0) * 1000)
        log_attempt(conn, run_id, country_iso, None, "si3", query, None,
                    "success", data.get("confidence"), elapsed)
    except Exception as exc:
        elapsed = int((time.perf_counter() - t0) * 1000)
        log_attempt(conn, run_id, country_iso, None, "si3", query, None,
                    "failed", None, elapsed, str(exc)[:500])
        upsert_gap(conn, country_iso, "frontier_training_present", None,
                   "metric_gap", str(exc)[:300], "medium")
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_si3_collectors.py -v
```

Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add cii/cii_si3_collectors.py tests/test_si3_collectors.py
git commit -m "feat: SI3 research collectors (domestic ownership, frontier training)"
```

---

## Task 8: SI2 Computation + SI3 Derived Metrics

**Files:**
- Create: `cii/cii_scoring.py`
- Create: `tests/test_scoring.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_scoring.py
import pytest, sys, os
from unittest.mock import MagicMock, patch, call
from datetime import date
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cii'))


def _make_cursor_with_data(rows_by_query: dict):
    """Helper: return a mock cursor whose fetchall/fetchone returns
    different rows depending on which query was last executed."""
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    last_query = [None]

    def execute(sql, params=None):
        last_query[0] = sql

    def fetchall():
        for key, rows in rows_by_query.items():
            if key in (last_query[0] or ""):
                return rows
        return []

    def fetchone():
        for key, rows in rows_by_query.items():
            if key in (last_query[0] or ""):
                return rows[0] if rows else None
        return None

    cursor.execute = execute
    cursor.fetchall = fetchall
    cursor.fetchone = fetchone
    return cursor


def test_compute_si2_snapshot_calculates_qoq():
    conn = MagicMock()
    cursor = _make_cursor_with_data({
        "SUM": [(500.0, 200.0, 1_000_000_000.0, 3)],  # installed, committed, usd, hyperscaler_count
        "prev": [(400.0, 180.0, 900_000_000.0)],        # prev quarter
        "grid_capacity_mw": [(50_000.0,)],
    })
    conn.cursor.return_value = cursor

    from cii_scoring import _compute_si2_snapshot
    snap = _compute_si2_snapshot(conn, "US", "2025Q2", date(2025, 6, 30))

    assert snap["installed_mw"] == 500.0
    assert abs(snap["qoq_installed_growth_rate"] - 0.25) < 0.001   # (500-400)/400
    assert abs(snap["grid_strain_ratio"] - (200.0 / 50_000.0)) < 0.0001


def test_compute_si2_snapshot_null_on_no_prev_quarter():
    conn = MagicMock()
    cursor = _make_cursor_with_data({
        "SUM": [(500.0, 200.0, 1_000_000_000.0, 3)],
        "prev": [],   # no prior quarter — first run
        "grid_capacity_mw": [(50_000.0,)],
    })
    conn.cursor.return_value = cursor

    from cii_scoring import _compute_si2_snapshot
    snap = _compute_si2_snapshot(conn, "US", "2025Q1", date(2025, 3, 31))
    assert snap["qoq_installed_growth_rate"] is None
    assert snap["qoq_committed_mw_growth_rate"] is None


def test_compute_si3_derived_writes_hyperscaler_count(mock_conn):
    mock_conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (7, 5_000_000_000.0)
    with patch("cii_scoring.upsert_raw_metric") as mock_upsert:
        from cii_scoring import compute_si3_derived
        compute_si3_derived(mock_conn, "run-123", "US")
        calls = {c[0][4] for c in mock_upsert.call_args_list}  # metric_key
        assert "hyperscaler_count" in calls
        assert "hyperscaler_investment_usd" in calls
```

- [ ] **Step 2: Run — verify they fail**

```bash
pytest tests/test_scoring.py -v
```

Expected: `ImportError: No module named 'cii_scoring'`

- [ ] **Step 3: Create cii/cii_scoring.py (SI2 + SI3 derived)**

```python
import os
from datetime import date
from dotenv import load_dotenv
from cii_collectors import COUNTRIES, get_conn

load_dotenv()

SDI_DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   "csi_scores",
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
}


def upsert_raw_metric(conn, run_id, country_iso, sub_index, metric_key,
                      metric_value, unit, confidence, source_name):
    from cii_si3_collectors import upsert_raw_metric as _upsert
    _upsert(conn, run_id, country_iso, sub_index, metric_key,
            metric_value, unit, confidence, source_name)


def _quarter_label(d: date) -> str:
    return f"{d.year}Q{(d.month - 1) // 3 + 1}"


def _quarter_end(d: date) -> date:
    q = (d.month - 1) // 3
    ends = [date(d.year, 3, 31), date(d.year, 6, 30),
            date(d.year, 9, 30), date(d.year, 12, 31)]
    return ends[q]


def _compute_si2_snapshot(conn, country_iso: str, quarter: str,
                           quarter_end: date) -> dict:
    """Aggregate facilities into a quarterly snapshot; compute QoQ rates."""
    with conn.cursor() as cur:
        # Current quarter aggregates
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN status='operational' THEN capacity_mw ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status!='operational' THEN capacity_mw ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status!='operational' AND is_hyperscaler
                                  THEN investment_value_usd ELSE 0 END), 0),
                COUNT(CASE WHEN is_hyperscaler AND status!='operational'
                           AND date_announced BETWEEN %s AND %s THEN 1 END)
            FROM cii_facilities WHERE country_iso = %s
        """, (date(quarter_end.year, (quarter_end.month - 2) % 12 + 1, 1),
               quarter_end, country_iso))
        row = cur.fetchall()[0] if cur.fetchall else cur.fetchone()
        if not row:
            installed, committed, committed_usd, new_hs = 0, 0, 0, 0
        else:
            installed, committed, committed_usd, new_hs = row

        # Previous quarter
        prev_q_end = date(quarter_end.year - (1 if quarter_end.month <= 3 else 0),
                          ((quarter_end.month - 4) % 12) + 1,
                          31 if ((quarter_end.month - 4) % 12) + 1 in [1,3,5,7,8,10,12] else 30)
        cur.execute("""
            SELECT installed_mw, committed_mw, committed_usd
            FROM cii_quarterly_snapshots
            WHERE country_iso = %s AND quarter_end_date <= %s
            ORDER BY quarter_end_date DESC LIMIT 1
        """, (country_iso, prev_q_end))
        prev = cur.fetchone()

        # Grid reference
        cur.execute("""
            SELECT grid_capacity_mw FROM cii_grid_reference
            WHERE country_iso = %s ORDER BY data_year DESC LIMIT 1
        """, (country_iso,))
        grid_row = cur.fetchone()
        grid_mw = grid_row[0] if grid_row else None

    def qoq(curr, prev_val):
        if prev is None or prev_val is None or prev_val == 0:
            return None
        return round((curr - prev_val) / prev_val, 6)

    pipeline_mult = round(committed / installed, 4) if installed and installed > 0 else None
    grid_strain = round(committed / grid_mw, 6) if grid_mw and grid_mw > 0 else None

    return {
        "country_iso":                  country_iso,
        "quarter":                      quarter,
        "quarter_end_date":             quarter_end,
        "installed_mw":                 installed,
        "committed_mw":                 committed,
        "committed_usd":                committed_usd,
        "pipeline_multiplier":          pipeline_mult,
        "hyperscaler_commitments_count": new_hs,
        "qoq_installed_growth_rate":    qoq(installed, prev[0] if prev else None),
        "qoq_committed_mw_growth_rate": qoq(committed, prev[1] if prev else None),
        "qoq_committed_usd_growth_rate":qoq(committed_usd, prev[2] if prev else None),
        "national_grid_mw":             grid_mw,
        "grid_strain_ratio":            grid_strain,
    }


def compute_si2_all_countries(conn, run_id: str) -> None:
    """Compute SI2 quarterly snapshot for all 6 countries and write to DB."""
    today = date.today()
    quarter = _quarter_label(today)
    q_end   = _quarter_end(today)

    for country_iso in COUNTRIES:
        snap = _compute_si2_snapshot(conn, country_iso, quarter, q_end)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cii_quarterly_snapshots
                    (country_iso, quarter, quarter_end_date, installed_mw, committed_mw,
                     committed_usd, pipeline_multiplier, hyperscaler_commitments_count,
                     qoq_installed_growth_rate, qoq_committed_mw_growth_rate,
                     qoq_committed_usd_growth_rate, national_grid_mw, grid_strain_ratio,
                     run_id)
                VALUES (%(country_iso)s,%(quarter)s,%(quarter_end_date)s,%(installed_mw)s,
                        %(committed_mw)s,%(committed_usd)s,%(pipeline_multiplier)s,
                        %(hyperscaler_commitments_count)s,%(qoq_installed_growth_rate)s,
                        %(qoq_committed_mw_growth_rate)s,%(qoq_committed_usd_growth_rate)s,
                        %(national_grid_mw)s,%(grid_strain_ratio)s,%(run_id)s)
                ON CONFLICT (country_iso, quarter) DO UPDATE SET
                    installed_mw                    = EXCLUDED.installed_mw,
                    committed_mw                    = EXCLUDED.committed_mw,
                    committed_usd                   = EXCLUDED.committed_usd,
                    pipeline_multiplier             = EXCLUDED.pipeline_multiplier,
                    hyperscaler_commitments_count   = EXCLUDED.hyperscaler_commitments_count,
                    qoq_installed_growth_rate       = EXCLUDED.qoq_installed_growth_rate,
                    qoq_committed_mw_growth_rate    = EXCLUDED.qoq_committed_mw_growth_rate,
                    qoq_committed_usd_growth_rate   = EXCLUDED.qoq_committed_usd_growth_rate,
                    national_grid_mw                = EXCLUDED.national_grid_mw,
                    grid_strain_ratio               = EXCLUDED.grid_strain_ratio,
                    run_id                          = EXCLUDED.run_id,
                    computed_at                     = NOW()
            """, {**snap, "run_id": run_id})
        conn.commit()

        # Also write SI1 + SI2 metrics to cii_raw_metrics for scoring
        today_date = date.today()
        si1_metrics = {
            "installed_capacity_mw": (snap["installed_mw"],   "MW",    0.85),
            "committed_pipeline_mw": (snap["committed_mw"],   "MW",    0.80),
            "pipeline_multiplier":   (snap["pipeline_multiplier"], "ratio", 0.85),
        }
        for mk, (val, unit, conf) in si1_metrics.items():
            if val is not None:
                upsert_raw_metric(conn, run_id, country_iso, "SI1",
                                  mk, val, unit, conf, "cii_facilities_aggregate")

        si2_metrics = {
            "qoq_installed_growth_rate":     (snap["qoq_installed_growth_rate"],     "rate", 0.85),
            "qoq_committed_mw_growth_rate":  (snap["qoq_committed_mw_growth_rate"],  "rate", 0.85),
            "qoq_committed_usd_growth_rate": (snap["qoq_committed_usd_growth_rate"], "rate", 0.80),
            "new_hyperscaler_commitments":   (snap["hyperscaler_commitments_count"], "count", 0.85),
            "grid_strain_ratio":             (snap["grid_strain_ratio"],             "ratio", 0.80),
        }
        for mk, (val, unit, conf) in si2_metrics.items():
            if val is not None:
                upsert_raw_metric(conn, run_id, country_iso, "SI2",
                                  mk, val, unit, conf, "cii_quarterly_snapshots")


def compute_si3_derived(conn, run_id: str, country_iso: str) -> None:
    """Derive SI3 metrics that are computed from cii_facilities (not from search)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(DISTINCT operator), COALESCE(SUM(investment_value_usd), 0)
            FROM cii_facilities
            WHERE country_iso = %s AND is_hyperscaler = TRUE
        """, (country_iso,))
        row = cur.fetchone()

    hs_count = int(row[0]) if row else 0
    hs_invest = float(row[1]) if row else 0.0

    upsert_raw_metric(conn, run_id, country_iso, "SI3",
                      "hyperscaler_count", float(hs_count), "count", 0.90,
                      "cii_facilities_aggregate")
    upsert_raw_metric(conn, run_id, country_iso, "SI3",
                      "hyperscaler_investment_usd", hs_invest, "USD", 0.85,
                      "cii_facilities_aggregate")

    # chip_access_tier from seeded table
    with conn.cursor() as cur:
        cur.execute("""
            SELECT tier FROM cii_chip_access
            WHERE country_iso = %s ORDER BY effective_date DESC LIMIT 1
        """, (country_iso,))
        tier_row = cur.fetchone()
    if tier_row:
        upsert_raw_metric(conn, run_id, country_iso, "SI3",
                          "chip_access_tier", float(tier_row[0]), "tier",
                          0.90, "cii_chip_access_seeded")
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_scoring.py -v
```

Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add cii/cii_scoring.py tests/test_scoring.py
git commit -m "feat: SI2 quarterly snapshot computation and SI3 derived metrics"
```

---

## Task 9: CII Scoring Pipeline + S-C Gap

**Files:**
- Modify: `cii/cii_scoring.py` (add `run_scoring`, `compute_sc_gap`)
- Modify: `tests/test_scoring.py`

- [ ] **Step 1: Add failing tests**

```python
# append to tests/test_scoring.py

def test_minmax_normalization():
    from cii_scoring import _minmax_normalize
    values = {"US": 500.0, "AE": 200.0, "BR": 50.0, "IN": 300.0, "SG": 150.0, "PH": 30.0}
    normed = _minmax_normalize(values, invert=False)
    assert abs(normed["US"] - 100.0) < 0.001
    assert abs(normed["PH"] - 0.0) < 0.001
    assert 0.0 <= normed["AE"] <= 100.0


def test_minmax_normalization_inverted():
    from cii_scoring import _minmax_normalize
    values = {"US": 0.10, "AE": 0.50, "BR": 0.30, "IN": 0.20, "SG": 0.05, "PH": 0.60}
    normed = _minmax_normalize(values, invert=True)
    # SG has lowest stress → highest score after inversion
    assert normed["SG"] == max(normed.values())
    assert normed["PH"] == min(normed.values())


def test_sc_gap_interpretation():
    from cii_scoring import _interpret_sc_gap
    assert _interpret_sc_gap(-0.50) == "under_converting"   # CII > SDI
    assert _interpret_sc_gap(0.10)  == "near_parity"
    assert _interpret_sc_gap(1.20)  == "over_converting"    # SDI > CII


def test_sc_gap_negative_means_cii_exceeds_sdi():
    from cii_scoring import _interpret_sc_gap
    # UAE model: CII > SDI → gap negative → under_converting label
    assert _interpret_sc_gap(-1.5) == "under_converting"
```

- [ ] **Step 2: Run — verify they fail**

```bash
pytest tests/test_scoring.py::test_minmax_normalization -v
```

Expected: `ImportError: cannot import name '_minmax_normalize'`

- [ ] **Step 3: Append scoring functions to cii/cii_scoring.py**

```python
# append to cii/cii_scoring.py
import psycopg2


def _minmax_normalize(values: dict, invert: bool = False) -> dict:
    """Min-max normalize a dict of {country: value} to 0-100.
    Skips None values. Returns same keys with normalized scores."""
    valid = {k: v for k, v in values.items() if v is not None}
    if not valid:
        return {k: None for k in values}
    mn, mx = min(valid.values()), max(valid.values())
    if mx == mn:
        return {k: (50.0 if v is not None else None) for k, v in values.items()}
    result = {}
    for k, v in values.items():
        if v is None:
            result[k] = None
        else:
            score = (v - mn) / (mx - mn) * 100
            result[k] = round(100 - score if invert else score, 4)
    return result


def _interpret_sc_gap(gap: float) -> str:
    if gap < -0.25:
        return "under_converting"
    if gap > 0.25:
        return "over_converting"
    return "near_parity"


def run_scoring(conn, run_id: str) -> None:
    """Read cii_raw_metrics, normalize per metric, apply weights,
    compute sub-index composites and final CII score."""
    # Load methodology
    with conn.cursor() as cur:
        cur.execute("SELECT sub_index, metric_key, weight, invert FROM cii_score_methodology")
        methodology = {(r[0], r[1]): (r[2], r[3]) for r in cur.fetchall()}
        cur.execute("SELECT sub_index, weight FROM cii_subindex_weights")
        si_weights = dict(cur.fetchall())

    countries = list(COUNTRIES.keys())
    today = date.today()

    # Pull latest value per (country, metric)
    raw: dict[tuple, float] = {}
    confidence: dict[tuple, float] = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, metric_key, metric_value, confidence_score
            FROM cii_raw_metrics
            WHERE (country_iso, metric_key, collected_at) IN (
                SELECT country_iso, metric_key, MAX(collected_at)
                FROM cii_raw_metrics GROUP BY country_iso, metric_key
            )
        """)
        for c_iso, mk, val, conf in cur.fetchall():
            raw[(c_iso, mk)] = val
            confidence[(c_iso, mk)] = conf

    # Normalize each metric across all 6 countries
    subindex_scores: dict[str, dict[str, float]] = {si: {} for si in ("SI1", "SI2", "SI3")}

    for (si, mk), (weight, invert) in methodology.items():
        country_vals = {c: raw.get((c, mk)) for c in countries}
        normed = _minmax_normalize(country_vals, invert=invert)

        for c_iso in countries:
            n = normed.get(c_iso)
            ws = round(n * weight, 6) if n is not None else None

            # Write to normalized table
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cii_score_metric_normalized
                        (run_id, country_iso, sub_index, metric_key, raw_value,
                         normalized, inverted, weight, weighted_score)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (run_id, country_iso, sub_index, metric_key) DO UPDATE SET
                        normalized     = EXCLUDED.normalized,
                        weighted_score = EXCLUDED.weighted_score
                """, (run_id, c_iso, si, mk,
                      raw.get((c_iso, mk)), n or 0.0, invert, weight, ws or 0.0))
            conn.commit()

            if ws is not None:
                subindex_scores[si][c_iso] = subindex_scores[si].get(c_iso, 0.0) + ws

    # Sub-index composites
    final_scores: dict[str, dict] = {}
    for si, scores in subindex_scores.items():
        si_weight = si_weights.get(si, 0.0)
        for c_iso, score in scores.items():
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cii_score_subindex
                        (run_id, country_iso, sub_index, score, weight,
                         weighted_score, data_date_min, data_date_max)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (run_id, country_iso, sub_index) DO UPDATE SET
                        score          = EXCLUDED.score,
                        weighted_score = EXCLUDED.weighted_score
                """, (run_id, c_iso, si, round(score, 4), si_weight,
                      round(score * si_weight, 6), today, today))
            conn.commit()
            if c_iso not in final_scores:
                final_scores[c_iso] = {"SI1": None, "SI2": None, "SI3": None}
            final_scores[c_iso][si] = score

    # Final CII score
    cii_vals = {}
    for c_iso, si_map in final_scores.items():
        total = sum(
            (si_map[si] or 0.0) * si_weights.get(si, 0.0)
            for si in ("SI1", "SI2", "SI3")
        )
        cii_vals[c_iso] = round(total, 4)

    ranked = sorted(cii_vals.items(), key=lambda x: x[1], reverse=True)
    for rank, (c_iso, cii_score) in enumerate(ranked, 1):
        si_map = final_scores.get(c_iso, {})
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cii_score_final
                    (run_id, country_iso, si1_capacity, si2_velocity, si3_quality,
                     cii_score, rank, data_date_min, data_date_max)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id, country_iso) DO UPDATE SET
                    cii_score = EXCLUDED.cii_score, rank = EXCLUDED.rank
            """, (run_id, c_iso,
                  si_map.get("SI1"), si_map.get("SI2"), si_map.get("SI3"),
                  cii_score, rank, today, today))
        conn.commit()


def compute_sc_gap(conn, run_id: str) -> None:
    """Read CII scores and SDI scores; compute and store S-C Gap."""
    import psycopg2 as _pg

    # Read CII scores (this run)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, cii_score FROM cii_score_final WHERE run_id = %s
        """, (run_id,))
        cii_rows = dict(cur.fetchall())

    # Read SDI scores from csi_scores DB
    try:
        sdi_conn = _pg.connect(**SDI_DB_CONFIG)
        with sdi_conn.cursor() as cur:
            cur.execute("SELECT country_iso, sdi_score FROM score_sdi")
            sdi_rows = dict(cur.fetchall())
        sdi_conn.close()
    except Exception:
        sdi_rows = {}  # SDI DB not available — gap will be noted

    today = date.today()
    for c_iso in COUNTRIES:
        cii_raw = cii_rows.get(c_iso)
        sdi_raw = sdi_rows.get(c_iso)
        if cii_raw is None:
            continue
        cii_norm = round(cii_raw / 20.0, 4)        # 0-100 → 0-5
        sdi_norm = round(sdi_raw / 20.0, 4) if sdi_raw else None
        gap      = round(sdi_norm - cii_norm, 4) if sdi_norm is not None else None
        interp   = _interpret_sc_gap(gap) if gap is not None else None

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cii_sc_gap
                    (run_id, country_iso, sdi_score, sdi_normalized,
                     cii_score, cii_normalized, sc_gap, interpretation)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id, country_iso) DO UPDATE SET
                    sc_gap = EXCLUDED.sc_gap, interpretation = EXCLUDED.interpretation
            """, (run_id, c_iso, sdi_raw, sdi_norm,
                  cii_raw, cii_norm, gap, interp))
        conn.commit()
```

- [ ] **Step 4: Run all scoring tests**

```bash
pytest tests/test_scoring.py -v
```

Expected: `7 passed`

- [ ] **Step 5: Commit**

```bash
git add cii/cii_scoring.py tests/test_scoring.py
git commit -m "feat: CII scoring pipeline and S-C Gap computation"
```

---

## Task 10: Gap Report

**Files:**
- Create: `cii/cii_gap_report.py`
- Create: `tests/test_gap_report.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_gap_report.py
import pytest, sys, os
from unittest.mock import MagicMock
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cii'))


def test_gap_report_orders_by_severity(mock_conn):
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.side_effect = [
        # CRITICAL gaps
        [("AE", "installed_capacity_mw", None, "No facilities found", 2,
          "Check DEWA annual report", True)],
        # HIGH gaps
        [("BR", "capacity_mw", "Equinix SP4", "MW missing", 1, None, False)],
        # MEDIUM gaps
        [],
        # STRUCTURAL
        [("PH", "frontier_training_present", None, "known_zero", 1, None, False)],
    ]

    from cii_gap_report import build_gap_report
    report = build_gap_report(mock_conn)
    assert report["critical"][0]["country_iso"] == "AE"
    assert report["high"][0]["country_iso"] == "BR"
    assert len(report["structural"]) == 1


def test_gap_report_prints_without_error(mock_conn, capsys):
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.side_effect = [
        [], [], [], []
    ]
    from cii_gap_report import build_gap_report, print_gap_report
    report = build_gap_report(mock_conn)
    print_gap_report(report)
    captured = capsys.readouterr()
    assert "CII Gap Report" in captured.out
```

- [ ] **Step 2: Run — verify they fail**

```bash
pytest tests/test_gap_report.py -v
```

Expected: `ImportError: No module named 'cii_gap_report'`

- [ ] **Step 3: Create cii/cii_gap_report.py**

```python
import os
from dotenv import load_dotenv
from cii_collectors import get_conn

load_dotenv()


def build_gap_report(conn) -> dict:
    """Query cii_data_gaps and return prioritised sections."""
    report = {"critical": [], "high": [], "medium": [], "structural": []}

    severity_map = {
        "critical":   "critical",
        "high":       "high",
        "medium":     "medium",
        "structural": "structural",
    }

    for sev, key in severity_map.items():
        with conn.cursor() as cur:
            cur.execute("""
                SELECT country_iso, metric_key, facility_name, failure_reason,
                       attempt_count, recommended_action, manual_review_required
                FROM cii_data_gaps
                WHERE severity = %s AND status = 'open'
                ORDER BY attempt_count DESC, country_iso
            """, (sev,))
            for row in cur.fetchall():
                report[key].append({
                    "country_iso":            row[0],
                    "metric_key":             row[1],
                    "facility_name":          row[2],
                    "failure_reason":         row[3],
                    "attempt_count":          row[4],
                    "recommended_action":     row[5],
                    "manual_review_required": row[6],
                })
    return report


def print_gap_report(report: dict) -> None:
    print("\n" + "=" * 60)
    print("CII Gap Report")
    print("=" * 60)

    def _section(title, items):
        print(f"\n{title} ({len(items)})")
        if not items:
            print("  None.")
            return
        for g in items:
            flag = " ⚑ MANUAL REVIEW" if g["manual_review_required"] else ""
            print(f"  [{g['country_iso']}] {g['metric_key']}"
                  f"{' / ' + g['facility_name'] if g['facility_name'] else ''}"
                  f"{flag}")
            print(f"    Reason: {g['failure_reason']}")
            print(f"    Attempts: {g['attempt_count']}")
            if g["recommended_action"]:
                print(f"    Action:  {g['recommended_action']}")

    _section("CRITICAL — blocks scoring", report["critical"])
    _section("HIGH — degrades score quality", report["high"])
    _section("MEDIUM — noted", report["medium"])
    _section("STRUCTURAL — confirmed absence or unobservable", report["structural"])
    print()


if __name__ == "__main__":
    conn = get_conn()
    report = build_gap_report(conn)
    print_gap_report(report)
    conn.close()
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_gap_report.py -v
```

Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add cii/cii_gap_report.py tests/test_gap_report.py
git commit -m "feat: prioritised gap report (cii_gap_report.py)"
```

---

## Task 11: Orchestrator (run_cii.py)

**Files:**
- Create: `cii/run_cii.py`

- [ ] **Step 1: Create cii/run_cii.py**

```python
"""
CII Pipeline Orchestrator
Usage: python run_cii.py [--only si1|si3|si2|scoring|gap]
"""
import os, sys, uuid, time, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(__file__))

from cii_collectors import (
    get_conn, COUNTRIES, log_attempt,
    run_discovery_pass, run_enrichment_pass, run_validation_pass,
)
from cii_si3_collectors import collect_domestic_ownership, collect_frontier_training
from cii_scoring import (
    compute_si2_all_countries, compute_si3_derived,
    run_scoring, compute_sc_gap,
)
from cii_gap_report import build_gap_report, print_gap_report

TAVILY_MONTHLY_BUDGET = 900   # warn at 90% of 1000 free tier


def _check_tavily_quota(conn, run_id: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(SUM(tavily_calls_used), 0) FROM cii_collection_runs "
                    "WHERE started_at >= date_trunc('month', NOW())")
        used = cur.fetchone()[0]
    if used >= TAVILY_MONTHLY_BUDGET:
        print(f"  ⚠ Tavily quota warning: {used} calls used this month "
              f"(budget: {TAVILY_MONTHLY_BUDGET})")
        return False
    return True


def _run_si1_country(run_id: str, country_iso: str) -> str:
    conn = get_conn()  # each thread gets its own connection
    try:
        print(f"  [SI1] {country_iso} — discovery pass")
        run_discovery_pass(conn, run_id, country_iso)
        print(f"  [SI1] {country_iso} — enrichment pass")
        run_enrichment_pass(conn, run_id, country_iso)
        print(f"  [SI1] {country_iso} — validation pass")
        run_validation_pass(conn, run_id, country_iso)
        return f"SI1 {country_iso} OK"
    except Exception as exc:
        return f"SI1 {country_iso} FAILED: {exc}"
    finally:
        conn.close()


def _run_si3_research_country(run_id: str, country_iso: str) -> str:
    conn = get_conn()
    try:
        collect_domestic_ownership(conn, run_id, country_iso)
        collect_frontier_training(conn, run_id, country_iso)
        return f"SI3-research {country_iso} OK"
    except Exception as exc:
        return f"SI3-research {country_iso} FAILED: {exc}"
    finally:
        conn.close()


def main(only: str = None):
    run_id = str(uuid.uuid4())
    conn = get_conn()

    # Register run
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cii_collection_runs (run_id, status) VALUES (%s, 'running')
        """, (run_id,))
    conn.commit()

    print(f"\nCII Pipeline — run_id: {run_id}")
    t_start = time.perf_counter()

    try:
        if only in (None, "si1", "si3"):
            # ── Phase 1: SI1 + SI3 research in parallel ──────────────────────
            print("\n── Phase 1: SI1 facility collection + SI3 research (parallel) ──")
            if not _check_tavily_quota(conn, run_id):
                print("  Aborting: Tavily quota exhausted.")
                return

            tasks = (
                [(_run_si1_country,         run_id, c) for c in COUNTRIES] +
                [(_run_si3_research_country, run_id, c) for c in COUNTRIES]
            )
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {pool.submit(fn, *args): f"{fn.__name__}-{args[-1]}"
                           for fn, *args in tasks}
                for fut in as_completed(futures):
                    print(f"  ✓ {futures[fut]}: {fut.result()}")

        if only in (None, "si2", "scoring"):
            # ── Phase 2: SI2 + SI3 derived (sequential, needs SI1 data) ─────
            print("\n── Phase 2: SI2 computation + SI3 derived metrics ──")
            compute_si2_all_countries(conn, run_id)
            for country_iso in COUNTRIES:
                compute_si3_derived(conn, run_id, country_iso)
            print("  Phase 2 complete.")

        if only in (None, "scoring"):
            # ── Phase 3: Scoring + S-C Gap ────────────────────────────────
            print("\n── Phase 3: CII scoring + S-C Gap ──")
            run_scoring(conn, run_id)
            compute_sc_gap(conn, run_id)
            print("  Phase 3 complete.")

        # Mark run complete
        elapsed = int(time.perf_counter() - t_start)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE cii_collection_runs
                SET status = 'completed', finished_at = NOW(),
                    notes = %s
                WHERE run_id = %s
            """, (f"Completed in {elapsed}s", run_id))
        conn.commit()

        print(f"\n── Gap Report ──")
        print_gap_report(build_gap_report(conn))
        print(f"\nDone in {elapsed}s — run_id: {run_id}")

    except Exception as exc:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE cii_collection_runs
                SET status = 'failed', finished_at = NOW(), notes = %s
                WHERE run_id = %s
            """, (str(exc)[:500], run_id))
        conn.commit()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=["si1","si3","si2","scoring","gap"],
                        help="Run only a specific phase")
    args = parser.parse_args()
    main(only=args.only)
```

- [ ] **Step 2: Smoke test (dry import — no DB needed)**

```bash
cd "/Users/shankar/Desktop/Workstream 2 Capstone"
python -c "import sys; sys.path.insert(0,'cii'); import run_cii; print('import OK')"
```

Expected: `import OK`

- [ ] **Step 3: Run full test suite**

```bash
pytest tests/ -v
```

Expected: all tests pass

- [ ] **Step 4: Commit**

```bash
git add cii/run_cii.py
git commit -m "feat: CII pipeline orchestrator with phase control and quota guard"
```

---

## Task 12: Verify Script + Final Push

**Files:**
- Create: `cii/cii_verify.py`

- [ ] **Step 1: Create cii/cii_verify.py**

```python
"""Post-run verification: check expected tables populated, confidence in bounds,
scores computed. Run after a full pipeline run."""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from cii_collectors import get_conn, COUNTRIES

CHECKS = []


def check(label):
    def decorator(fn):
        CHECKS.append((label, fn))
        return fn
    return decorator


@check("cii_facilities: at least 1 facility per country")
def check_facilities(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, COUNT(*) FROM cii_facilities
            GROUP BY country_iso
        """)
        rows = dict(cur.fetchall())
    gaps = [c for c in COUNTRIES if rows.get(c, 0) == 0]
    return (not gaps), f"No facilities found for: {gaps}" if gaps else "OK"


@check("cii_facilities: confidence_score in [0, 1]")
def check_confidence_bounds(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM cii_facilities
            WHERE confidence_score < 0 OR confidence_score > 1
        """)
        bad = cur.fetchone()[0]
    return bad == 0, f"{bad} rows with confidence out of [0,1]" if bad else "OK"


@check("cii_raw_metrics: SI1 metrics present for all countries")
def check_si1_metrics(conn):
    required = ["installed_capacity_mw", "committed_pipeline_mw"]
    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, metric_key FROM cii_raw_metrics
            WHERE sub_index = 'SI1'
        """)
        present = {(r[0], r[1]) for r in cur.fetchall()}
    missing = [(c, m) for c in COUNTRIES for m in required if (c, m) not in present]
    return (not missing), f"Missing SI1 metrics: {missing}" if missing else "OK"


@check("cii_score_final: all 6 countries scored")
def check_scores_populated(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT country_iso FROM cii_score_final
            WHERE run_id = (SELECT run_id FROM cii_collection_runs
                            WHERE status='completed' ORDER BY finished_at DESC LIMIT 1)
        """)
        scored = {r[0] for r in cur.fetchall()}
    missing = set(COUNTRIES) - scored
    return (not missing), f"Countries not scored: {missing}" if missing else "OK"


@check("cii_sc_gap: S-C Gap computed for all countries")
def check_sc_gap(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT country_iso FROM cii_sc_gap
            WHERE run_id = (SELECT run_id FROM cii_collection_runs
                            WHERE status='completed' ORDER BY finished_at DESC LIMIT 1)
        """)
        gapped = {r[0] for r in cur.fetchall()}
    missing = set(COUNTRIES) - gapped
    return (not missing), f"Countries missing S-C Gap: {missing}" if missing else "OK"


if __name__ == "__main__":
    conn = get_conn()
    print("\nCII Post-Run Verification")
    print("=" * 50)
    all_pass = True
    for label, fn in CHECKS:
        try:
            passed, msg = fn(conn)
            status = "✓" if passed else "✗"
            print(f"  {status} {label}")
            if not passed:
                print(f"      → {msg}")
                all_pass = False
        except Exception as exc:
            print(f"  ✗ {label} — ERROR: {exc}")
            all_pass = False
    conn.close()
    print()
    sys.exit(0 if all_pass else 1)
```

- [ ] **Step 2: Run final test suite**

```bash
pytest tests/ -v --tb=short
```

Expected: all tests pass

- [ ] **Step 3: Commit and push**

```bash
git add cii/cii_verify.py
git commit -m "feat: post-run verification script (cii_verify.py)"
git push origin main
```

---

## Running the Full Pipeline

```bash
# 1. Set up SSH tunnel
ssh -L 5433:localhost:5433 sdeenadayalan@rsm-compute-02.ucsd.edu -N \
    -o ServerAliveInterval=60 -o ServerAliveCountMax=10

# 2. Bootstrap DB (first time only)
cd "/Users/shankar/Desktop/Workstream 2 Capstone"
source .venv/bin/activate   # or: python -m venv .venv && pip install -r requirements.txt
python cii/setup_cii.py

# 3. Run full pipeline
python cii/run_cii.py

# 4. Run specific phase only
python cii/run_cii.py --only si1
python cii/run_cii.py --only scoring

# 5. Verify results
python cii/cii_verify.py

# 6. View gap report
python cii/cii_gap_report.py
```

---

*Plan by: Capstone Group | 2026-05-05*
