# Compute Infrastructure Index (CII) — System Design
**Gramercy Investment | Workstream 2**
*Version 1.0 — 2026-05-05*

---

## 1. Purpose

The Compute Infrastructure Index (CII) measures, by country, the scale, growth rate, and quality of AI compute infrastructure being built and operated. It captures a critical distinction from Workstream 1 (SDI): a country may own abundant natural resources (high SDI) yet fail to convert them into AI-era infrastructure (low CII). The gap between the two — the **S-C Gap** — is the primary analytical output.

**Target countries:** United States (US), United Arab Emirates (AE), Brazil (BR), India (IN), Singapore (SG), Philippines (PH)

**Update cadence:** Quarterly

---

## 2. Index Structure

```
CII = 0.40 × SI1 (Installed and Committed Capacity)
    + 0.35 × SI2 (Growth Velocity)
    + 0.25 × SI3 (Compute Quality and Access)
```

### Sub-Index 1 — Installed and Committed Capacity (40%)

Measures current operational AI compute capacity and the committed pipeline.

| Metric | Description | Scoring Weight |
|---|---|---|
| `installed_capacity_mw` | Total MW of operational data center capacity | 0.40 |
| `committed_pipeline_mw` | Total MW permitted, under construction, or announced | 0.40 |
| `pipeline_multiplier` | committed ÷ installed (acceleration signal) | 0.20 |

*Note: `investment_value_usd` of committed hyperscaler projects is collected and stored at facility level but feeds SI2 pipeline USD growth, not SI1 scoring directly.*

### Sub-Index 2 — Growth Velocity (35%)

Measures the rate at which compute capacity is scaling. Acceleration is the signal, not the level. Computed entirely from SI1's time-series facility database — no separate data collection required.

| Metric | Description | Scoring Weight |
|---|---|---|
| `qoq_installed_growth_rate` | Quarter-over-quarter growth of installed capacity (MW) | 0.40 |
| `qoq_committed_mw_growth_rate` | QoQ growth of committed pipeline (MW) | 0.20 |
| `qoq_committed_usd_growth_rate` | QoQ growth of committed pipeline (USD) | 0.10 |
| `new_hyperscaler_commitments` | New major platform commitments per quarter | 0.20 |
| `grid_strain_ratio` | Committed DC capacity ÷ national grid capacity (inverted) | 0.10 |

*SI2 is NULL on first run. The second quarterly run produces the first valid growth velocity scores.*

### Sub-Index 3 — Compute Quality and Access (25%)

Measures the quality of compute being deployed — not just scale, but what kind and who controls it.

| Metric | Description | Scoring Weight |
|---|---|---|
| `chip_access_tier` | Policy-classified 1–5 scale (1 = unrestricted latest-gen, 5 = denied/sanctioned) | 0.40 |
| `hyperscaler_count` | Number of distinct major cloud/AI platforms with committed presence | 0.20 |
| `hyperscaler_investment_usd` | Total investment value of hyperscaler commitments | 0.15 |
| `domestic_ownership_ratio` | Domestic operator capacity ÷ total capacity | 0.15 |
| `frontier_training_present` | Boolean: confirmed in-jurisdiction frontier AI model training | 0.10 |

---

## 3. S-C Gap Analysis

```
S-C Gap = SDI (normalized 0–5) − CII (normalized 0–5)
```

| Gap Value | Interpretation | Country Model |
|---|---|---|
| Negative (CII > SDI) | Converting beyond resource base — importing substrate through capital | UAE |
| Near zero (±0.25) | Converting at roughly the rate endowment supports | US |
| Positive (SDI > CII) | Under-converting — value leaking through commodity export | Brazil |

---

## 4. System Architecture

### 4.1 Directory Structure

```
Gramercy/
└── cii/
    ├── cii_schema.sql          # Full DB schema + seeded methodology weights
    ├── setup_cii.py            # One-shot DB bootstrap (idempotent)
    ├── cii_pipeline.py         # Main orchestrator
    ├── cii_collectors.py       # 3-pass facility collection (discovery → enrichment → validation)
    ├── cii_si3_collectors.py   # SI3 quality/access collectors (chip tier, diversity, ownership)
    ├── cii_scoring.py          # SI2 computation + CII composite + S-C Gap
    ├── cii_gap_report.py       # Prioritized gap report for analyst review
    ├── cii_verify.py           # Post-run bounds and freshness checks
    └── run_cii.py              # Entry point
```

### 4.2 Pipeline Phases

```
run_cii.py
│
├── Phase 1 — PARALLEL (ThreadPoolExecutor, 2 workers)
│   ├── SI1 Collector: 3-pass facility discovery per country
│   │     Pass 1: Discovery    → enumerate all facilities (6–8 queries/country)
│   │     Pass 2: Enrichment   → deep-dive each facility (3–4 queries/facility)
│   │     Pass 3: Validation   → cross-check MW across sources
│   │     → streams each record to DB immediately (UPSERT on conflict)
│   │
│   └── SI3 Research Collector: metrics requiring search (run independently of SI1)
│         → domestic ownership ratio (research agent per country)
│         → frontier training presence (research agent per country)
│         → writes to cii_raw_metrics
│         Note: chip tier is read from seeded cii_chip_access — no search needed
│
├── Phase 2 — SEQUENTIAL (after Phase 1 completes, SI1 facilities must be in DB)
│   ├── SI2 Computation: derives growth velocity from SI1 facility time series
│   │     → reads cii_facilities, aggregates per country per quarter
│   │     → computes QoQ growth rates (installed MW, committed MW, committed USD)
│   │     → computes grid strain ratio (committed_mw ÷ cii_grid_reference.grid_capacity_mw)
│   │     → writes snapshot to cii_quarterly_snapshots
│   │     → writes latest SI2 values to cii_raw_metrics
│   │
│   └── SI3 Derived Metrics: computed from SI1 facility database
│         → hyperscaler_count (COUNT from cii_facilities WHERE is_hyperscaler=TRUE)
│         → hyperscaler_investment_usd (SUM from cii_facilities WHERE is_hyperscaler=TRUE)
│         → domestic_ownership_ratio (aggregate cii_facilities by ownership_type)
│         → writes to cii_raw_metrics
│
└── Phase 3 — SEQUENTIAL
    └── Scoring: CII composite + S-C Gap
          → reads cii_raw_metrics (uniform interface, all sub-indexes)
          → applies min-max normalization per metric across 6 countries
          → applies methodology weights (config-driven, no code change to re-weight)
          → writes cii_score_metric_inputs → cii_score_metric_normalized
                 → cii_score_subindex → cii_score_final → cii_sc_gap
```

### 4.3 SI1 Three-Pass Collection (Per Country)

**Pass 1 — Discovery (6–8 Tavily queries per country)**

Goal: enumerate every AI data center facility. Queries approach from multiple angles simultaneously to avoid missing facilities:

```
"{country} hyperscaler data center capacity MW announced 2024 2025"
"{country} Microsoft Azure data center investment"
"{country} AWS Amazon data center facility"
"{country} Google cloud data center"
"{country} Meta AI data center"
"{country} Oracle data center"
"{country} data center energy permit MW grid connection {year}"
"{country} AI compute infrastructure investment billion USD"
```

Claude extracts per result: `facility_name`, `operator`, `capacity_hint`, `status`, `source_url`

Each discovered facility is immediately UPSERTed to `cii_facilities` with `confidence_score = 0.30` (discovery-level). Deduplication is enforced by the unique constraint `(country_iso, facility_name, operator)`.

**Pass 2 — Enrichment (3–4 targeted queries per facility)**

Goal: fill in exact MW, investment, dates, energy source, chip type for each discovered facility:

```
"{facility_name} {operator} capacity megawatt MW"
"{facility_name} {operator} investment USD billion"
"{facility_name} {operator} operational date commissioned"
```

Claude extracts structured values with units. Confidence escalates based on source quality (see Section 6). Record updated in-place via UPSERT.

**Pass 3 — Validation (per facility)**

Goal: cross-check `capacity_mw` across independently sourced results:

```
≥ 2 sources agree on MW  → confidence = 0.85–0.90
1 credible source         → confidence = 0.70–0.75
1 source, MW extrapolated → confidence = 0.55–0.65
No MW found               → benchmark estimate at 0.50, flagged
```

`source_count` is incremented for each independent URL that confirms the value. `has_estimated_fields = TRUE` is set if any field used a benchmark.

---

## 5. Database Schema

**Database name:** `cii`

### Collection Layer

```sql
cii_facilities
-- One row per physical data center facility
-- country_iso, facility_name, operator, capacity_mw,
-- status (operational/permitted/under_construction/announced),
-- date_announced, date_operational, investment_value_usd,
-- energy_source, chip_type_if_known,
-- ownership_type (domestic/foreign/joint_venture/unknown),
-- is_hyperscaler BOOLEAN,
-- has_estimated_fields BOOLEAN,
-- confidence_score, source_urls[], source_count,
-- first_seen_run_id, last_updated_run_id
-- UNIQUE (country_iso, facility_name, operator)

cii_quarterly_snapshots
-- Aggregated per country per quarter — SI2 time series
-- country_iso, quarter ('2025Q1'), quarter_end_date,
-- installed_mw, committed_mw, committed_usd, pipeline_multiplier,
-- hyperscaler_commitments_count,
-- qoq_installed_growth_rate, qoq_committed_mw_growth_rate,
-- qoq_committed_usd_growth_rate,
-- national_grid_mw, grid_strain_ratio
-- UNIQUE (country_iso, quarter)

cii_chip_access
-- Policy-classified chip access tiers (seeded in schema, updated when policy changes)
-- country_iso, tier (1–5), tier_label, policy_basis, effective_date, review_date
-- UNIQUE (country_iso, effective_date)

cii_grid_reference
-- National grid capacity for SI2 grid strain denominator
-- country_iso, grid_capacity_mw, data_year, source_name, source_url
-- UNIQUE (country_iso, data_year)

cii_raw_metrics
-- Country-level metrics — uniform scoring input layer for all sub-indexes
-- country_iso, sub_index, metric_key, metric_value, unit,
-- data_date, confidence_score, source_name, run_id
-- UNIQUE (country_iso, metric_key, data_date)
```

### Scoring Layer (mirrors existing score_schema.sql pattern exactly)

```sql
cii_score_methodology      -- weights + inversions per metric (config-driven)
cii_subindex_weights       -- SI1=0.40, SI2=0.35, SI3=0.25
cii_score_metric_inputs    -- raw values snapshot at scoring time
cii_score_metric_normalized -- 0–100 normalized + weighted scores
cii_score_subindex         -- per sub-index composite with data_date_min/max
cii_score_final            -- final CII per country + rank
```

### Output Layer

```sql
cii_sc_gap
-- country_iso, sdi_normalized (0–5), cii_normalized (0–5),
-- sc_gap, interpretation (under_converting/near_parity/over_converting)
```

### Operational Layer

```sql
cii_collection_runs   -- run metadata (started_at, status, facilities_discovered, tavily_calls_used)
cii_collection_log    -- per-attempt audit trail (pass_type, query, source_url, confidence, elapsed_ms)
cii_data_gaps         -- open gaps with severity, gap_type, attempt_count, manual_review_required
```

### Views

```sql
v_cii_facilities_summary  -- installed_mw, committed_mw, committed_usd, avg_confidence per country
v_cii_latest_scores       -- final CII scores ranked (mirrors v_sdi_ranked)
v_cii_sc_gap_ranked       -- S-C Gap ranked with interpretation label
```

**Total: 13 tables + 3 views**

### Seeded Chip Access Tiers (as of 2026-05)

| Country | Tier | Label | Basis |
|---|---|---|---|
| US | 1 | Unrestricted latest-gen | Domestic producer — no restrictions |
| SG | 2 | Unrestricted prior-gen | BIS Tier 1 ally — H100/H200 unrestricted |
| IN | 2 | Unrestricted prior-gen | BIS Tier 2 — approved up to H100 class |
| BR | 2 | Unrestricted prior-gen | BIS Tier 2 — approved up to H100 class |
| PH | 2 | Unrestricted prior-gen | BIS Tier 2 — approved up to H100 class |
| AE | 3 | Restricted | Entity List concern; NVIDIA-Microsoft Emirati AI deal 2025 subject to conditions |

*Review dates set at 12 months. AE review at 6 months given active policy evolution.*

---

## 6. Confidence Scoring

Every data point carries a `confidence_score` (0–1) reflecting source quality and corroboration.

| Source Type | Confidence | Examples |
|---|---|---|
| Official company IR / government permit | 0.90 | AWS press release, DEWA permit filing |
| 2+ independent credible sources agree | 0.85 | Two trade publications confirm same MW |
| Single credible trade publication | 0.75 | DC Byte, Synergy Research, Structure Research |
| Research agent, multiple search angles | 0.70 | Tavily multi-query synthesis |
| Research agent, single source | 0.65 | Single news article extraction |
| Industry benchmark estimate | 0.50 | Hyperscale campus ~100MW assumption |

Scores feed directly into reporting — the client deliverable discloses confidence alongside every score.

---

## 7. Gap Handling

### Gap Types

| Type | Example | Handling |
|---|---|---|
| **Facility gap** | Missed a real data center | Wider discovery queries on retry |
| **Field gap** | Found facility, no exact MW | Benchmark estimate at confidence 0.50, flagged |
| **Metric gap** | Can't compute domestic ownership | Proxy or structural unknown |
| **Structural unknown** | PH has no frontier AI training — confirmed absence | Store `0`, `source='known_zero_documented'`, confidence 0.90 |
| **Structural unobservable** | Private operator, ownership undisclosed | Document in methodology, use sector proxy |

### Gap Severity and Response

| Severity | Trigger | Response |
|---|---|---|
| **CRITICAL** | Entire country missing from installed capacity | Auto-escalate to manual review after 1 failed attempt |
| **HIGH** | MW missing on facilities representing >20% of estimated national capacity | Retry with fresh queries; escalate after 2 failures |
| **MEDIUM** | investment_value_usd missing on some facilities | Retry queue; no escalation |
| **LOW** | energy_source, chip_type_if_known missing | Log only; not in scoring formula |
| **STRUCTURAL** | Confirmed absence or genuinely unobservable | Exclude from retry queue; document in methodology paper |

### Partial Scoring

If a sub-index has some gaps, the scoring engine does not drop the entire sub-index:

- **≥ 70% of weighted metrics available** → score computed; weights of available metrics re-normalized to sum to 1.0; flagged in output as partial
- **< 70% available** → sub-index score set to NULL; flagged as CRITICAL gap; CII composite computed from remaining sub-indexes with re-weighted final formula

### Retry Logic

```
attempt_count = 1  → retry with different query angles next quarterly run
attempt_count = 2  → retry + flag manual_review_required = TRUE
attempt_count = 3  → gap report surfaces with recommended_action for analyst
```

### Gap Report Output (cii_gap_report.py)

The gap report produces a prioritized action list for the analyst:

```
CRITICAL — requires manual resolution before scoring:
  [AE] installed_capacity_mw: 0 operational facilities found after 2 runs
  Recommended: Check DEWA annual report, EWEC capacity statements, TAQA IR

HIGH — degrades score quality:
  [BR] capacity_mw missing on 4 facilities (estimated ~35% of installed base)
  Recommended: Check Equinix Brazil, Ascenty, HostDime BR press kits directly

STRUCTURAL unknowns (excluded from retry, documented in methodology):
  [PH] frontier_training_present: No frontier model training confirmed — stored as 0
  [BR] frontier_training_present: No frontier model training confirmed — stored as 0
```

---

## 8. Failure Prevention

| Risk | Mitigation |
|---|---|
| Crash mid-collection | Every facility UPSERTed immediately — re-run resumes from where it left off |
| Duplicate facility records | `UNIQUE (country_iso, facility_name, operator)` enforced at DB level |
| Tavily quota exhaustion | Call counter in `cii_collection_runs`; warn at 80% of monthly budget; discovery queries batched to 6 per country |
| Pipeline multiplier undefined (installed = 0) | `pipeline_multiplier = NULL` when `installed_mw = 0`; excluded from normalization |
| SI2 NULL on first run | Expected behavior; `qoq_*` fields are nullable; scoring logs first-run caveat |
| Stale data re-collection | Staleness check per facility: skip enrichment if `last_updated > 80 days` |
| Cross-country MW normalization with outliers | Min-max normalization is run across 6 countries; if US dominates (likely), log the range so client sees the spread |

---

## 9. Trusted Search Domains (Per Country)

Research agent queries are scoped to these domains first, falling back to general search only when needed.

| Country | Primary Domains |
|---|---|
| US | aws.amazon.com, azure.microsoft.com, cloud.google.com, meta.com, oracle.com, datacenterknowledge.com, datacenterdynamics.com |
| AE | dewa.gov.ae, moei.gov.ae, ewec.ae, taqa.com, mubadala.com, g42.ai, datacenterdynamics.com |
| BR | equinix.com.br, ascenty.com, hostdime.com.br, anatel.gov.br, datacenterdynamics.com |
| IN | niti.gov.in, meity.gov.in, stpi.in, datacenterdynamics.com, economictimes.indiatimes.com |
| SG | edb.gov.sg, imda.gov.sg, ema.gov.sg, datacenterdynamics.com, straitstimes.com |
| PH | dict.gov.ph, peza.gov.ph, datacenterdynamics.com, businessmirror.com.ph |

Global: datacenterdynamics.com, datacenterknowledge.com, synergy-rp.com, structureresearch.net, cloudscene.com

---

## 10. Client Deliverable Format

Every quarterly CII report includes:

1. **CII Score Table** — ranked scores per country with sub-index breakdown
2. **S-C Gap Analysis** — normalized 0–5 comparison with SDI, interpretation label
3. **Confidence Disclosure** — weighted average confidence per country score
4. **Coverage Report** — % of metrics directly sourced vs estimated per country
5. **Facility Database** — full `cii_facilities` export (CSV) with source URLs
6. **Gap Log** — open gaps, severity, and recommended analyst actions
7. **Estimated values marked ⚠** — any benchmark-estimated field clearly labeled

---

## 11. Key Design Decisions

| Decision | Rationale |
|---|---|
| Facility-level DB as SI1 foundation | QoQ growth velocity (SI2) requires tracking when each facility was announced and committed — aggregating after the fact loses this signal |
| Stream writes during collection | Protects against mid-run crashes; ~150–250 facility records means no performance concern |
| SI2 derived, not collected | Growth velocity is math over SI1 time series — no additional API calls needed once SI1 is populated |
| Chip access tier seeded in schema | Policy classification changes infrequently and deterministically; search would add noise, not accuracy |
| `cii_raw_metrics` as uniform scoring input | Scoring pipeline reads one table regardless of sub-index; same pattern as existing `score_pipeline.py` |
| Partial scoring with re-weighting | Client receives a score with caveats rather than no score — more useful for quarterly reporting |
| Known-zero pattern for structural absences | Prevents genuine zeros (no frontier training in PH) from being treated as gaps and corrupting normalization |
| Self-contained `cii_grid_reference` | Avoids cross-database dependency on `subindex_1` for the grid strain denominator |

---

## 12. Estimated Data Volume

| Table | Estimated Rows | Notes |
|---|---|---|
| `cii_facilities` | 150–250 | Grows ~20–40 rows per quarter as new facilities announced |
| `cii_quarterly_snapshots` | 6 × quarters | 24 rows after 4 quarters |
| `cii_raw_metrics` | ~84 | 6 countries × 14 metrics |
| `cii_score_final` | 6 | One per country |
| `cii_sc_gap` | 6 | One per country |
| `cii_collection_log` | 500–1,500 per run | Audit trail only |

Total working data: well under 500 rows. Highly efficient to query and maintain.

---

## 13. Estimated Run Costs (Per Quarterly Run)

| Component | Estimate |
|---|---|
| Tavily searches | ~120–180 calls (6 countries × ~8 discovery + ~3–4 enrichment per facility) |
| Claude API (extraction + SI3 research) | ~$0.15–0.30 USD |
| All other APIs | Free |

*Tavily free tier: 1,000 calls/month. One CII quarterly run consumes ~15–18% of monthly quota.*

---

*Design by: Capstone Group | 2026-05-05*
*Status: Approved for implementation*
