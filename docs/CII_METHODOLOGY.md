# Compute Infrastructure Index (CII) — Methodology Paper

**Workstream 2 — Gramercy**

---

## 1. Purpose & Framework

The Compute Infrastructure Index (CII) measures, by country, the **scale, growth rate, and
quality of AI compute infrastructure** being built and operated.

The CII exists to capture a distinction that raw resource endowment cannot: the gap between
*owning physical resources* (measured by the Strategic Datacenter Index, SDI) and *converting
those resources into operational AI infrastructure*. A country can hold abundant power, land,
and capital and still fail to convert it into compute — or convert beyond its endowment by
importing substrate through capital.

The index covers **6 target countries**: United States (US), United Arab Emirates (AE),
Brazil (BR), India (IN), Singapore (SG), and the Philippines (PH).

The CII is a composite of three sub-indices:

| Sub-index | Name | Weight |
|-----------|------|--------|
| SI1 | Installed and Committed Capacity | 0.40 |
| SI2 | Growth Velocity | 0.35 |
| SI3 | Compute Quality and Access | 0.25 |

```
CII = (0.40 × SI1) + (0.35 × SI2) + (0.25 × SI3)
```

---

## 2. Sub-Indices

### 2.1 SI1 — Installed and Committed Capacity (weight 0.40)

Measures current operational AI compute capacity and the committed pipeline.

| Metric | Weight | Direction |
|--------|--------|-----------|
| `installed_capacity_mw` — operational data center capacity (MW) | 0.40 | higher = better |
| `committed_pipeline_mw` — permitted + under-construction + announced (MW) | 0.40 | higher = better |
| `pipeline_multiplier` — committed ÷ installed | 0.20 | higher = better |

`pipeline_multiplier` is NULL when installed capacity is zero (division undefined).

### 2.2 SI2 — Growth Velocity (weight 0.35)

Measures the *rate* at which compute capacity is scaling — acceleration is the signal, not
the level. Computed quarter-over-quarter (QoQ) from `cii_quarterly_snapshots`.

| Metric | Weight | Direction |
|--------|--------|-----------|
| `qoq_installed_growth_rate` — QoQ growth of operational capacity | 0.40 | higher = better |
| `qoq_committed_mw_growth_rate` — QoQ growth of committed pipeline (MW) | 0.20 | higher = better |
| `qoq_committed_usd_growth_rate` — QoQ growth of committed pipeline (USD) | 0.10 | higher = better |
| `new_hyperscaler_commitments` — new hyperscaler announcements this quarter | 0.20 | higher = better |
| `grid_strain_ratio` — committed capacity ÷ national grid capacity | 0.10 | **inverted** |

The methodology's "pipeline growth rate" weight of 0.30 is split across the MW (0.20) and
USD (0.10) components. `grid_strain_ratio` is inverted: a higher strain ratio indicates
infrastructure stress and therefore *lowers* the score.

### 2.3 SI3 — Compute Quality and Access (weight 0.25)

Measures the *quality* of compute being deployed — not just how much, but what kind and who
controls it.

| Metric | Weight | Direction |
|--------|--------|-----------|
| `chip_access_tier` — 5-tier scale (1 = unrestricted latest-gen, 5 = denied/sanctioned) | 0.40 | **inverted** |
| `hyperscaler_count` — distinct major cloud/AI platform operators | 0.20 | higher = better |
| `hyperscaler_investment_usd` — total committed hyperscaler investment | 0.15 | higher = better |
| `domestic_ownership_ratio` — domestic-operated capacity ÷ total | 0.15 | higher = better |
| `frontier_training_present` — 1 if confirmed in-jurisdiction frontier training | 0.10 | higher = better |

The methodology's "hyperscaler diversity" weight of 0.35 is split across operator count
(0.20) and investment value (0.15). `chip_access_tier` is inverted so that tier 1
(unrestricted) scores highest.

**Chip access tiers (seeded reference data):**

| Country | Tier | Label | Basis |
|---------|------|-------|-------|
| US | 1 | unrestricted_latest_gen | Domestic producer — no export restrictions |
| SG | 2 | unrestricted_prior_gen | BIS Tier 1 ally — H100/H200 unrestricted |
| IN | 2 | unrestricted_prior_gen | BIS Tier 2 — approved up to H100 class |
| BR | 2 | unrestricted_prior_gen | BIS Tier 2 — approved up to H100 class |
| PH | 2 | unrestricted_prior_gen | BIS Tier 2 — approved up to H100 class |
| AE | 3 | restricted | BIS Entity List concern; conditional 2025 deal |

---

## 3. Normalization Method

Each metric is normalized **across countries** to a 0–100 scale using min-max normalization:

```
normalized = (value − min) / (max − min) × 100
```

- For **inverted** metrics (`grid_strain_ratio`, `chip_access_tier`), the result is
  `100 − normalized` so that lower raw values score higher.
- When all countries share the same value, every country receives 50.0 (neutral).
- Missing values (NULL) receive no score and contribute 0 to the weighted sub-index total.

Each normalized metric is multiplied by its weight to produce a `weighted_score`. The
sub-index composite is the sum of its metrics' weighted scores (also 0–100, since the
metric weights within each sub-index sum to 1.0).

---

## 4. Final CII Calculation

The final CII is the sub-index-weighted sum:

```
CII = (0.40 × SI1) + (0.35 × SI2) + (0.25 × SI3)
```

Because each sub-index is 0–100 and the sub-index weights sum to 1.0, the final CII is
also on a 0–100 scale. Countries are then ranked by CII score (descending).

Outputs are written to:
- `cii_score_metric_normalized` — per-metric raw → normalized → weighted
- `cii_score_subindex` — per-country sub-index composites
- `cii_score_final` — per-country final CII score and rank

---

## 5. The S-C Gap

The S-C Gap is an additional analytical output comparing **substrate** (SDI) against
**conversion** (CII):

```
S-C Gap = SDI(normalized 0–5) − CII(normalized 0–5)
```

Both indices are normalized to a 0–5 scale (raw 0–100 score ÷ 20) for comparability.

| Gap | Condition | Interpretation | Archetype |
|-----|-----------|----------------|-----------|
| Gap < −0.25 | CII > SDI | **over_converting** — converting beyond resource base, importing substrate through capital | UAE model |
| −0.25 ≤ Gap ≤ 0.25 | CII ≈ SDI | **near_parity** — converting at roughly the rate the endowment supports | US model |
| Gap > 0.25 | SDI > CII | **under_converting** — value leaking through commodity export | Brazil model |

The S-C Gap requires SDI scores from Workstream 1 (`csi_scores.score_sdi`). When the SDI
database is unavailable, the CII pipeline still records its own scores and marks the gap
as NULL.

---

## 6. What Counts as AI Compute Infrastructure

The CII deliberately measures **AI compute infrastructure**, not data center capacity in
general. This scoping decision is central to the index's validity, and the collection
logic enforces it explicitly.

### 6.1 Why AI-specific, not all data centers

The entire purpose of the CII — and of the SDI/CII pairing — is to separate *substrate*
from *conversion*. SDI measures the physical and capital base: power, land, connectivity,
financing. CII measures how much of that base has been converted into compute that can
actually train and serve AI models.

A country can host a large number of generic enterprise data centers — running CRM
systems, email, web hosting, database backends, disaster recovery — and still have very
little AI compute. Those facilities are cloud infrastructure, but they are not AI
infrastructure. Counting them would inflate the CII and destroy the very signal the index
is built to isolate: the conversion of resources into *AI-capable* compute. The S-C Gap
would become meaningless, because both sides would partly measure the same generic
substrate.

So the inclusion rule is strict by design: **a facility counts toward the CII only if it
is AI compute infrastructure.**

### 6.2 What qualifies

A facility is treated as AI compute infrastructure if any of the following hold:

- It contains **GPU / accelerator clusters** — Nvidia H100/H200/Blackwell, Google TPU,
  AWS Trainium, AMD MI300, or comparable AI silicon.
- It is **announced for AI/ML workloads** — LLM training, large-scale inference, or
  frontier model training.
- It is a **hyperscale campus operated by a major cloud/AI platform** (see 6.3).

### 6.3 The hyperscaler-operator heuristic

The current global data center buildout is overwhelmingly AI-capex-driven. When Microsoft,
AWS, Google, Meta, Oracle, Nvidia, CoreWeave, OpenAI, or xAI announce new hyperscale
capacity from 2023 onward, that marginal capacity is going to AI compute — GPU clusters
for training and inference. Their non-AI cloud footprint is comparatively static; the
growth is AI.

Requiring every such facility to carry an explicit "this is for AI" quote from a news
snippet would discard real signal. Trade press and investor communications routinely
report a new campus's capacity (MW) and investment (USD) without spelling out the
workload, because for these operators the workload is assumed. An evidence gate that
demanded a verbatim AI mention would therefore reject genuine AI infrastructure simply
because the source was terse.

The CII resolves this with a heuristic: **facilities operated by a major cloud/AI platform
are included as AI compute by default — the operator's identity is itself the evidence.**
The platform set used by the collector is: Microsoft / Azure, Amazon / AWS, Google, Meta,
Oracle, Apple, Alibaba, Tencent, Huawei, Nvidia, CoreWeave, OpenAI, xAI, Anthropic.

### 6.4 The evidence gate for ambiguous operators

For **non-platform operators** — regional colocation providers, telecom carriers, generic
enterprise builders — operator identity is *not* sufficient evidence. A facility from one
of these operators is included only if the source text provides explicit AI evidence: a
GPU/accelerator mention, an AI/ML workload reference, or a training-use statement.

If no such evidence exists, the facility is excluded as likely generic cloud or
colocation. This is the precision half of the rule: the heuristic in 6.3 keeps recall
high for the operators that matter, while the evidence gate keeps generic infrastructure
out.

### 6.5 Explicit exclusions

The following are never counted as AI compute, regardless of operator:

- **Pure CDN / edge POPs** — content delivery, not training or inference compute.
- **Telecom switching / carrier hotels** — connectivity infrastructure, not compute.
- **Crypto-mining facilities** — different hardware class, not AI.
- **Disaster-recovery-only sites** — cold standby, not active compute.
- **Small enterprise / colocation** facilities with no cloud-platform or AI tie.

### 6.6 Source-quality filtering

Before extraction, the discovery stage filters out low-signal sources:

- **Social media** (LinkedIn, Twitter/X, Facebook, Instagram, TikTok, YouTube) and
  **personal blogs / newsletters** (Medium, Substack, WordPress, Blogspot) and
  **forums** (Reddit, Quora) are dropped entirely. These carry promotional or
  unverifiable claims with no editorial standard.
- **Catalog sites** that index all cloud/colocation indiscriminately
  (`cloudinfrastructuremap.com`, `datacentermap.com`, `baxtel.com`) are flagged
  "cautious" — usable as corroboration but assigned lower confidence and never used as
  the sole basis for a facility.

---

## 7. Data Collection Pipeline

The pipeline (`cii/run_cii.py`) runs in three phases.

### Phase 1 — SI1 facility collection + SI3 research (parallel)

Six countries are processed concurrently (thread pool, 4 workers). For each country, SI1
runs a three-pass collection:

**Discovery pass** — 8 AI-specific search queries per country (GPU campuses, hyperscaler
AI investment, H100/H200/Blackwell deployments, sovereign AI compute, etc.). Each query:
1. Calls the Tavily web search API (`search_depth=advanced`).
2. Filters out low-signal sources per Section 6.6.
3. Passes results to Claude (Haiku) for structured facility extraction, applying the
   inclusion criteria in Section 6.

**Enrichment pass** — for each discovered facility, 3 targeted queries fill in missing
fields (capacity MW, investment USD, operational dates). The facility's discovery-assigned
`status` and `ownership_type` are preserved (enrichment is field-level only).

**Validation pass** — assigns final confidence based on source agreement:
- Multi-source + known MW → `multi_source` (0.85)
- Single-source + known MW → `agent_single` (0.65)
- MW still missing → hyperscaler benchmark estimate applied, `benchmark_est` (0.50),
  and a data gap is recorded.

SI3 research (domestic ownership ratio, frontier training presence) runs in parallel with
SI1 across the same thread pool.

### Phase 2 — SI2 computation + SI3 derived metrics (sequential)

SI2 quarterly snapshots are computed from the populated `cii_facilities` table: installed
vs committed MW, pipeline multiplier, QoQ growth rates, hyperscaler commitment counts, and
grid strain ratio. SI3-derived metrics (hyperscaler count, hyperscaler investment, chip
access tier) are computed from facilities and seeded reference tables.

### Phase 3 — Scoring + S-C Gap (sequential)

Normalization, weighting, sub-index composites, final CII, ranking, and the S-C Gap
computation against SDI scores.

### Confidence scoring scheme

| Tier | Score | Meaning |
|------|-------|---------|
| `official_ir` | 0.90 | Official investor-relations / government source |
| `multi_source` | 0.85 | Corroborated by multiple independent sources |
| `trade_pub` | 0.75 | Trade publication (DCD, DCK) |
| `agent_multi` | 0.70 | Agent-extracted, multiple sources |
| `agent_single` | 0.65 | Agent-extracted, single source |
| `benchmark_est` | 0.50 | Hyperscaler benchmark estimate (MW imputed) |

---

## 8. Country Results

The index produces the following per-country outputs, refreshed each quarterly run:

- **Final CII score and rank** — `cii_score_final`, surfaced via `v_cii_latest_scores`.
- **Sub-index breakdown** — SI1 / SI2 / SI3 composites per country, in
  `cii_score_subindex`.
- **Facility-level capacity tables** — every collected facility with capacity, status,
  operator, investment, confidence, and source URLs, in `cii_facilities`; aggregated in
  `v_cii_facilities_summary`.
- **S-C Gap per country** — gap value, archetype interpretation, and rank, in
  `cii_sc_gap`, surfaced via `v_cii_sc_gap_ranked`.
- **Growth-velocity time series** — quarterly snapshots accumulate in
  `cii_quarterly_snapshots`; QoQ rates become available once two or more quarters exist.

---

## 9. Data Gaps & Limitations

- **Single-quarter snapshot.** SI2 growth velocity needs at least two quarterly snapshots
  to produce non-NULL QoQ rates. The first quarterly run establishes a baseline; growth
  signal appears from the second quarterly run onward.

- **Grid reference data.** `grid_strain_ratio` depends on `cii_grid_reference` being
  seeded with national grid capacity per country.

- **SDI dependency.** The S-C Gap requires the Workstream 1 SDI scores
  (`csi_scores.score_sdi`). Without that database the gap is recorded as NULL while CII
  scores are still produced.

- **Source quality.** Capacity figures from announcements may be aspirational; the
  confidence scoring scheme and the `has_estimated_fields` flag track this. Facilities
  with imputed (benchmark) MW are explicitly marked.

- **Gap tracking.** All collection failures and missing metrics are recorded in
  `cii_data_gaps` with severity and recommended action, surfaced via the end-of-run gap
  report.

---

## Appendix — Database Schema (key tables)

| Table | Purpose |
|-------|---------|
| `cii_facilities` | Facility-level records (the SI1 raw data) |
| `cii_quarterly_snapshots` | Per-country quarterly aggregates (SI2 source) |
| `cii_raw_metrics` | All normalized-ready metric values per country |
| `cii_chip_access` | Seeded chip-access tier reference |
| `cii_grid_reference` | Seeded national grid capacity reference |
| `cii_score_metric_normalized` | Per-metric normalization detail |
| `cii_score_subindex` | Per-country sub-index composites |
| `cii_score_final` | Per-country final CII score and rank |
| `cii_sc_gap` | S-C Gap output |
| `cii_collection_runs` | Run-level operational log |
| `cii_collection_log` | Per-query attempt log |
| `cii_data_gaps` | Tracked data gaps and recommended actions |
