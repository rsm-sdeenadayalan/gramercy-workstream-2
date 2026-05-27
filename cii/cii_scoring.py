import calendar
import os
import psycopg2
from datetime import date, timedelta
from dotenv import load_dotenv
from cii_collectors import COUNTRIES
from cii_si3_collectors import upsert_raw_metric

load_dotenv()

SDI_DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "dbname":   "gramercy_workstream1",
}


def _quarter_label(d: date) -> str:
    return f"{d.year}Q{(d.month - 1) // 3 + 1}"


def _quarter_end(d: date) -> date:
    q = (d.month - 1) // 3
    ends = [date(d.year, 3, 31), date(d.year, 6, 30),
            date(d.year, 9, 30), date(d.year, 12, 31)]
    return ends[q]


def _compute_si2_snapshot(conn, country_iso: str, quarter: str,
                           quarter_end: date) -> dict:
    """Aggregate facilities into a quarterly snapshot, as of `quarter_end`.

    Installed / committed capacity is measured *as of* the quarter-end date
    using facility dates: a facility is installed once date_operational has
    passed, committed once date_announced has passed. Facilities with no
    documented date are treated as pre-existing baseline (counted in every
    quarter), so QoQ growth is driven only by dated facilities."""
    with conn.cursor() as cur:
        # As-of-quarter-end aggregates (D = quarter_end)
        q = (quarter_end.month - 1) // 3
        q_start = date(quarter_end.year, [1, 4, 7, 10][q], 1)
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN status='operational'
                                   AND (date_operational IS NULL
                                        OR date_operational <= %(d)s)
                                  THEN capacity_mw ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status!='operational'
                                   AND (date_announced IS NULL
                                        OR date_announced <= %(d)s)
                                  THEN capacity_mw ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status!='operational' AND is_hyperscaler
                                   AND (date_announced IS NULL
                                        OR date_announced <= %(d)s)
                                  THEN investment_value_usd ELSE 0 END), 0),
                COUNT(CASE WHEN is_hyperscaler AND status!='operational'
                           AND date_announced BETWEEN %(q_start)s AND %(d)s
                           THEN 1 END)
            FROM cii_facilities WHERE country_iso = %(country)s
        """, {"d": quarter_end, "q_start": q_start, "country": country_iso})
        row = cur.fetchall()
        if not row or not row[0]:
            installed, committed, committed_usd, new_hs = 0.0, 0.0, 0.0, 0
        else:
            installed, committed, committed_usd, new_hs = (row[0][0] or 0.0,
                                                            row[0][1] or 0.0,
                                                            row[0][2] or 0.0,
                                                            row[0][3] or 0)

        # Previous quarter
        prev_month = ((quarter_end.month - 4) % 12) + 1
        prev_year  = quarter_end.year if quarter_end.month > 3 else quarter_end.year - 1
        # last day of the month before this quarter started
        prev_day   = calendar.monthrange(prev_year, prev_month)[1]
        prev_q_end = date(prev_year, prev_month, prev_day)

        cur.execute("""
            SELECT installed_mw, committed_mw, committed_usd  -- prev quarter lookup
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
        if prev_val is None or prev_val == 0:
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


def _write_snapshot(conn, snap: dict, run_id: str) -> None:
    """Upsert one quarterly snapshot row into cii_quarterly_snapshots."""
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


def compute_si2_all_countries(conn, run_id: str) -> None:
    """Compute SI2 quarterly snapshots for all 6 countries and write to DB.

    The immediately-preceding quarter is reconstructed date-aware from
    facility commissioning/announcement dates so QoQ growth has a baseline
    to compare against. Undated facilities are treated as pre-existing (they
    appear in both quarters and contribute 0 growth); growth is therefore
    driven only by facilities with documented dates. Only the current
    quarter's metrics are written to cii_raw_metrics for scoring — the
    previous quarter exists purely as the QoQ baseline."""
    today = date.today()
    cur_quarter = _quarter_label(today)
    cur_q_end   = _quarter_end(today)

    # Previous quarter end = day before the current quarter started.
    q = (cur_q_end.month - 1) // 3
    cur_q_start  = date(cur_q_end.year, [1, 4, 7, 10][q], 1)
    prev_q_end   = cur_q_start - timedelta(days=1)
    prev_quarter = _quarter_label(prev_q_end)

    print(f"  [SI2] computing {prev_quarter} (reconstructed baseline) + "
          f"{cur_quarter} snapshots for {len(COUNTRIES)} countries")

    for country_iso in COUNTRIES:
        # 1. Previous-quarter baseline first, so the current-quarter QoQ
        #    lookup finds something to compare against.
        prev_snap = _compute_si2_snapshot(conn, country_iso, prev_quarter, prev_q_end)
        _write_snapshot(conn, prev_snap, run_id)
        print(f"  [SI2:{country_iso}] {prev_quarter} baseline → "
              f"installed={prev_snap['installed_mw']} "
              f"committed={prev_snap['committed_mw']}")

        # 2. Current quarter.
        snap = _compute_si2_snapshot(conn, country_iso, cur_quarter, cur_q_end)
        _write_snapshot(conn, snap, run_id)
        print(f"  [SI2:{country_iso}] {cur_quarter} → installed={snap['installed_mw']} "
              f"committed={snap['committed_mw']} committed_usd={snap['committed_usd']}")
        print(f"  [SI2:{country_iso}]   pipeline_multiplier={snap['pipeline_multiplier']} "
              f"new_hyperscaler_commitments={snap['hyperscaler_commitments_count']}")
        print(f"  [SI2:{country_iso}]   QoQ installed={snap['qoq_installed_growth_rate']} "
              f"committed_mw={snap['qoq_committed_mw_growth_rate']} "
              f"committed_usd={snap['qoq_committed_usd_growth_rate']}")
        print(f"  [SI2:{country_iso}]   grid_mw={snap['national_grid_mw']} "
              f"grid_strain_ratio={snap['grid_strain_ratio']}")

        # 3. Raw metrics — current quarter only (these feed scoring).
        si1_metrics = {
            "installed_capacity_mw": (snap["installed_mw"],       "MW",    0.85),
            "committed_pipeline_mw": (snap["committed_mw"],       "MW",    0.80),
            "pipeline_multiplier":   (snap["pipeline_multiplier"], "ratio", 0.85),
        }
        si1_written = 0
        si1_skipped = []
        for mk, (val, unit, conf) in si1_metrics.items():
            if val is not None:
                upsert_raw_metric(conn, run_id, country_iso, "SI1",
                                  mk, val, unit, conf, "cii_facilities_aggregate")
                si1_written += 1
            else:
                si1_skipped.append(mk)
        print(f"  [SI2:{country_iso}]   wrote {si1_written} SI1 raw metrics"
              + (f" | skipped (null): {si1_skipped}" if si1_skipped else ""))

        si2_metrics = {
            "qoq_installed_growth_rate":     (snap["qoq_installed_growth_rate"],     "rate", 0.85),
            "qoq_committed_mw_growth_rate":  (snap["qoq_committed_mw_growth_rate"],  "rate", 0.85),
            "qoq_committed_usd_growth_rate": (snap["qoq_committed_usd_growth_rate"], "rate", 0.80),
            "new_hyperscaler_commitments":   (snap["hyperscaler_commitments_count"], "count", 0.85),
            "grid_strain_ratio":             (snap["grid_strain_ratio"],             "ratio", 0.80),
        }
        si2_written = 0
        si2_skipped = []
        for mk, (val, unit, conf) in si2_metrics.items():
            if val is not None:
                upsert_raw_metric(conn, run_id, country_iso, "SI2",
                                  mk, val, unit, conf, "cii_quarterly_snapshots")
                si2_written += 1
            else:
                si2_skipped.append(mk)
        print(f"  [SI2:{country_iso}]   wrote {si2_written} SI2 raw metrics"
              + (f" | skipped (null): {si2_skipped}" if si2_skipped else ""))


def compute_si3_derived(conn, run_id: str, country_iso: str) -> None:
    """Derive SI3 metrics computed from cii_facilities (not from search)."""
    print(f"  [SI3-derived:{country_iso}] computing from cii_facilities")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(DISTINCT operator), COALESCE(SUM(investment_value_usd), 0)
            FROM cii_facilities
            WHERE country_iso = %s AND is_hyperscaler = TRUE
        """, (country_iso,))
        row = cur.fetchone()

    hs_count  = int(row[0])  if row else 0
    hs_invest = float(row[1]) if row else 0.0

    print(f"  [SI3-derived:{country_iso}]   hyperscaler_count={hs_count} "
          f"hyperscaler_investment_usd={hs_invest}")

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
        print(f"  [SI3-derived:{country_iso}]   chip_access_tier={tier_row[0]} "
              f"(from cii_chip_access seed)")
        upsert_raw_metric(conn, run_id, country_iso, "SI3",
                          "chip_access_tier", float(tier_row[0]), "tier",
                          0.90, "cii_chip_access_seeded")
    else:
        print(f"  [SI3-derived:{country_iso}]   ○ no chip_access_tier seed row found — skipped")


def _minmax_normalize(values: dict, invert: bool = False) -> dict:
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
    # gap = SDI - CII (both normalized to 0-5).
    # gap < 0  → CII > SDI → over_converting (UAE model: importing substrate via capital)
    # gap > 0  → SDI > CII → under_converting (Brazil model: value leaking via commodity export)
    # |gap| ≤ 0.25 → near_parity (US model: converting at endowment rate)
    if gap < -0.25:
        return "over_converting"
    if gap > 0.25:
        return "under_converting"
    return "near_parity"


def run_scoring(conn, run_id: str) -> None:
    """Read cii_raw_metrics, normalize per metric, apply weights,
    compute sub-index composites and final CII score."""
    print(f"  [scoring] loading methodology + subindex weights")
    with conn.cursor() as cur:
        cur.execute("SELECT sub_index, metric_key, weight, invert FROM cii_score_methodology")
        methodology = {(r[0], r[1]): (r[2], r[3]) for r in cur.fetchall()}
        cur.execute("SELECT sub_index, weight FROM cii_subindex_weights")
        si_weights = dict(cur.fetchall())

    print(f"  [scoring]   loaded {len(methodology)} metric weights "
          f"| subindex weights: {si_weights}")

    countries = list(COUNTRIES.keys())
    today = date.today()

    print(f"  [scoring] loading latest raw metrics for {len(countries)} countries")
    raw: dict[tuple, float] = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, metric_key, metric_value, confidence_score
            FROM cii_raw_metrics
            WHERE (country_iso, metric_key, collected_at) IN (
                SELECT country_iso, metric_key, MAX(collected_at)
                FROM cii_raw_metrics GROUP BY country_iso, metric_key
            )
        """)
        for c_iso, mk, val, _ in cur.fetchall():
            raw[(c_iso, mk)] = val
    print(f"  [scoring]   loaded {len(raw)} (country, metric) raw values")

    subindex_scores: dict[str, dict[str, float]] = {si: {} for si in ("SI1", "SI2", "SI3")}

    print(f"  [scoring] normalizing + weighting {len(methodology)} metrics across countries")
    for (si, mk), (weight, invert) in methodology.items():
        country_vals = {c: raw.get((c, mk)) for c in countries}
        normed = _minmax_normalize(country_vals, invert=invert)
        present = sum(1 for v in country_vals.values() if v is not None)
        print(f"  [scoring]   [{si}/{mk}] weight={weight} invert={invert} "
              f"| {present}/{len(countries)} countries have data")
        for c_iso in countries:
            n = normed.get(c_iso)
            ws = round(n * weight, 6) if n is not None else None
            raw_v = raw.get((c_iso, mk))
            print(f"  [scoring]     {c_iso}: raw={raw_v} → norm={n} → weighted={ws}")

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

    print(f"  [scoring] writing sub-index composites")
    final_scores: dict[str, dict] = {}
    for si, scores in subindex_scores.items():
        si_weight = si_weights.get(si, 0.0)
        print(f"  [scoring]   {si} (weight={si_weight}):")
        for c_iso, score in scores.items():
            weighted = round(score * si_weight, 6)
            print(f"  [scoring]     {c_iso}: composite={round(score, 4)} "
                  f"→ weighted={weighted}")
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
                      weighted, today, today))
            conn.commit()
            if c_iso not in final_scores:
                final_scores[c_iso] = {"SI1": None, "SI2": None, "SI3": None}
            final_scores[c_iso][si] = score

    cii_vals = {}
    for c_iso, si_map in final_scores.items():
        total = sum(
            (si_map[si] or 0.0) * si_weights.get(si, 0.0)
            for si in ("SI1", "SI2", "SI3")
        )
        cii_vals[c_iso] = round(total, 4)

    ranked = sorted(cii_vals.items(), key=lambda x: x[1], reverse=True)
    print(f"  [scoring] final CII ranking:")
    for rank, (c_iso, cii_score) in enumerate(ranked, 1):
        si_map = final_scores.get(c_iso, {})
        print(f"  [scoring]   #{rank} {c_iso}: CII={cii_score} "
              f"(SI1={round(si_map.get('SI1') or 0, 2)} "
              f"SI2={round(si_map.get('SI2') or 0, 2)} "
              f"SI3={round(si_map.get('SI3') or 0, 2)})")
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
    print(f"  [SC-Gap] loading CII final scores for run_id={run_id}")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, cii_score FROM cii_score_final WHERE run_id = %s
        """, (run_id,))
        cii_rows = dict(cur.fetchall())
    print(f"  [SC-Gap]   loaded {len(cii_rows)} CII scores: {dict(cii_rows)}")

    print(f"  [SC-Gap] fetching SDI scores from csi_scores DB "
          f"({SDI_DB_CONFIG['host']}:{SDI_DB_CONFIG['port']})")
    try:
        sdi_conn = psycopg2.connect(**SDI_DB_CONFIG)
        try:
            with sdi_conn.cursor() as cur:
                cur.execute("SELECT country_iso, sdi_score FROM score_sdi")
                sdi_rows = dict(cur.fetchall())
        finally:
            sdi_conn.close()
        print(f"  [SC-Gap]   loaded {len(sdi_rows)} SDI scores: {dict(sdi_rows)}")
    except Exception as exc:
        print(f"  [SC-Gap]   ✗ SDI fetch FAILED: {exc} — proceeding with no SDI data")
        sdi_rows = {}

    today = date.today()
    written = 0
    skipped_no_cii = 0
    skipped_no_sdi = 0
    for c_iso in COUNTRIES:
        cii_raw = cii_rows.get(c_iso)
        sdi_raw = sdi_rows.get(c_iso)
        if cii_raw is None:
            print(f"  [SC-Gap]   {c_iso}: ○ no CII score for this run — skipping")
            skipped_no_cii += 1
            continue
        cii_norm = round(cii_raw / 20.0, 4)
        sdi_norm = round(sdi_raw / 20.0, 4) if sdi_raw else None
        gap      = round(sdi_norm - cii_norm, 4) if sdi_norm is not None else None
        interp   = _interpret_sc_gap(gap) if gap is not None else None

        if sdi_norm is None:
            print(f"  [SC-Gap]   {c_iso}: CII={cii_raw} (norm={cii_norm}) "
                  f"| SDI missing → gap=NULL")
            skipped_no_sdi += 1
        else:
            print(f"  [SC-Gap]   {c_iso}: CII={cii_raw}→{cii_norm} "
                  f"SDI={sdi_raw}→{sdi_norm} | gap={gap} → {interp}")

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
        written += 1

    print(f"  [SC-Gap] complete: {written} rows written "
          f"| skipped: {skipped_no_cii} no-CII, {skipped_no_sdi} no-SDI")
