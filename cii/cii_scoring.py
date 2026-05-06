import calendar
import os
import psycopg2
from datetime import date
from dotenv import load_dotenv
from cii_collectors import COUNTRIES
from cii_si3_collectors import upsert_raw_metric

load_dotenv()

SDI_DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5432)),
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "dbname":   "csi_scores",
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
    """Aggregate facilities into a quarterly snapshot; compute QoQ rates."""
    with conn.cursor() as cur:
        # Current quarter aggregates
        q = (quarter_end.month - 1) // 3
        q_start = date(quarter_end.year, [1, 4, 7, 10][q], 1)
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE WHEN status='operational' THEN capacity_mw ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status!='operational' THEN capacity_mw ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status!='operational' AND is_hyperscaler
                                  THEN investment_value_usd ELSE 0 END), 0),
                COUNT(CASE WHEN is_hyperscaler AND status!='operational'
                           AND date_announced BETWEEN %s AND %s THEN 1 END)
            FROM cii_facilities WHERE country_iso = %s
        """, (q_start, quarter_end, country_iso))
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

        # Write SI1 + SI2 metrics to cii_raw_metrics for scoring
        si1_metrics = {
            "installed_capacity_mw": (snap["installed_mw"],       "MW",    0.85),
            "committed_pipeline_mw": (snap["committed_mw"],       "MW",    0.80),
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
    """Derive SI3 metrics computed from cii_facilities (not from search)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(DISTINCT operator), COALESCE(SUM(investment_value_usd), 0)
            FROM cii_facilities
            WHERE country_iso = %s AND is_hyperscaler = TRUE
        """, (country_iso,))
        row = cur.fetchone()

    hs_count  = int(row[0])  if row else 0
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
    if gap < -0.25:
        return "under_converting"
    if gap > 0.25:
        return "over_converting"
    return "near_parity"


def run_scoring(conn, run_id: str) -> None:
    """Read cii_raw_metrics, normalize per metric, apply weights,
    compute sub-index composites and final CII score."""
    with conn.cursor() as cur:
        cur.execute("SELECT sub_index, metric_key, weight, invert FROM cii_score_methodology")
        methodology = {(r[0], r[1]): (r[2], r[3]) for r in cur.fetchall()}
        cur.execute("SELECT sub_index, weight FROM cii_subindex_weights")
        si_weights = dict(cur.fetchall())

    countries = list(COUNTRIES.keys())
    today = date.today()

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

    subindex_scores: dict[str, dict[str, float]] = {si: {} for si in ("SI1", "SI2", "SI3")}

    for (si, mk), (weight, invert) in methodology.items():
        country_vals = {c: raw.get((c, mk)) for c in countries}
        normed = _minmax_normalize(country_vals, invert=invert)

        for c_iso in countries:
            n = normed.get(c_iso)
            ws = round(n * weight, 6) if n is not None else None

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
    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, cii_score FROM cii_score_final WHERE run_id = %s
        """, (run_id,))
        cii_rows = dict(cur.fetchall())

    try:
        sdi_conn = psycopg2.connect(**SDI_DB_CONFIG)
        with sdi_conn.cursor() as cur:
            cur.execute("SELECT country_iso, sdi_score FROM score_sdi")
            sdi_rows = dict(cur.fetchall())
        sdi_conn.close()
    except Exception:
        sdi_rows = {}

    today = date.today()
    for c_iso in COUNTRIES:
        cii_raw = cii_rows.get(c_iso)
        sdi_raw = sdi_rows.get(c_iso)
        if cii_raw is None:
            continue
        cii_norm = round(cii_raw / 20.0, 4)
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
