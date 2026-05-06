import calendar
import os
from datetime import date
import psycopg2
from dotenv import load_dotenv
from cii_collectors import COUNTRIES, get_conn
from cii_si3_collectors import upsert_raw_metric

load_dotenv()

SDI_DB_CONFIG = {
    "host":     os.environ.get("POSTGRES_HOST", "localhost"),
    "port":     int(os.environ.get("POSTGRES_PORT", 5433)),
    "dbname":   "csi_scores",
    "user":     os.environ.get("POSTGRES_USER", ""),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
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
        q_start = date(quarter_end.year,
                       ((quarter_end.month - 4) % 12) + 1,
                       1)
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
