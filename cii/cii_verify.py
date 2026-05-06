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
    try:
        conn = get_conn()
    except Exception as exc:
        print(f"  ✗ Cannot connect to database — {exc}")
        sys.exit(1)
    print("\nCII Post-Run Verification")
    print("=" * 50)
    all_pass = True
    try:
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
    finally:
        conn.close()
    print()
    sys.exit(0 if all_pass else 1)
