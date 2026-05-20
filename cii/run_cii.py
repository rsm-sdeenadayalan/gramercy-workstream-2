"""
CII Pipeline Orchestrator
Usage: python run_cii.py [--only si1|si3|si2|scoring|gap]
"""
import os, sys, uuid, time, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

load_dotenv()
sys.path.insert(0, os.path.dirname(__file__))

from cii_collectors import (
    get_conn, COUNTRIES,
    run_discovery_pass, run_enrichment_pass, run_validation_pass,
)
from cii_si3_collectors import collect_domestic_ownership, collect_frontier_training
from cii_scoring import (
    compute_si2_all_countries, compute_si3_derived,
    run_scoring, compute_sc_gap,
)
from cii_gap_report import build_gap_report, print_gap_report

TAVILY_MONTHLY_BUDGET = 900   # warn at 90% of 1000 free tier


def _check_tavily_quota(conn) -> bool:
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

    if only == "gap":
        try:
            print_gap_report(build_gap_report(conn))
        finally:
            conn.close()
        return

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
            if not _check_tavily_quota(conn):
                print("  Aborting: Tavily quota exhausted.")
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE cii_collection_runs SET status='failed', "
                        "finished_at=NOW(), notes='Aborted: Tavily quota exhausted' "
                        "WHERE run_id=%s", (run_id,)
                    )
                conn.commit()
                return

            tasks = (
                [(_run_si1_country,         run_id, c) for c in COUNTRIES] +
                [(_run_si3_research_country, run_id, c) for c in COUNTRIES]
            )
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {pool.submit(fn, *args): f"{fn.__name__}-{args[-1]}"
                           for fn, *args in tasks}
                results = []
                for fut in as_completed(futures):
                    result = fut.result()
                    results.append(result)
                    status_mark = "✗" if "FAILED" in result else "✓"
                    print(f"  {status_mark} {futures[fut]}: {result}")
                failed = [r for r in results if "FAILED" in r]
                if failed:
                    print(f"\n  ⚠ {len(failed)} worker(s) failed in Phase 1 — proceeding with partial data")

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
