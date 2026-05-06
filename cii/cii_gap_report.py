from dotenv import load_dotenv
from cii_collectors import get_conn

load_dotenv()


def build_gap_report(conn) -> dict:
    """Query cii_data_gaps and return prioritised sections."""
    report = {"critical": [], "high": [], "medium": [], "structural": []}

    for sev in ("critical", "high", "medium", "structural"):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT country_iso, metric_key, facility_name, failure_reason,
                       attempt_count, recommended_action, manual_review_required
                FROM cii_data_gaps
                WHERE severity = %s AND status = 'open'
                ORDER BY attempt_count DESC, country_iso
            """, (sev,))
            for row in cur.fetchall():
                report[sev].append({
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
