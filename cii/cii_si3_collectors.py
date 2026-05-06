import os, json, re, time
import anthropic
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
        text = resp.content[0].text.strip()
        match = re.search(r"```[a-zA-Z]*\n?(.*?)```", text, re.DOTALL)
        text = match.group(1).strip() if match else text
        return json.loads(text)
    except Exception:
        return {}


def collect_domestic_ownership(conn, run_id: str, country_iso: str) -> None:
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
        text = resp.content[0].text.strip()
        match = re.search(r"```[a-zA-Z]*\n?(.*?)```", text, re.DOTALL)
        text = match.group(1).strip() if match else text
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
