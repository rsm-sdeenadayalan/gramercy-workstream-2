import os, json, time, re, anthropic
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
    "Microsoft": 100.0, "Amazon": 100.0, "AWS": 100.0, "Google": 80.0,
    "Meta": 80.0, "Oracle": 60.0, "default": 50.0,
}

HYPERSCALER_OPERATORS = {
    "Microsoft", "Azure", "Amazon", "AWS", "Google", "Meta", "Oracle",
    "Apple", "Alibaba", "Tencent", "Huawei",
    # AI-native compute platforms — "major cloud/AI platforms" per SI3 methodology
    "Nvidia", "NVIDIA", "CoreWeave", "OpenAI", "xAI", "Anthropic",
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
    "{country} AI training data center GPU campus capacity MW {year}",
    "{country} hyperscaler AI infrastructure investment announcement {year}",
    "{country} Microsoft Azure AI data center GPU cluster",
    "{country} AWS Amazon AI ML data center accelerator",
    "{country} Google cloud TPU AI data center",
    "{country} Meta AI supercomputer data center campus",
    "{country} Nvidia H100 H200 Blackwell data center deployment {year}",
    "{country} sovereign AI compute infrastructure frontier model training {year}",
]

# Sources we explicitly reject (low signal, promotional, unverifiable MW claims).
DENY_DOMAINS = {
    "linkedin.com", "twitter.com", "x.com", "facebook.com",
    "instagram.com", "tiktok.com", "youtube.com",
    "medium.com", "substack.com", "wordpress.com", "blogspot.com",
    "reddit.com", "quora.com",
}

# Sources that catalog ALL cloud/colocation (not AI-specific). Acceptable as
# corroboration, but require a second non-cautious source for inclusion.
CAUTIOUS_DOMAINS = {
    "cloudinfrastructuremap.com", "datacentermap.com", "baxtel.com",
}


def _domain_of(url: str) -> str:
    m = re.search(r"https?://([^/]+)", url or "")
    host = (m.group(1) if m else "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _filter_results(results: list[dict]) -> tuple[list[dict], list[str]]:
    """Drop deny-listed sources. Return (kept, dropped_urls)."""
    kept, dropped = [], []
    for r in results:
        host = _domain_of(r.get("url", ""))
        if any(host == d or host.endswith("." + d) for d in DENY_DOMAINS):
            dropped.append(r.get("url", ""))
        else:
            kept.append(r)
    return kept, dropped


KNOWN_ZERO_FRONTIER = {"PH", "BR"}   # confirmed no frontier AI training


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def web_search(query: str, count: int = 5) -> list[dict]:
    key = os.environ.get("TAVILY_API_KEY", TAVILY_API_KEY)
    if not key:
        raise ValueError("TAVILY_API_KEY not set in .env")
    from tavily import TavilyClient
    client = TavilyClient(api_key=key)
    # Request more than `count` so the deny-list filter still leaves enough.
    resp = client.search(query, max_results=count + 4,
                         include_raw_content=False, search_depth="advanced")
    raw = [
        {"url": r.get("url", ""), "title": r.get("title", ""),
         "content": r.get("content", "") or ""}
        for r in resp.get("results", [])
    ]
    kept, dropped = _filter_results(raw)
    if dropped:
        print(f"      web_search filter dropped {len(dropped)} low-signal source(s): "
              f"{[_domain_of(u) for u in dropped]}")
    return kept[:count]


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
    prompt = f"""Extract data center facilities serving AI compute for {country_name} ({country_iso}).

CONTEXT: The current data center buildout by major cloud/AI platforms is driven
by AI demand. Treat any facility operated by a major cloud/AI platform as AI
compute infrastructure unless the source explicitly says it is non-AI.

INCLUDE:
  - Any data center operated by a major cloud/AI platform (Microsoft, Azure,
    Amazon, AWS, Google, Meta, Oracle, Nvidia, OpenAI, Anthropic, xAI,
    CoreWeave, Alibaba, Tencent, Huawei, and similar)
  - GPU / accelerator clusters (Nvidia H100/H200/Blackwell, TPU, Trainium, MI300)
  - Facilities announced for AI/ML workloads, LLM training, or large-scale inference
  - Frontier model training sites or sovereign AI compute projects
  - Large hyperscale data center campuses announced 2023 or later

EXCLUDE only clearly non-AI infrastructure:
  - Pure CDN / edge POPs, telecom switching, carrier hotels
  - Crypto-mining facilities, disaster-recovery-only sites
  - Small enterprise/colocation facilities with no cloud-platform or AI tie

Return a JSON array. Each item MUST have these exact keys:
  facility_name (string), operator (string), capacity_mw (number or null),
  status (one of: operational, permitted, under_construction, announced),
  date_announced (YYYY-MM-DD or null), date_operational (YYYY-MM-DD or null),
  investment_value_usd (number in USD or null), energy_source (string or null),
  chip_type_if_known (string or null),
  ownership_type (one of: domestic, foreign, joint_venture, unknown),
  is_hyperscaler (true/false), source_url (string),
  ai_evidence (string: one sentence on why this is AI compute — quote the
               source if it mentions AI/GPU/training; otherwise state that the
               operator is a major AI/cloud platform. Use null only if the
               facility is genuinely not tied to AI or any cloud platform.)

Return ONLY the JSON array. No explanation. If the sources contain no data
center facilities at all, return []."""

    try:
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        text = resp.content[0].text.strip()
        # strip markdown code fences if present
        match = re.search(r"```[a-zA-Z]*\n?(.*?)```", text, re.DOTALL)
        text = match.group(1).strip() if match else text
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
                source_urls          = (
                    SELECT ARRAY(SELECT DISTINCT unnest
                                 FROM unnest(COALESCE(cii_facilities.source_urls, ARRAY[]::TEXT[]) ||
                                             COALESCE(EXCLUDED.source_urls, ARRAY[]::TEXT[])))
                ),
                source_count         = GREATEST(EXCLUDED.source_count, cii_facilities.source_count),
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
    norm_mk = metric_key if metric_key is not None else ""
    norm_fn = facility_name if facility_name is not None else ""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cii_data_gaps
                (country_iso, metric_key, facility_name, gap_type,
                 failure_reason, severity, recommended_action,
                 manual_review_required)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (country_iso, norm_mk or None, norm_fn or None,
              gap_type, failure_reason, severity, recommended_action, False))

        if cur.rowcount == 0:
            # Row already existed — update it
            cur.execute("""
                UPDATE cii_data_gaps
                SET attempt_count = attempt_count + 1,
                    last_attempted = NOW(),
                    failure_reason = %s,
                    manual_review_required = (attempt_count + 1) >= 2
                WHERE country_iso = %s
                  AND COALESCE(metric_key, '') = %s
                  AND COALESCE(facility_name, '') = %s
            """, (failure_reason, country_iso, norm_mk, norm_fn))
    conn.commit()


def run_discovery_pass(conn, run_id: str, country_iso: str) -> int:
    """Pass 1: enumerate all AI data center facilities for a country.
    Returns number of facilities discovered."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    country_name = COUNTRIES[country_iso]
    year = date.today().year
    found = 0

    for i, template in enumerate(DISCOVERY_QUERY_TEMPLATES, 1):
        query = template.format(country=country_name, year=year)
        print(f"    [{country_iso}] discovery query {i}/{len(DISCOVERY_QUERY_TEMPLATES)}: {query}")
        t0 = time.perf_counter()
        try:
            results = web_search(query, count=5)
            print(f"    [{country_iso}] → {len(results)} web results (post-filter)")
            facilities = _extract_facilities_claude(client, results, country_iso, country_name)
            print(f"    [{country_iso}] → Claude extracted {len(facilities)} facility candidates")
            skipped_no_evidence = 0
            skipped_no_keys = 0
            for fac in facilities:
                if not fac.get("facility_name") or not fac.get("operator"):
                    skipped_no_keys += 1
                    continue
                operator = fac.get("operator", "")
                is_hs = any(op in operator for op in HYPERSCALER_OPERATORS)
                evidence = (fac.get("ai_evidence") or "").strip()
                # AI-evidence gate applies ONLY to non-platform operators.
                # Hyperscaler/AI-platform facilities are AI compute by default
                # in the current buildout — the operator IS the evidence.
                if not evidence and not is_hs:
                    skipped_no_evidence += 1
                    print(f"    [{country_iso}]   ⊘ skipped (non-platform, no AI evidence): "
                          f"{fac['facility_name']} ({operator})")
                    continue
                if not evidence:
                    evidence = f"operator '{operator}' is a major AI/cloud platform"
                src_url = fac.pop("source_url", None)
                src_domain = _domain_of(src_url) if src_url else ""
                is_cautious = any(src_domain == d or src_domain.endswith("." + d)
                                  for d in CAUTIOUS_DOMAINS)
                conf = (CONFIDENCE["benchmark_est"] if is_cautious
                        else CONFIDENCE["agent_single"])
                tag = " [cautious-source]" if is_cautious else ""
                print(f"    [{country_iso}]   + {fac['facility_name']} ({fac['operator']}) "
                      f"| {fac.get('capacity_mw')} MW | {fac.get('status')}{tag}")
                print(f"    [{country_iso}]     ai_evidence: {evidence[:140]}")
                # Drop ai_evidence before upsert (not a column in cii_facilities).
                fac.pop("ai_evidence", None)
                fac["country_iso"]       = country_iso
                fac["confidence_score"]  = conf
                fac["source_urls"] = [src_url] if src_url else []
                fac["source_count"]      = 1
                fac["has_estimated_fields"] = False
                upsert_facility(conn, run_id, fac)
                found += 1
            if skipped_no_evidence or skipped_no_keys:
                print(f"    [{country_iso}]   filtered out: "
                      f"{skipped_no_evidence} no-AI-evidence, "
                      f"{skipped_no_keys} missing name/operator")
            elapsed = int((time.perf_counter() - t0) * 1000)
            print(f"    [{country_iso}] discovery query {i} done in {elapsed}ms — {found} total found so far")
            log_attempt(conn, run_id, country_iso, None, "discovery",
                        query, None, "success", CONFIDENCE["agent_single"], elapsed)
        except Exception as exc:
            elapsed = int((time.perf_counter() - t0) * 1000)
            print(f"    [{country_iso}] ✗ discovery query {i} FAILED ({elapsed}ms): {exc}")
            log_attempt(conn, run_id, country_iso, None, "discovery",
                        query, None, "failed", None, elapsed, str(exc)[:500])
        time.sleep(0.3)  # respect Tavily rate limits

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE cii_collection_runs
            SET facilities_discovered = facilities_discovered + %s,
                tavily_calls_used = tavily_calls_used + %s
            WHERE run_id = %s
        """, (found, len(DISCOVERY_QUERY_TEMPLATES), run_id))
    conn.commit()
    return found


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
    if not content_block.strip():
        return {}
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
        text = resp.content[0].text.strip()
        match = re.search(r"```[a-zA-Z]*\n?(.*?)```", text, re.DOTALL)
        text = match.group(1).strip() if match else text
        return json.loads(text)
    except Exception:
        return {}


def run_enrichment_pass(conn, run_id: str, country_iso: str) -> int:
    """Pass 2: deep-dive each facility to fill in MW, investment, dates.
    Returns number of facilities enriched."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    enriched_facilities: set = set()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, facility_name, operator, capacity_mw,
                   status, ownership_type
            FROM cii_facilities
            WHERE country_iso = %s
            ORDER BY facility_name
        """, (country_iso,))
        facilities = cur.fetchall()

    print(f"    [{country_iso}] enrichment: {len(facilities)} facilities × "
          f"{len(ENRICH_QUERY_TEMPLATES)} queries each "
          f"= {len(facilities) * len(ENRICH_QUERY_TEMPLATES)} planned searches")

    fail_count = 0
    gap_count = 0
    success_count = 0

    for fi, (c_iso, fname, operator, existing_mw,
             existing_status, existing_ownership) in enumerate(facilities, 1):
        print(f"    [{country_iso}] enriching {fi}/{len(facilities)}: "
              f"{fname} ({operator}) — existing MW: {existing_mw}, "
              f"status={existing_status}")
        per_facility_hits = 0
        for qi, template in enumerate(ENRICH_QUERY_TEMPLATES, 1):
            query = template.format(facility_name=fname, operator=operator)
            print(f"    [{country_iso}]   query {qi}/{len(ENRICH_QUERY_TEMPLATES)}: {query}")
            t0 = time.perf_counter()
            try:
                results = web_search(query, count=3)
                print(f"    [{country_iso}]   → {len(results)} web results")
                enriched_data = _enrich_facility_claude(
                    client, results, {"facility_name": fname, "operator": operator}
                )
                if enriched_data:
                    had_enrichment = True
                    src_url = enriched_data.pop("source_url", None)
                    conf_score = enriched_data.get("confidence_score")
                    filled = [k for k, v in enriched_data.items()
                              if v is not None and k != "confidence_score"]
                    print(f"    [{country_iso}]   ✓ Claude filled: {filled or '(no new fields)'} "
                          f"| conf={conf_score}")
                    update = {
                        "country_iso": c_iso, "facility_name": fname,
                        "operator": operator,
                        # Preserve discovery-assigned status/ownership_type;
                        # enrichment is field-level (MW, dates, USD), not
                        # status-level — must NOT collapse all facilities to
                        # 'operational' or wipe ownership.
                        "status": existing_status,
                        "ownership_type": existing_ownership or "unknown",
                        "is_hyperscaler": any(op in operator for op in HYPERSCALER_OPERATORS),
                        **enriched_data,
                        "source_urls": [src_url] if src_url else [],
                        "source_count": 1,
                    }
                    upsert_facility(conn, run_id, update)
                    enriched_facilities.add(fname)
                    per_facility_hits += 1
                    success_count += 1
                else:
                    had_enrichment = False
                    src_url = None
                    conf_score = None
                    print(f"    [{country_iso}]   ○ Claude returned no enrichment data (gap)")
                    gap_count += 1
                elapsed = int((time.perf_counter() - t0) * 1000)
                print(f"    [{country_iso}]   query {qi} done in {elapsed}ms")
                log_attempt(conn, run_id, c_iso, fname, "enrichment",
                            query, src_url,
                            "success" if had_enrichment else "gap",
                            conf_score, elapsed)
            except Exception as exc:
                elapsed = int((time.perf_counter() - t0) * 1000)
                fail_count += 1
                print(f"    [{country_iso}]   ✗ query {qi} FAILED ({elapsed}ms): {exc}")
                log_attempt(conn, run_id, c_iso, fname, "enrichment",
                            query, None, "failed", None, elapsed, str(exc)[:500])
            time.sleep(0.2)
        print(f"    [{country_iso}] facility {fi} done — {per_facility_hits} enrichment hit(s)")

    print(f"    [{country_iso}] enrichment pass complete: "
          f"{len(enriched_facilities)}/{len(facilities)} facilities enriched "
          f"| {success_count} success, {gap_count} gap, {fail_count} failed across queries")

    with conn.cursor() as cur:
        cur.execute("""
            UPDATE cii_collection_runs
            SET facilities_enriched = facilities_enriched + %s,
                tavily_calls_used   = tavily_calls_used + %s
            WHERE run_id = %s
        """, (len(enriched_facilities), len(facilities) * len(ENRICH_QUERY_TEMPLATES), run_id))
    conn.commit()
    return len(enriched_facilities)


def run_validation_pass(conn, run_id: str, country_iso: str) -> int:
    """Pass 3: assign final confidence based on source agreement;
    apply hyperscaler benchmark when MW still missing.
    Returns number of facilities updated."""
    updated = 0
    multi_source = 0
    single_source = 0
    benchmark_est = 0
    gaps_created = 0

    with conn.cursor() as cur:
        cur.execute("""
            SELECT country_iso, facility_name, operator, capacity_mw, source_count,
                   status, ownership_type
            FROM cii_facilities WHERE country_iso = %s
        """, (country_iso,))
        facilities = cur.fetchall()

    print(f"    [{country_iso}] validation: scoring {len(facilities)} facilities")

    for fi, (c_iso, fname, operator, cap_mw, src_count,
             existing_status, existing_ownership) in enumerate(facilities, 1):
        update = {"country_iso": c_iso, "facility_name": fname,
                  "operator": operator,
                  # Preserve discovery-assigned status — validation only
                  # adjusts confidence/MW-estimate, never flips status to
                  # 'operational'. Doing so would erase the entire
                  # committed-pipeline signal that SI1/SI2 depend on.
                  "status": existing_status,
                  "ownership_type": existing_ownership or "unknown",
                  "is_hyperscaler": any(op in operator for op in HYPERSCALER_OPERATORS),
                  "source_urls": [], "source_count": 0}

        if cap_mw is not None and src_count >= 2:
            update["confidence_score"]     = CONFIDENCE["multi_source"]
            update["has_estimated_fields"] = False
            tier = "multi_source"
            multi_source += 1
            print(f"    [{country_iso}]   {fi}/{len(facilities)} {fname} ({operator}) "
                  f"→ multi_source (MW={cap_mw}, sources={src_count}) "
                  f"conf={update['confidence_score']}")
        elif cap_mw is not None:
            update["confidence_score"]     = CONFIDENCE["agent_single"]
            update["has_estimated_fields"] = False
            tier = "agent_single"
            single_source += 1
            print(f"    [{country_iso}]   {fi}/{len(facilities)} {fname} ({operator}) "
                  f"→ agent_single (MW={cap_mw}, sources={src_count}) "
                  f"conf={update['confidence_score']}")
        else:
            # Apply benchmark estimate
            benchmark = next(
                (v for op, v in HYPERSCALER_BENCHMARK_MW.items() if op in operator),
                HYPERSCALER_BENCHMARK_MW["default"]
            )
            update["capacity_mw"]          = benchmark
            update["confidence_score"]     = CONFIDENCE["benchmark_est"]
            update["has_estimated_fields"] = True
            tier = "benchmark_est"
            benchmark_est += 1
            severity = "medium" if update["is_hyperscaler"] else "low"
            print(f"    [{country_iso}]   {fi}/{len(facilities)} {fname} ({operator}) "
                  f"→ benchmark_est (MW missing → assigned {benchmark} MW) "
                  f"conf={update['confidence_score']} | gap severity={severity}")
            upsert_gap(conn, c_iso, "capacity_mw", fname,
                       "field_gap", "capacity_mw not found after enrichment",
                       severity)
            gaps_created += 1

        upsert_facility(conn, run_id, update)
        log_attempt(conn, run_id, c_iso, fname, "validation",
                    None, None, "success", update["confidence_score"], 0)
        updated += 1

    conn.commit()
    print(f"    [{country_iso}] validation pass complete: {updated} facilities updated "
          f"| {multi_source} multi_source, {single_source} single_source, "
          f"{benchmark_est} benchmark_est | {gaps_created} new gap rows")
    return updated
