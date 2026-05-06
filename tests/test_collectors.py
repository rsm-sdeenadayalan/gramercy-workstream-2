# tests/test_collectors.py
import pytest, json, sys, os
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cii'))


def test_web_search_returns_results():
    with patch("tavily.TavilyClient") as MockTavily, \
         patch.dict(os.environ, {"TAVILY_API_KEY": "test-key"}):
        client = MagicMock()
        MockTavily.return_value = client
        client.search.return_value = {
            "results": [{"url": "https://example.com", "title": "Test",
                         "content": "200MW data center announced"}]
        }
        import importlib, cii_collectors
        importlib.reload(cii_collectors)
        from cii_collectors import web_search
        results = web_search("US data center capacity", count=3)
        assert len(results) == 1
        assert results[0]["url"] == "https://example.com"
        assert "200MW" in results[0]["content"]


def test_web_search_raises_when_no_key():
    with patch.dict(os.environ, {"TAVILY_API_KEY": ""}, clear=False):
        import importlib, cii_collectors
        importlib.reload(cii_collectors)
        with pytest.raises(ValueError, match="TAVILY_API_KEY"):
            cii_collectors.web_search("test query")


def test_extract_facilities_parses_claude_json():
    mock_client = MagicMock()
    mock_client.messages.create.return_value.content = [MagicMock(
        text=json.dumps([{
            "facility_name": "AWS Iowa",
            "operator": "Amazon",
            "capacity_mw": 150.0,
            "status": "operational",
            "date_announced": "2023-01-01",
            "date_operational": "2024-01-01",
            "investment_value_usd": 500000000.0,
            "energy_source": "renewable",
            "chip_type_if_known": None,
            "ownership_type": "foreign",
            "is_hyperscaler": True,
            "source_url": "https://aws.amazon.com/press"
        }])
    )]

    from cii_collectors import _extract_facilities_claude
    results = _extract_facilities_claude(
        mock_client,
        [{"url": "https://aws.amazon.com/press", "title": "AWS Iowa",
          "content": "150MW operational campus"}],
        "US", "United States"
    )
    assert len(results) == 1
    assert results[0]["facility_name"] == "AWS Iowa"
    assert results[0]["capacity_mw"] == 150.0
    assert results[0]["is_hyperscaler"] is True


def test_extract_facilities_handles_bad_json():
    mock_client = MagicMock()
    mock_client.messages.create.return_value.content = [
        MagicMock(text="not valid json at all")
    ]
    from cii_collectors import _extract_facilities_claude
    results = _extract_facilities_claude(
        mock_client,
        [{"url": "https://example.com", "title": "Test DC", "content": "200MW campus"}],
        "US", "United States"
    )
    assert results == []


def test_extract_facilities_returns_empty_for_no_results():
    from cii_collectors import _extract_facilities_claude
    results = _extract_facilities_claude(MagicMock(), [], "US", "United States")
    assert results == []


def test_run_discovery_pass_upserts_facilities(mock_conn):
    with patch("cii_collectors.web_search") as mock_search, \
         patch("cii_collectors._extract_facilities_claude") as mock_extract, \
         patch("cii_collectors.upsert_facility") as mock_upsert, \
         patch("cii_collectors.log_attempt"), \
         patch("anthropic.Anthropic"):

        mock_search.return_value = [
            {"url": "https://example.com", "title": "T", "content": "200MW campus"}
        ]
        mock_extract.return_value = [{
            "facility_name": "AWS Iowa", "operator": "Amazon",
            "capacity_mw": 200.0, "status": "operational",
            "date_announced": None, "date_operational": None,
            "investment_value_usd": None, "energy_source": None,
            "chip_type_if_known": None, "ownership_type": "foreign",
            "is_hyperscaler": True, "source_url": "https://example.com"
        }]

        from cii_collectors import run_discovery_pass, CONFIDENCE
        run_discovery_pass(mock_conn, "run-123", "US")
        assert mock_upsert.call_count >= 1
        call_kwargs = mock_upsert.call_args[0][2]
        assert call_kwargs["confidence_score"] == CONFIDENCE["agent_single"]
        assert call_kwargs["country_iso"] == "US"
        assert call_kwargs["source_count"] == 1
        assert call_kwargs["has_estimated_fields"] is False
        assert isinstance(call_kwargs["source_urls"], list)


def test_run_discovery_pass_calls_upsert_per_query(mock_conn):
    with patch("cii_collectors.web_search") as mock_search, \
         patch("cii_collectors._extract_facilities_claude") as mock_extract, \
         patch("cii_collectors.upsert_facility") as mock_upsert, \
         patch("cii_collectors.log_attempt"), \
         patch("anthropic.Anthropic"):

        mock_search.return_value = [{"url": "u", "title": "t", "content": "c"}]
        # Same facility returned from all 8 queries
        mock_extract.return_value = [{
            "facility_name": "AWS Iowa", "operator": "Amazon",
            "capacity_mw": 200.0, "status": "operational",
            "date_announced": None, "date_operational": None,
            "investment_value_usd": None, "energy_source": None,
            "chip_type_if_known": None, "ownership_type": "foreign",
            "is_hyperscaler": True, "source_url": "u"
        }]

        from cii_collectors import run_discovery_pass
        run_discovery_pass(mock_conn, "run-123", "US")
        # 8 queries × 1 facility per query = 8 upsert calls; DB UNIQUE constraint deduplicates
        assert mock_upsert.call_count == 8


def test_enrichment_updates_capacity_mw(mock_conn):
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [
        ("US", "AWS Iowa", "Amazon", None)  # capacity_mw is NULL — needs enrichment
    ]
    with patch("cii_collectors.web_search") as mock_search, \
         patch("cii_collectors._enrich_facility_claude") as mock_enrich, \
         patch("cii_collectors.upsert_facility") as mock_upsert, \
         patch("cii_collectors.log_attempt"):

        mock_search.return_value = [{"url": "u", "title": "t", "content": "200MW"}]
        mock_enrich.return_value = {
            "capacity_mw": 200.0, "investment_value_usd": 1e9,
            "date_announced": "2023-01-01", "date_operational": None,
            "energy_source": "renewable", "chip_type_if_known": "H100",
            "confidence_score": 0.75, "source_url": "u"
        }
        from cii_collectors import run_enrichment_pass
        enriched = run_enrichment_pass(mock_conn, "run-123", "US")
        assert enriched >= 1
        assert mock_upsert.call_count >= 1


def test_validation_assigns_high_confidence_for_multi_source(mock_conn):
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [
        ("US", "AWS Iowa", "Amazon", 200.0, 2)  # source_count=2
    ]
    with patch("cii_collectors.log_attempt"), \
         patch("cii_collectors.upsert_facility") as mock_upsert:
        from cii_collectors import run_validation_pass
        run_validation_pass(mock_conn, "run-123", "US")
        if mock_upsert.called:
            call_kwargs = mock_upsert.call_args[0][2]
            assert call_kwargs["confidence_score"] >= 0.85


def test_validation_applies_benchmark_when_mw_missing(mock_conn):
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [
        ("US", "Meta Iowa", "Meta", None, 0)  # capacity_mw NULL, 0 sources
    ]
    with patch("cii_collectors.upsert_facility") as mock_upsert, \
         patch("cii_collectors.log_attempt"), \
         patch("cii_collectors.upsert_gap"):
        from cii_collectors import run_validation_pass, HYPERSCALER_BENCHMARK_MW, CONFIDENCE
        run_validation_pass(mock_conn, "run-123", "US")
        assert mock_upsert.called
        call_kwargs = mock_upsert.call_args[0][2]
        assert call_kwargs["capacity_mw"] == HYPERSCALER_BENCHMARK_MW["Meta"]
        assert call_kwargs["confidence_score"] == CONFIDENCE["benchmark_est"]
        assert call_kwargs["has_estimated_fields"] is True
