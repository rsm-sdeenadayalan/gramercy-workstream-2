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
