import pytest
from unittest.mock import MagicMock, patch
import psycopg2


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor
    return conn


@pytest.fixture
def mock_claude():
    with patch("anthropic.Anthropic") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def mock_tavily():
    with patch("tavily.TavilyClient") as mock:
        client = MagicMock()
        mock.return_value = client
        yield client


@pytest.fixture
def sample_facility():
    return {
        "country_iso": "US",
        "facility_name": "Microsoft Iowa Campus",
        "operator": "Microsoft",
        "capacity_mw": 200.0,
        "status": "operational",
        "date_announced": "2023-01-15",
        "date_operational": "2024-06-01",
        "investment_value_usd": 1_000_000_000.0,
        "energy_source": "renewable",
        "chip_type_if_known": "NVIDIA H100",
        "ownership_type": "foreign",
        "is_hyperscaler": True,
        "confidence_score": 0.85,
        "source_urls": ["https://news.microsoft.com/iowa"],
        "source_count": 2,
    }
