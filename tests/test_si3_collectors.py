# tests/test_si3_collectors.py
import pytest, sys, os
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cii'))


def test_frontier_training_known_zero_stored_with_high_confidence(mock_conn):
    from cii_si3_collectors import collect_frontier_training
    collect_frontier_training(mock_conn, "run-123", "PH")

    calls = mock_conn.cursor.return_value.__enter__.return_value.execute.call_args_list
    insert_calls = [str(c) for c in calls if "cii_raw_metrics" in str(c)]
    assert any("frontier_training_present" in c for c in insert_calls)
    # value should be 0.0, confidence high (0.90)
    for c in calls:
        args = c[0]
        if len(args) > 1 and isinstance(args[1], tuple):
            arg_str = str(args[1])
            if "frontier_training_present" in arg_str:
                assert "0.0" in arg_str
                assert "0.9" in arg_str


def test_domestic_ownership_writes_raw_metric(mock_conn):
    with patch("cii_si3_collectors.web_search") as mock_search, \
         patch("cii_si3_collectors._extract_ownership_claude") as mock_extract, \
         patch("anthropic.Anthropic"):

        mock_search.return_value = [
            {"url": "https://edb.gov.sg", "title": "SG DC", "content": "70% foreign-owned"}
        ]
        mock_extract.return_value = {"domestic_ratio": 0.30, "confidence": 0.70}

        from cii_si3_collectors import collect_domestic_ownership
        collect_domestic_ownership(mock_conn, "run-123", "SG")

        calls = mock_conn.cursor.return_value.__enter__.return_value.execute.call_args_list
        assert any("cii_raw_metrics" in str(c) for c in calls)
        # Also verify the ratio value and metric key appear in the SQL args
        insert_args_list = [c[0][1] for c in calls if "cii_raw_metrics" in str(c) and len(c[0]) > 1 and isinstance(c[0][1], tuple)]
        assert any("domestic_ownership_ratio" in str(args) for args in insert_args_list)
        assert any("0.3" in str(args) for args in insert_args_list)
