# tests/test_gap_report.py
import pytest, sys, os
from unittest.mock import MagicMock
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cii'))


def test_gap_report_orders_by_severity(mock_conn):
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.side_effect = [
        # CRITICAL gaps
        [("AE", "installed_capacity_mw", None, "No facilities found", 2,
          "Check DEWA annual report", True)],
        # HIGH gaps
        [("BR", "capacity_mw", "Equinix SP4", "MW missing", 1, None, False)],
        # MEDIUM gaps
        [],
        # STRUCTURAL
        [("PH", "frontier_training_present", None, "known_zero", 1, None, False)],
    ]

    from cii_gap_report import build_gap_report
    report = build_gap_report(mock_conn)
    assert report["critical"][0]["country_iso"] == "AE"
    assert report["high"][0]["country_iso"] == "BR"
    assert len(report["structural"]) == 1


def test_gap_report_prints_without_error(mock_conn, capsys):
    mock_conn.cursor.return_value.__enter__.return_value.fetchall.side_effect = [
        [], [], [], []
    ]
    from cii_gap_report import build_gap_report, print_gap_report
    report = build_gap_report(mock_conn)
    print_gap_report(report)
    captured = capsys.readouterr()
    assert "CII Gap Report" in captured.out
