# tests/test_scoring.py
import pytest, sys, os
from unittest.mock import MagicMock, patch, call
from datetime import date
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'cii'))


def _make_cursor_with_data(rows_by_query: dict):
    """Helper: return a mock cursor whose fetchall/fetchone returns
    different rows depending on which query was last executed."""
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    last_query = [None]

    def execute(sql, params=None):
        last_query[0] = sql

    def fetchall():
        for key, rows in rows_by_query.items():
            if key in (last_query[0] or ""):
                return rows
        return []

    def fetchone():
        for key, rows in rows_by_query.items():
            if key in (last_query[0] or ""):
                return rows[0] if rows else None
        return None

    cursor.execute = execute
    cursor.fetchall = fetchall
    cursor.fetchone = fetchone
    return cursor


def test_compute_si2_snapshot_calculates_qoq():
    conn = MagicMock()
    cursor = _make_cursor_with_data({
        "SUM": [(500.0, 200.0, 1_000_000_000.0, 3)],  # installed, committed, usd, hyperscaler_count
        "prev": [(400.0, 180.0, 900_000_000.0)],        # prev quarter
        "grid_capacity_mw": [(50_000.0,)],
    })
    conn.cursor.return_value = cursor

    from cii_scoring import _compute_si2_snapshot
    snap = _compute_si2_snapshot(conn, "US", "2025Q2", date(2025, 6, 30))

    assert snap["installed_mw"] == 500.0
    assert abs(snap["qoq_installed_growth_rate"] - 0.25) < 0.001   # (500-400)/400
    assert abs(snap["grid_strain_ratio"] - (200.0 / 50_000.0)) < 0.0001


def test_compute_si2_snapshot_null_on_no_prev_quarter():
    conn = MagicMock()
    cursor = _make_cursor_with_data({
        "SUM": [(500.0, 200.0, 1_000_000_000.0, 3)],
        "prev": [],   # no prior quarter — first run
        "grid_capacity_mw": [(50_000.0,)],
    })
    conn.cursor.return_value = cursor

    from cii_scoring import _compute_si2_snapshot
    snap = _compute_si2_snapshot(conn, "US", "2025Q1", date(2025, 3, 31))
    assert snap["qoq_installed_growth_rate"] is None
    assert snap["qoq_committed_mw_growth_rate"] is None


def test_compute_si3_derived_writes_hyperscaler_count():
    mock_conn = MagicMock()
    # fetchone returns data for both queries (hyperscaler count/invest + chip_access_tier)
    mock_conn.cursor.return_value.__enter__.return_value.fetchone.return_value = (7, 5_000_000_000.0)
    with patch("cii_scoring.upsert_raw_metric") as mock_upsert:
        from cii_scoring import compute_si3_derived
        compute_si3_derived(mock_conn, "run-123", "US")
        calls = {c[0][4] for c in mock_upsert.call_args_list}  # metric_key is 5th positional arg
        assert "hyperscaler_count" in calls
        assert "hyperscaler_investment_usd" in calls
        assert "chip_access_tier" in calls
