"""
AppTest coverage for the Conviction Terminal (R-A7).
Every page renders exception-free on Q1 2026 AND Q4 2025, quarter switch works,
deep links restore state, a manager profile renders, and screener filters apply.
Run: pytest tests/test_app.py  (streamlit/pandas/plotly must be importable).
"""
from pathlib import Path
import pytest
from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parents[1] / "app.py")
TIMEOUT = 90


def _run(**qp):
    at = AppTest.from_file(APP)
    for k, v in qp.items():
        at.query_params[k] = v
    at.run(timeout=TIMEOUT)
    return at


def _no_exc(at):
    assert not at.exception, [e.value for e in at.exception]


@pytest.mark.parametrize("q", ["Q1_2026", "Q4_2025"])
@pytest.mark.parametrize("page", ["signals", "managers", "screener"])
def test_pages_render_both_quarters(page, q):
    at = _run(page=page, q=q)
    _no_exc(at)
    assert at.title  # every page sets a title


@pytest.mark.parametrize("q", ["Q1_2026", "Q4_2025"])
def test_security_page_renders(q):
    at = _run(page="security", q=q)  # no ticker -> empty-state prompt, no crash
    _no_exc(at)


def test_signals_metrics_present():
    at = _run(q="Q1_2026")
    _no_exc(at)
    labels = [m.label for m in at.metric]
    assert "Filers" in labels
    assert "Long book tracked" in labels
    assert "Consensus buys ≥3" in labels


def test_ticker_deep_link_msft():
    at = _run(page="security", t="MSFT", q="Q1_2026")
    _no_exc(at)
    labels = {m.label: m.value for m in at.metric}
    assert "Conviction holders" in labels
    assert labels["% shares out"].endswith("%")


def test_manager_profile_renders():
    at = _run(page="managers", m="Berkshire Hathaway", q="Q1_2026")
    _no_exc(at)
    labels = [m.label for m in at.metric]
    assert "Top-5 weight" in labels
    assert "Long book" in labels


def test_manager_profile_no_duplicate_notional():
    # with metadata.institutions absent, the notional slot must be the honest
    # options hint — never two labels showing the same derived number
    at = _run(page="managers", m="Berkshire Hathaway", q="Q1_2026")
    _no_exc(at)
    metrics = {m.label: m.value for m in at.metric}
    assert "13F long notional" not in metrics
    assert metrics.get("Options exposure") == "n/a"


def test_manager_deep_link_without_data_shows_notice():
    # roster manager with no filing in the committed Q1 dataset
    at = _run(page="managers", m="Greenlight Capital", q="Q1_2026")
    _no_exc(at)
    warnings = [w.value for w in at.warning]
    assert any("Greenlight Capital" in w and "no Q1 2026 filing" in w for w in warnings)
    assert any(t.value == "Managers" for t in at.title)  # list still renders after the notice


def test_quarter_switch_changes_data():
    a1 = _run(q="Q1_2026")
    a2 = _run(q="Q4_2025")
    _no_exc(a1)
    _no_exc(a2)
    v1 = {m.label: m.value for m in a1.metric}["Long book tracked"]
    v2 = {m.label: m.value for m in a2.metric}["Long book tracked"]
    assert v1 != v2  # distinct quarters -> distinct aggregates


def test_screener_type_filter_applies():
    at = _run(page="screener", q="Q1_2026")
    _no_exc(at)
    at.multiselect[0].select("Activist Hedge Fund").run(timeout=TIMEOUT)
    _no_exc(at)
    assert any("match" in c.value for c in at.caption)


def test_screener_scatter_view():
    at = _run(page="screener", q="Q1_2026")
    at.radio[0].set_value("Scatter").run(timeout=TIMEOUT)
    _no_exc(at)


def test_screener_has_csv_download():
    at = _run(page="screener", q="Q1_2026")
    _no_exc(at)
    assert len(at.download_button) == 1


def test_manager_compare_overlap():
    at = _run(page="managers", q="Q1_2026")
    _no_exc(at)
    opts = at.multiselect[0].options[:2]
    at.multiselect[0].set_value(opts).run(timeout=TIMEOUT)
    _no_exc(at)
