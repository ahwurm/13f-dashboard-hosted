"""
AppTest coverage for the single-page dashboard (restored pre-rewrite UX).
Every view renders exception-free on the two retained quarters, the sidebar
ticker search jumps straight to the security detail view, single- and
multi-institution selections show their metrics, type filters apply, and the
quarter switch changes aggregates.
Run: pytest tests/test_app.py  (streamlit/pandas/plotly must be importable;
cwd must be the repo root so relative data paths resolve).
"""
from pathlib import Path
import pytest
from streamlit.testing.v1 import AppTest

APP = str(Path(__file__).resolve().parents[1] / "app.py")
TIMEOUT = 180


def _run():
    at = AppTest.from_file(APP)
    at.run(timeout=TIMEOUT)
    return at


def _no_exc(at):
    assert not at.exception, [e.value for e in at.exception]


def _metrics(at):
    return {m.label: m.value for m in at.metric}


@pytest.mark.parametrize("qidx", [0, 1])  # newest and second-newest quarter
def test_default_view_renders_both_quarters(qidx):
    at = _run()
    if qidx:
        at.sidebar.selectbox[0].set_value(qidx).run(timeout=TIMEOUT)
    _no_exc(at)
    m = _metrics(at)
    for label in ("Total Securities", "Total Value", "Capital Consensus",
                  "Institutions Filed", "Data Date"):
        assert label in m, f"missing metric {label!r} in {sorted(m)}"


def test_changes_tab_has_data():
    at = _run()
    _no_exc(at)
    assert any("Institutional Movements" in h.value for h in at.header)
    subs = [s.value for s in at.subheader]
    assert any("Top Portfolio Increases" in s for s in subs)
    assert any("Top Portfolio Decreases" in s for s in subs)
    def _cols(el):
        v = el.value
        return list(getattr(v, "data", v).columns)  # heat-shaded tables arrive as Styler
    df_cols = [_cols(d) for d in at.dataframe]
    assert any("# Adds" in cols for cols in df_cols)
    assert any("# Drops" in cols for cols in df_cols)


def test_ticker_search_shows_security_detail():
    at = _run()
    at.sidebar.text_input[0].set_value("MSFT").run(timeout=TIMEOUT)
    _no_exc(at)
    m = _metrics(at)
    for label in ("Inst. Own", "# Holders", "Net Adds", "Shares"):
        assert label in m, f"missing metric {label!r} in {sorted(m)}"
    assert any(s.value.startswith("MSFT") for s in at.subheader)


def test_ticker_search_no_match_is_graceful():
    at = _run()
    at.sidebar.text_input[0].set_value("ZZZZZZ").run(timeout=TIMEOUT)
    _no_exc(at)
    assert any("No data found" in i.value for i in at.info)


def test_single_institution_view():
    at = _run()
    at.sidebar.multiselect[1].select("Berkshire Hathaway").run(timeout=TIMEOUT)
    _no_exc(at)
    m = _metrics(at)
    assert any(label.endswith("Holdings") for label in m)
    assert "Top 5" in m


def test_two_institution_overlap():
    at = _run()
    opts = list(at.sidebar.multiselect[1].options[:2])
    assert len(opts) == 2
    at.sidebar.multiselect[1].set_value(opts).run(timeout=TIMEOUT)
    _no_exc(at)
    assert "Overlap" in _metrics(at)


def test_investor_type_filter_applies():
    at = _run()
    opts = at.sidebar.multiselect[0].options
    assert opts, "no investor types derived from cik_metadata"
    at.sidebar.multiselect[0].select(opts[0]).run(timeout=TIMEOUT)
    _no_exc(at)
    assert "Inst." in _metrics(at)


def test_quarter_switch_changes_data():
    at = _run()
    v1 = _metrics(at)["Total Value"]
    at.sidebar.selectbox[0].set_value(1).run(timeout=TIMEOUT)
    _no_exc(at)
    v2 = _metrics(at)["Total Value"]
    assert v1 != v2  # distinct quarters -> distinct aggregates


def test_holdings_tab_has_csv_download():
    at = _run()
    _no_exc(at)
    assert len(at.download_button) >= 1


def test_reset_filters_button():
    at = _run()
    at.sidebar.text_input[0].set_value("MSFT").run(timeout=TIMEOUT)
    assert any(s.value.startswith("MSFT") for s in at.subheader)
    at.sidebar.button[0].click().run(timeout=TIMEOUT)
    _no_exc(at)
    assert "Total Securities" in _metrics(at)  # back to the unfiltered default view
