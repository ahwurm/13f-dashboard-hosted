"""
Conviction Terminal — 13F institutional holdings, organized by question not data-shape.
Four pages (Signals / Managers / Securities / Screener) on an st.navigation shell.
All monetary JSON is in millicents; SCALE_FACTOR converts to $M. Conviction math is
roster-only: only positions held by managers in config/clean_institutions.csv count.
Gracefully degrades on quarters whose JSON lacks the richer fields.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import json, csv, html
from pathlib import Path
from datetime import date, timedelta
from statistics import median

SCALE_FACTOR = 1_000_000_000  # millicents -> $M
ACCENT, AMBER, RED, DIM, INK, EDGE, SURFACE, FAINT = (
    "#56dc85", "#f0bb3b", "#e5484d", "#9b9ea6", "#e9ebef", "#272b34", "#14171e", "#6b6f78")
Q_END = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}

# ---- query params read (before set_page_config so the tab title can be dynamic) ----
_qp = st.query_params
_page, _t, _m, _q = (_qp.get(k, "") for k in ("page", "t", "m", "q"))

def _tab_title():
    qs = f" · {_q.replace('_', ' ')}" if _q else ""
    if _page in ("security", "securities") and _t:
        return f"{_t} — Conviction{qs}"
    if _page == "managers" and _m:
        return f"{_m} — Conviction"
    label = {"managers": "Managers", "security": "Securities", "securities": "Securities",
             "screener": "Screener"}.get(_page, "Signals")
    return f"{label} — Conviction{qs}"

st.set_page_config(page_title=_tab_title(), page_icon="static/favicon.svg",
                   layout="wide", initial_sidebar_state="auto")

# ---- plotly template (ported from the shipped app) ----
pio.templates['lh'] = go.layout.Template(layout=go.Layout(
    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family='Geist Variable, ui-sans-serif, sans-serif', color=INK, size=12),
    title=dict(font=dict(size=13, color=DIM)),
    colorway=[ACCENT, AMBER, DIM, INK],
    xaxis=dict(gridcolor=EDGE, zerolinecolor=EDGE, linecolor=EDGE,
               tickfont=dict(family='Geist Mono Variable, ui-monospace, monospace', size=11)),
    yaxis=dict(gridcolor=EDGE, zerolinecolor=EDGE, linecolor=EDGE,
               tickfont=dict(family='Geist Mono Variable, ui-monospace, monospace', size=11)),
    hoverlabel=dict(bgcolor=SURFACE, bordercolor=EDGE,
                    font=dict(family='Geist Mono Variable, ui-monospace, monospace', size=12, color=INK)),
))
pio.templates.default = 'lh'

st.markdown("""
<style>
.stApp::before{content:"";position:fixed;inset:0;z-index:0;pointer-events:none;
 background-image:linear-gradient(to right,rgba(86,220,133,.025) 1px,transparent 1px),
 linear-gradient(to bottom,rgba(86,220,133,.025) 1px,transparent 1px),
 linear-gradient(to right,rgba(233,235,239,.015) 1px,transparent 1px),
 linear-gradient(to bottom,rgba(233,235,239,.015) 1px,transparent 1px);
 background-size:96px 96px,96px 96px,24px 24px,24px 24px;}
div[data-testid="stMetric"]{background:#14171e;border:1px solid #272b34;border-radius:8px;padding:.55rem .9rem;}
div[data-testid="stMetric"] label{font-size:.72rem;letter-spacing:.04em;text-transform:uppercase;color:#9b9ea6;}
div[data-testid="stMetricValue"]{font-size:1.15rem;font-variant-numeric:tabular-nums;}
h1{font-size:1.45rem!important;margin:.4rem 0!important;letter-spacing:-.01em;}
h2{font-size:1.15rem!important;margin:.4rem 0!important;}
h3{font-size:1rem!important;margin:.4rem 0!important;}
.block-container{padding-top:3rem;padding-bottom:1rem;}
.vintage{font-family:'Geist Mono Variable',ui-monospace,monospace;font-size:11px;letter-spacing:.03em;
 color:#9b9ea6;text-transform:uppercase;border:1px solid #272b34;background:#14171e;border-radius:6px;
 padding:6px 10px;margin:2px 0 12px;}
.vintage b{color:#e9ebef;}
.ctab{width:100%;border-collapse:collapse;font-size:13px;}
.ctab th{text-align:left;color:#9b9ea6;font-weight:600;font-size:11px;text-transform:uppercase;
 letter-spacing:.03em;border-bottom:1px solid #272b34;padding:5px 8px;}
.ctab td{padding:5px 8px;border-bottom:1px solid #191d24;}
.ctab td.r{text-align:right;font-family:'Geist Mono Variable',ui-monospace,monospace;}
.ctab a{color:#e9ebef;text-decoration:none;font-weight:600;}
.ctab a:hover{color:#56dc85;}
.ctab .nm{color:#9b9ea6;}
.pill{font-family:'Geist Mono Variable',ui-monospace,monospace;font-size:11px;padding:1px 6px;border-radius:4px;}
.buy{background:rgba(86,220,133,.15);color:#56dc85;}
.exit{background:rgba(229,72,77,.15);color:#e5484d;}
.new{background:rgba(86,220,133,.15);color:#56dc85;}
.trim{background:rgba(240,187,59,.15);color:#f0bb3b;}
</style>
""", unsafe_allow_html=True)

# ---------------- data layer ----------------
@st.cache_data
def load_config():
    return json.load(open('config/analysis_config.json'))

@st.cache_data
def load_roster():
    return {r['Institution']: r for r in csv.DictReader(open('config/clean_institutions.csv'))}

@st.cache_data
def get_quarters():
    out = Path('output')
    if not out.exists():
        return []
    qs = []
    for f in out.iterdir():
        if f.is_dir() and f.name.startswith('Q') and '_' in f.name \
                and (f / 'quarterly_adds_data.json').exists():
            q, y = f.name.split('_')
            qs.append((f.name, int(y), int(q[1])))
    qs.sort(key=lambda x: (x[1], x[2]), reverse=True)
    return [x[0] for x in qs]

def prior_quarter(qdir):
    qs = get_quarters()
    if qdir in qs:
        i = qs.index(qdir)
        if i + 1 < len(qs):
            return qs[i + 1]
    return None

@st.cache_data
def load_conviction(qdir):
    """One conviction row per roster-held security (roster-only recomputed math)."""
    d = json.load(open(f'output/{qdir}/quarterly_adds_data.json'))
    roster = set(load_roster())
    cap = load_config().get('analysis', {}).get('ownership_cap_percent', 100)
    rows = []
    for s in d['securities']:
        pos_all = s.get('positions') or {}
        rpos = {k: v for k, v in pos_all.items() if k in roster}
        if not rpos:
            continue
        so = s.get('shares_outstanding') or 0
        r_shares = sum(p['shares'] for p in rpos.values())
        r_value = sum(p['value'] for p in rpos.values()) / SCALE_FACTOR
        r_pct = r_shares / so * 100 if so else 0.0
        if r_pct > cap:
            continue
        ic = s.get('institution_changes') or {}
        nh = [h for h in (s.get('new_holders') or []) if h in roster]
        dh = [h for h in (s.get('dropped_holders') or []) if h in roster]
        buyers = sorted(set(nh) | {i for i, c in ic.items() if i in roster and c.get('shares_change', 0) > 0})
        sellers = sorted(set(dh) | {i for i, c in ic.items() if i in roster and c.get('shares_change', 0) < 0})
        rows.append(dict(
            cusip=s['cusip'], ticker=s['ticker'] or s['cusip'], name=s['name'] or s['ticker'] or s['cusip'],
            shares_outstanding=so, positions=rpos,
            r_holders=list(rpos.keys()), r_num=len(rpos), r_value=r_value, r_shares=r_shares, r_pct=r_pct,
            new_holders=nh, dropped_holders=dh, institution_changes=ic,
            buyers=buyers, sellers=sellers, n_buyers=len(buyers), n_sellers=len(sellers),
            n_exits=len(dh), net_dir=len(buyers) - len(sellers)))
    df = pd.DataFrame(rows)
    return df, d.get('metadata', {})

@st.cache_data
def load_manager_books(qdir):
    """Per-roster-manager book derived from positions (metadata.institutions absent today)."""
    df, _ = load_conviction(qdir)
    roster = load_roster()
    books = {}
    for _, r in df.iterrows():
        for inst, p in r['positions'].items():
            b = books.setdefault(inst, {'positions': [], 'long_value': 0.0, 'exits': []})
            ch = r['institution_changes'].get(inst, {})
            b['long_value'] += p['value'] / SCALE_FACTOR
            b['positions'].append(dict(
                ticker=r['ticker'], name=r['name'], cusip=r['cusip'],
                value=p['value'] / SCALE_FACTOR, shares=p['shares'],
                pct_co=p.get('pct_of_company_shares', 0) or 0,
                shares_change=ch.get('shares_change', 0), prev_shares=ch.get('prev_shares', 0),
                is_new=inst in r['new_holders']))
        for inst in r['dropped_holders']:
            if inst in books:
                books[inst]['exits'].append(r['ticker'])
    for inst, b in books.items():
        vals = sorted((p['value'] for p in b['positions']), reverse=True)
        b['n_pos'] = len(vals)
        b['top5_weight'] = (sum(vals[:5]) / b['long_value'] * 100) if b['long_value'] else 0.0
        b['exits'] = sorted(set(b['exits']))
    return books

# ---------------- helpers ----------------
def fmt_money_m(m):
    """$M value -> B/T/M string."""
    if m >= 1_000_000:
        return f"${m/1_000_000:.2f}T"
    if m >= 1_000:
        return f"${m/1_000:.1f}B"
    return f"${m:.1f}M"

def quarter_dates(qdir):
    q, y = qdir.split('_')
    mo, da = Q_END[int(q[1])]
    end = date(int(y), mo, da)
    return end, end + timedelta(days=45)

def vintage_eyebrow(qdir, meta, prefix=""):
    end, deadline = quarter_dates(qdir)
    pull = (meta.get('generated') or '')[:10] or '—'
    st.markdown(
        f'<div class="vintage">{prefix}POSITIONS AS OF <b>{end}</b> · FILED BY <b>{deadline}</b> · '
        f'PULLED <b>{pull}</b> · 13F = 45-DAY LAG, LONG US EQUITY ONLY</div>',
        unsafe_allow_html=True)

def takeaway(text, source):
    st.caption(f"**Takeaway:** {text}  \n*Source: {source}*")

def link(url_params, label):
    q = "&".join(f"{k}={html.escape(str(v))}" for k, v in url_params.items())
    return f'<a href="?{q}" target="_self">{html.escape(label)}</a>'

def sec_link(ticker, extra=None):
    p = {"page": "security", "t": ticker, "q": CURRENT_Q}
    return link(p, ticker)

def mgr_link(name):
    return link({"page": "managers", "m": name, "q": CURRENT_Q}, name)

def sync_qp(**kw):
    for k, v in kw.items():
        if _qp.get(k, "") != str(v):
            _qp[k] = str(v)

def drop_qp(*keys):
    for k in keys:
        if k in _qp:
            del _qp[k]

def copy_link(url_params):
    q = "&".join(f"{k}={v}" for k, v in url_params.items())
    st.code(f"?{q}", language=None)

# ---------------- sidebar: brand + global quarter picker ----------------
QUARTERS = get_quarters()
if not QUARTERS:
    st.error("No data found in output/. Run the pipeline first.")
    st.stop()

with st.sidebar:
    st.markdown(
        '<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px;">'
        '<svg width="22" height="22" viewBox="0 0 32 32"><rect width="32" height="32" rx="7" fill="#0d0f15"/>'
        '<rect x="6" y="18" width="4.5" height="8" rx="1" fill="#6b6f78"/>'
        '<rect x="13.75" y="12" width="4.5" height="14" rx="1" fill="#9b9ea6"/>'
        '<rect x="21.5" y="6" width="4.5" height="20" rx="1" fill="#56dc85"/></svg>'
        '<b style="letter-spacing:.08em;">CONVICTION</b></div>', unsafe_allow_html=True)
    _default_q = _q if _q in QUARTERS else QUARTERS[0]
    CURRENT_Q = st.selectbox("Quarter", QUARTERS, index=QUARTERS.index(_default_q),
                             format_func=lambda s: s.replace('_', ' '))
    sync_qp(q=CURRENT_Q)
    st.caption(f"{len(load_roster())}-name conviction roster")

# ============================================================ PAGES ============
def page_signals():
    sync_qp(page="signals")
    drop_qp("t", "m")
    df, meta = load_conviction(CURRENT_Q)
    roster = load_roster()
    st.title("Signals")
    vintage_eyebrow(CURRENT_Q, meta)
    if df.empty:
        st.info("No roster holdings in this quarter.")
        return

    fp = meta.get('institution_breakdown', {}).get('filing_periods', {})
    q, y = CURRENT_Q.split('_'); qn, yn = int(q[1]), int(y)
    filers = len([n for n, p in fp.items() if n in roster and p.get('quarter') == qn and p.get('year') == yn])
    if not filers:  # older/thin schema: fall back to managers present in the book
        filers = len(load_manager_books(CURRENT_Q))
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Filers", f"{filers} / {len(roster)}")
    c2.metric("Long book tracked", fmt_money_m(df['r_value'].sum()))
    c3.metric("Securities", f"{len(df):,}")
    c4.metric("Consensus buys ≥3", f"{int((df['n_buyers'] >= 3).sum()):,}")

    # ---- treemap hero: size = roster $ held, color = net managers buying - selling ----
    st.subheader("Where conviction money sits")
    top = df.nlargest(40, 'r_value').copy()
    top['label'] = top['ticker'] + "<br>" + top['r_value'].apply(fmt_money_m) + " · " + \
        top['net_dir'].apply(lambda n: (f"+{n} ▲" if n > 0 else (f"{n} ▼" if n < 0 else "0 ·")))
    lim = max(1, int(top['net_dir'].abs().max()))
    fig = go.Figure(go.Treemap(
        labels=top['label'], parents=[""] * len(top), values=top['r_value'],
        marker=dict(colors=top['net_dir'], colorscale=[[0, RED], [0.5, "#454b57"], [1, ACCENT]],
                    cmid=0, cmin=-lim, cmax=lim, line=dict(width=1, color="#0d0f15")),
        textinfo="label", hovertemplate="%{label}<extra></extra>",
        tiling=dict(pad=2)))
    fig.update_layout(height=380, margin=dict(t=6, b=6, l=0, r=0))
    st.plotly_chart(fig, width="stretch", config={'displayModeBar': False})
    worst = df.nlargest(1, 'n_sellers').iloc[0] if df['n_sellers'].max() else None
    takeaway(
        (f"Net direction across the roster's biggest positions — {worst['ticker']} saw the widest exit "
         f"({worst['n_sellers']} reducing/closing vs {worst['n_buyers']} adding)." if worst is not None
         else "Size is roster dollars held; color is net managers buying minus selling."),
        f"{CURRENT_Q.replace('_',' ')} 13F-HR filings, conviction roster only.")

    # ---- three panels: consensus buys / crowded exits / new-stakes tape ----
    a, b, c = st.columns(3)
    with a:
        st.markdown("**CONSENSUS BUYS** · managers adding")
        cb = df[df['n_buyers'] > 0].nlargest(8, 'n_buyers')
        rows = "".join(
            f'<tr><td>{sec_link(r.ticker)}</td><td class="nm">{html.escape(r.name[:16])}</td>'
            f'<td class="nm" style="font-size:11px">{html.escape(", ".join(r.buyers[:3]))}'
            f'{" +"+str(len(r.buyers)-3) if len(r.buyers)>3 else ""}</td>'
            f'<td class="r"><span class="pill buy">{r.n_buyers}</span></td></tr>'
            for r in cb.itertuples())
        st.markdown(f'<table class="ctab">{rows}</table>', unsafe_allow_html=True)
        takeaway("Count = roster managers initiating or increasing.", f"{CURRENT_Q.replace('_',' ')} new_holders + institution_changes.")
    with b:
        st.markdown("**CROWDED EXITS** · full closes")
        ce = df[df['n_exits'] > 0].nlargest(8, 'n_exits')
        if ce.empty:
            st.caption("No full exits recorded this quarter.")
        else:
            rows = "".join(
                f'<tr><td>{sec_link(r.ticker)}</td><td class="nm">{html.escape(r.name[:18])}</td>'
                f'<td class="r"><span class="pill exit">{r.n_exits}</span></td></tr>'
                for r in ce.itertuples())
            st.markdown(f'<table class="ctab">{rows}</table>', unsafe_allow_html=True)
        takeaway("First surfacing of dropped_holders — full position closes.", f"{CURRENT_Q.replace('_',' ')} dropped_holders.")
    with c:
        st.markdown("**BIGGEST NEW STAKES**")
        tape = []
        for r in df.itertuples():
            for inst in r.new_holders:
                tape.append((r.ticker, inst, r.positions[inst]['value'] / SCALE_FACTOR))
        tape.sort(key=lambda x: x[2], reverse=True)
        rows = "".join(
            f'<tr><td>{sec_link(tk)}</td><td class="nm">{html.escape(nm[:16])}</td>'
            f'<td class="r">{fmt_money_m(v)}</td></tr>'
            for tk, nm, v in tape[:8])
        st.markdown(f'<table class="ctab">{rows}</table>', unsafe_allow_html=True)
        takeaway("new_holders × position value — the quarter's headline entries.", f"{CURRENT_Q.replace('_',' ')} new_holders.")


def page_managers():
    sync_qp(page="managers")
    drop_qp("t")
    books = load_manager_books(CURRENT_Q)
    roster = load_roster()
    _, meta = load_conviction(CURRENT_Q)
    m = _qp.get("m", "")
    if m and m in books:
        _manager_profile(m, books, roster, meta)
    else:
        _manager_roster(books, roster, meta)


def _manager_roster(books, roster, meta):
    st.title("Managers")
    vintage_eyebrow(CURRENT_Q, meta)
    present = sorted(books, key=lambda n: books[n]['long_value'], reverse=True)
    st.caption(f"{len(present)} roster managers filed with holdings this quarter. Select to compare or open a profile.")
    # multi-manager overlap (preserves the old Portfolio Analysis capability)
    sel = st.multiselect("Compare managers (portfolio overlap)", present)
    if len(sel) >= 2:
        _manager_compare(sel, books)
        st.markdown("---")
    grid = pd.DataFrame([{
        "Manager": n, "Key person": roster[n].get('Key_Person', ''), "Strategy": roster[n].get('Type', ''),
        "Long book ($M)": books[n]['long_value'], "Positions": books[n]['n_pos'],
        "Top-5 weight": books[n]['top5_weight']} for n in present])
    st.dataframe(grid, width="stretch", hide_index=True, height=430, column_config={
        "Long book ($M)": st.column_config.NumberColumn(format="$%.0f M"),
        "Top-5 weight": st.column_config.ProgressColumn(format="%.0f%%", min_value=0, max_value=100)})
    st.caption("Open a profile via the URL: `?page=managers&m=<name>` — or pick two above to see shared conviction.")
    pick = st.selectbox("Open profile", ["—"] + present, index=0)
    if pick != "—":
        sync_qp(m=pick)
        st.rerun()


def _manager_compare(sel, books):
    st.markdown("**PORTFOLIO OVERLAP** — shared conviction positions")
    sets = {n: {p['ticker']: p['value'] for p in books[n]['positions']} for n in sel}
    common = set.intersection(*[set(s) for s in sets.values()])
    if not common:
        st.info("No tickers held by all selected managers.")
        return
    rows = [{"Ticker": tk, **{n: sets[n].get(tk, 0) for n in sel}} for tk in common]
    ov = pd.DataFrame(rows).sort_values(sel[0], ascending=False)
    st.dataframe(ov, width="stretch", hide_index=True,
                 column_config={n: st.column_config.NumberColumn(format="$%.0f M") for n in sel})
    st.caption(f"{len(common)} shared names across {len(sel)} managers.")


def _manager_profile(name, books, roster, meta):
    b = books[name]
    info = roster.get(name, {})
    st.title(name)
    vintage_eyebrow(CURRENT_Q, meta,
                    prefix=f"{html.escape(info.get('Key_Person',''))} · {html.escape(info.get('Type',''))} · "
                           f"CIK {info.get('CIK','—')} · ")
    copy_link({"page": "managers", "m": name, "q": CURRENT_Q})
    pos = sorted(b['positions'], key=lambda p: p['value'], reverse=True)
    n_new = sum(1 for p in pos if p['is_new'])
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("13F long notional", fmt_money_m(b['long_value']))
    c2.metric("Long book", fmt_money_m(b['long_value']))
    c3.metric("Positions", f"{b['n_pos']} L")
    c4.metric("Top-5 weight", f"{b['top5_weight']:.0f}%")

    left, right = st.columns([1.4, 1])
    with left:
        _manager_waterfall(name, b, books)
    with right:
        _concentration_gauge(b, books)

    # composition treemap
    st.subheader("Portfolio composition")
    tdf = pd.DataFrame(pos[:30])
    if not tdf.empty:
        fig = px.treemap(tdf, path=['ticker'], values='value')
        fig.update_traces(marker=dict(line=dict(width=1, color="#0d0f15")),
                          hovertemplate="%{label}<br>$%{value:.0f}M<extra></extra>")
        fig.update_layout(height=300, margin=dict(t=6, b=6, l=0, r=0))
        st.plotly_chart(fig, width="stretch", config={'displayModeBar': False})
        takeaway("Position sizes across the long book.", f"{name}, {CURRENT_Q.replace('_',' ')} 13F-HR.")

    # options overlay donut — only if metadata.institutions present (options data underivable otherwise)
    insts_meta = meta.get('institutions', {}).get(name) if isinstance(meta.get('institutions'), dict) else None
    if insts_meta and (insts_meta.get('puts_value') or insts_meta.get('calls_value')):
        _options_donut(insts_meta)

    # holdings table with weight bars + QoQ badges
    st.subheader("Holdings")
    def badge(p):
        if p['is_new']:
            return "NEW"
        pv = p['prev_shares']
        if pv and p['shares_change']:
            return f"{p['shares_change']/pv*100:+.0f}%"
        return "—"
    hd = pd.DataFrame([{
        "Ticker": p['ticker'], "Company": p['name'][:28], "Weight": p['value'] / b['long_value'] * 100 if b['long_value'] else 0,
        "Value ($M)": p['value'], "Shares": p['shares'], "% of co.": p['pct_co'], "QoQ": badge(p)} for p in pos])
    st.dataframe(hd, width="stretch", hide_index=True, height=420, column_config={
        "Weight": st.column_config.ProgressColumn(format="%.1f%%", min_value=0,
                                                  max_value=float(hd['Weight'].max()) if len(hd) else 100),
        "Value ($M)": st.column_config.NumberColumn(format="$%.0f M"),
        "Shares": st.column_config.NumberColumn(format="%d"),
        "% of co.": st.column_config.NumberColumn(format="%.2f%%")})
    if b['exits']:
        st.markdown(f"**Q1 exits** · <span style='color:{RED}'>" + " · ".join(b['exits'][:12]) +
                    (f" +{len(b['exits'])-12}" if len(b['exits']) > 12 else "") + "</span>",
                    unsafe_allow_html=True)


def _manager_waterfall(name, b, books):
    st.markdown("**LONG BOOK — QoQ WATERFALL ($M)**")
    prev_q = prior_quarter(CURRENT_Q)
    pb = load_manager_books(prev_q).get(name) if prev_q else None
    cur = {p['ticker']: p['value'] for p in b['positions']}
    Cb = b['long_value']
    if not pb:
        st.caption("Prior quarter not retained — waterfall unlocks once history is kept (R-D1).")
        return
    prev = {p['ticker']: p['value'] for p in pb['positions']}
    Pb = pb['long_value']
    new_v = sum(v for t, v in cur.items() if t not in prev)
    exit_v = sum(v for t, v in prev.items() if t not in cur)
    cont = Cb - new_v - Pb + exit_v
    fig = go.Figure(go.Waterfall(
        orientation="v", measure=["absolute", "relative", "relative", "relative", "total"],
        x=["Prior book", "New", "Continuing", "Exits", "Current"],
        y=[Pb, new_v, cont, -exit_v, Cb],
        increasing=dict(marker=dict(color=ACCENT)), decreasing=dict(marker=dict(color=RED)),
        totals=dict(marker=dict(color="#454b57")), connector=dict(line=dict(color=EDGE))))
    fig.update_layout(height=300, margin=dict(t=6, b=6, l=0, r=0), showlegend=False)
    st.plotly_chart(fig, width="stretch", config={'displayModeBar': False})
    takeaway(f"{fmt_money_m(exit_v)} exited, {fmt_money_m(new_v)} new; continuing bar mixes flows and price moves.",
             f"{name}, {prev_q.replace('_',' ')} → {CURRENT_Q.replace('_',' ')}.")


def _concentration_gauge(b, books):
    st.markdown("**CONCENTRATION**")
    med = median([x['top5_weight'] for x in books.values()]) if books else 50
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=b['top5_weight'], number=dict(suffix="%", font=dict(size=26)),
        gauge=dict(axis=dict(range=[0, 100]), bar=dict(color=ACCENT),
                   threshold=dict(line=dict(color=INK, width=3), value=med),
                   steps=[dict(range=[0, 50], color="#191d24"), dict(range=[50, 100], color="#14171e")])))
    fig.update_layout(height=230, margin=dict(t=10, b=0, l=20, r=20))
    st.plotly_chart(fig, width="stretch", config={'displayModeBar': False})
    takeaway(f"Top-5 weight vs roster median ({med:.0f}%).", "plotly indicator.")


def _options_donut(insts_meta):
    st.subheader("Notional mix")
    longv = insts_meta.get('long_value', 0) / SCALE_FACTOR
    puts = insts_meta.get('puts_value', 0) / SCALE_FACTOR
    calls = insts_meta.get('calls_value', 0) / SCALE_FACTOR
    fig = go.Figure(go.Pie(labels=["Long", "Puts", "Calls"], values=[longv, puts, calls], hole=.55,
                           marker=dict(colors=[ACCENT, AMBER, DIM])))
    fig.update_layout(height=260, margin=dict(t=6, b=6, l=6, r=6))
    st.plotly_chart(fig, width="stretch", config={'displayModeBar': False})
    tot = longv + puts + calls
    takeaway(f"{puts/tot*100:.0f}% of notional is downside hedge." if tot else "Options overlay.",
             "metadata.institutions options notional.")


def page_securities():
    sync_qp(page="security")
    drop_qp("m")
    df, meta = load_conviction(CURRENT_Q)
    roster = load_roster()
    st.title("Securities")
    if df.empty:
        st.info("No roster holdings in this quarter.")
        return
    tickers = sorted(df['ticker'].unique())
    want = _qp.get("t", "")
    idx = tickers.index(want) + 1 if want in tickers else 0
    sel = st.selectbox("Search ticker", ["—"] + tickers, index=idx,
                       help="Type-ahead over roster-held securities")
    if sel == "—":
        vintage_eyebrow(CURRENT_Q, meta)
        st.caption("Pick a ticker to see conviction holders, entries and exits.")
        return
    sync_qp(t=sel)
    row = df[df['ticker'] == sel].iloc[0]
    vintage_eyebrow(CURRENT_Q, meta, prefix=f"{html.escape(row['name'])} · CUSIP {row['cusip']} · ")
    copy_link({"page": "security", "t": sel, "q": CURRENT_Q})
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Conviction holders", row['r_num'])
    c2.metric("Held by roster", fmt_money_m(row['r_value']))
    c3.metric("% shares out", f"{row['r_pct']:.2f}%")
    c4.metric("Q1 exits", row['n_exits'])

    left, right = st.columns([1.6, 1])
    with left:
        st.markdown("**HOLDERS BY POSITION**")
        def badge(inst):
            if inst in row['new_holders']:
                return "NEW"
            ch = row['institution_changes'].get(inst, {})
            pv, sc = ch.get('prev_shares', 0), ch.get('shares_change', 0)
            return f"{sc/pv*100:+.0f}%" if pv and sc else "—"
        hd = pd.DataFrame([{
            "Manager": inst, "Value ($M)": p['value'] / SCALE_FACTOR,
            "% of co.": p.get('pct_of_company_shares', 0) or 0, "QoQ": badge(inst)}
            for inst, p in sorted(row['positions'].items(), key=lambda kv: kv[1]['value'], reverse=True)])
        st.dataframe(hd, width="stretch", hide_index=True, height=380, column_config={
            "Value ($M)": st.column_config.NumberColumn(format="$%.1f M"),
            "% of co.": st.column_config.NumberColumn(format="%.3f%%")})
    with right:
        st.markdown(f"<span style='color:{RED}'>**EXITED THIS QUARTER**</span>", unsafe_allow_html=True)
        if row['dropped_holders']:
            st.markdown('<table class="ctab">' + "".join(
                f'<tr><td>{mgr_link(x)}</td><td class="r"><span class="pill exit">OUT</span></td></tr>'
                for x in row['dropped_holders']) + '</table>', unsafe_allow_html=True)
        else:
            st.caption("No full exits this quarter.")
        _security_trend(df, sel)


def _security_trend(df, ticker):
    st.markdown("**CONVICTION OWNERSHIP · BY QUARTER**")
    prev_q = prior_quarter(CURRENT_Q)
    pts = []
    if prev_q:
        pdf, _ = load_conviction(prev_q)
        pr = pdf[pdf['ticker'] == ticker]
        if not pr.empty:
            pts.append((prev_q.replace('_', ' '), pr.iloc[0]['r_num'], pr.iloc[0]['r_value']))
    cur = df[df['ticker'] == ticker].iloc[0]
    pts.append((CURRENT_Q.replace('_', ' '), cur['r_num'], cur['r_value']))
    if len(pts) < 2:
        st.caption("Trend unlocks as more quarters are retained (R-D1).")
        return
    tdf = pd.DataFrame(pts, columns=["Quarter", "Holders", "Value"])
    fig = px.area(tdf, x="Quarter", y="Holders", markers=True)
    fig.update_traces(line=dict(color=ACCENT), fillcolor="rgba(86,220,133,.12)")
    fig.update_layout(height=180, margin=dict(t=6, b=6, l=6, r=6))
    st.plotly_chart(fig, width="stretch", config={'displayModeBar': False})
    takeaway(f"Roster holder count {tdf['Holders'].iloc[0]}→{tdf['Holders'].iloc[-1]} across retained quarters.",
             "conviction dataset, retained quarters.")


def page_screener():
    sync_qp(page="screener")
    drop_qp("t", "m")
    df, meta = load_conviction(CURRENT_Q)
    roster = load_roster()
    st.title("Screener")
    vintage_eyebrow(CURRENT_Q, meta)
    if df.empty:
        st.info("No roster holdings in this quarter.")
        return
    types = sorted({r.get('Type', '') for r in roster.values() if r.get('Type')})
    managers = sorted(roster)
    with st.sidebar:
        st.markdown("**Filters**")
        f_ticker = st.text_input("Ticker", "").upper().strip()
        f_types = st.multiselect("Strategy types", types)
        f_invs = st.multiselect("Specific managers", managers)
        f_own = st.slider("Ownership % of shares out", 0.0, 100.0, (0.0, 100.0))
        maxv = float(df['r_value'].max())
        f_val = st.slider("Roster value ($M)", 0.0, maxv, (0.0, maxv))
        f_hold = st.slider("Min holders", 1, int(df['r_num'].max()), 1)
        view = st.radio("View", ["Table", "Scatter"], horizontal=True)

    d = df.copy()
    # per-security type of the largest roster holder (for clustering/color + type filter)
    def dom_type(r):
        top = max(r['positions'].items(), key=lambda kv: kv[1]['value'])[0]
        return roster.get(top, {}).get('Type', 'Other')
    d['Type'] = d.apply(dom_type, axis=1)
    if f_ticker:
        d = d[d['ticker'] == f_ticker]
    if f_invs:
        d = d[d['r_holders'].apply(lambda hs: any(i in hs for i in f_invs))]
    if f_types:
        keep = {n for n, r in roster.items() if r.get('Type') in f_types}
        # recompute roster value/holders to selected-type holders only (roster-only math, R-A4)
        recs = []
        for i, r in d.iterrows():
            sub = {k: v for k, v in r['positions'].items() if k in keep}
            if not sub:
                continue
            rs = sum(p['shares'] for p in sub.values())
            so = r['shares_outstanding']
            recs.append((i, sum(p['value'] for p in sub.values()) / SCALE_FACTOR, len(sub),
                         rs / so * 100 if so else 0))
        if recs:
            d = d.loc[[x[0] for x in recs]].copy()
            d['r_value'] = [x[1] for x in recs]
            d['r_num'] = [x[2] for x in recs]
            d['r_pct'] = [x[3] for x in recs]
        else:
            d = d.iloc[0:0]
    d = d[(d['r_pct'] >= f_own[0]) & (d['r_pct'] <= f_own[1])]
    d = d[(d['r_value'] >= f_val[0]) & (d['r_value'] <= f_val[1])]
    d = d[d['r_num'] >= f_hold]
    d = d.sort_values('r_value', ascending=False)
    st.caption(f"{len(d):,} securities match — roster-only value & ownership.")

    if view == "Scatter":
        if d.empty:
            st.info("No securities match filters.")
            return
        fig = px.scatter(d, x='r_pct', y='r_num', size='r_value', color='Type', hover_name='ticker',
                         labels={'r_pct': '% shares out (roster)', 'r_num': 'Holders'})
        fig.update_layout(height=460, margin=dict(t=6, b=6, l=6, r=6))
        st.plotly_chart(fig, width="stretch", config={'displayModeBar': False})
        takeaway("Ownership vs holder count, clustered by dominant-holder strategy.", f"{CURRENT_Q.replace('_',' ')} conviction dataset.")
        return

    show = pd.DataFrame({
        "Ticker": d['ticker'], "Name": d['name'].str[:32], "Holders": d['r_num'],
        "Roster value ($M)": d['r_value'], "Roster % shares out": d['r_pct']})
    st.dataframe(show, width="stretch", hide_index=True, height=460, column_config={
        "Roster value ($M)": st.column_config.NumberColumn(format="$%.0f M"),
        "Roster % shares out": st.column_config.ProgressColumn(format="%.2f%%", min_value=0,
                                    max_value=float(show['Roster % shares out'].max()) if len(show) else 100)})
    csv_df = show.copy()  # already in $M / scaled dollars, never millicents
    st.download_button("⬇ Export CSV ($M)", csv_df.to_csv(index=False),
                       file_name=f"conviction_screener_{CURRENT_Q}.csv", mime="text/csv")


# ============================================================ NAV ==============
_pages = [
    st.Page(page_signals, title="Signals", url_path="signals", default=(_page in ("", "signals"))),
    st.Page(page_managers, title="Managers", url_path="managers", default=(_page == "managers")),
    st.Page(page_securities, title="Securities", url_path="securities",
            default=(_page in ("security", "securities"))),
    st.Page(page_screener, title="Screener", url_path="screener", default=(_page == "screener")),
]
st.navigation(_pages, position="top").run()
