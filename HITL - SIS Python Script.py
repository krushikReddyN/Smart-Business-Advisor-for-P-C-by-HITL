# ════════════════════════════════════════════════════════════════════════════════
# SMART P&C ADVISOR — UNDERWRITING INTELLIGENCE DASHBOARD
# Streamlit-in-Snowflake application for Property & Casualty insurance
# Team HITL: Abhijeet Zagade · Krushik Reddy · Prakash Vinukonda
# ════════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────
import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session
from streamlit_option_menu import option_menu
import pandas as pd
from json import dumps as json_dumps
import pydeck as pdk
import json
import math
import io

# ─────────────────────────────────────────
# SNOWFLAKE SESSION & PAGE CONFIG
# ─────────────────────────────────────────
session = get_active_session()
st.set_page_config(layout="wide")

# ─────────────────────────────────────────
# DATABASE CONSTANTS & HELPER
# ─────────────────────────────────────────
DB = "HITL_SBA_DB"
SCH = "PC_INSURANCE"
FQ = lambda t: f"{DB}.{SCH}.{t}"

# ─────────────────────────────────────────
# THEME COLORS
# ─────────────────────────────────────────
ACCENT = "#B3D943"
ACCENT_DARK = "#5a8a0f"
DANGER = "#e74c3c"
WARN = "#f39c12"
MUTED = "#94a3b8"
BG_CARD = "#fafbfc"

# ─────────────────────────────────────────
# CUSTOM CSS INJECTION
# ─────────────────────────────────────────
st.html(f"""
<style>
    .block-container {{ padding-top: 0.75rem; padding-bottom: 1; }}
    [data-testid="stSidebar"] {{ background: #ffffff; }}
    [data-testid="stSidebar"][aria-expanded="true"] {{ min-width: 280px; max-width: 280px; }}
    [data-testid="collapsedControl"] {{ display: none !important; }}
    [data-testid="stSidebar"] button[kind="header"] {{ display: none !important; }}
    [data-testid="stMetric"] {{
        background: {BG_CARD};
        border-left: 3px solid {ACCENT};
        border-radius: 6px;
        padding: 12px 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }}
    [data-testid="stMetric"] label {{ color: {MUTED} !important; font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.03em; }}
    [data-testid="stMetric"] [data-testid="stMetricValue"] {{ color: #1e293b !important; font-weight: 600; }}
    [data-testid="stMetric"] [data-testid="stMetricDelta"] {{ color: {ACCENT_DARK} !important; }}
    div[data-testid="stExpander"] {{ border: 1px solid #e2e8f0; border-radius: 6px; }}
    [data-testid='stHeaderActionElements'] {{display: none;}}
    [data-testid="stSidebar"] {{ border-right: 1px solid #d1d5db; }}
    h1, h2, h3 {{ font-weight: 600; color: #1e293b; }}
</style>
""")

# ─────────────────────────────────────────
# DATA LOADING — MASTER JOIN ACROSS ALL 7 TABLES
# ─────────────────────────────────────────
@st.cache_data(ttl=600)
def load_master_data():
    return session.sql(f"""
        SELECT p.PROPERTY_ID, p.POLICY_NUMBER, p.PROVIDER_NAME,
               p.ADDRESS, p.CITY, p.STATE, p.ZIP,
               p.LATITUDE, p.LONGITUDE, p.PROPERTY_TYPE, p.YEAR_BUILT,
               p.ROOF_AGE, p.CONSTRUCTION_TYPE, p.INSURED_VALUE,
               m.RISK_SCORE, m.RISK_CATEGORY, m.RECOMMENDED_ACTION, m.SUGGESTED_PREMIUM,
               r.FLOOD_ZONE, r.WILDFIRE_RISK, r.CRIME_INDEX, r.DISTANCE_TO_COAST_KM, r.HAZARD_SCORE,
               w.AVG_ANNUAL_RAINFALL, w.STORM_FREQUENCY, w.AVG_TEMPERATURE, w.HURRICANE_EVENTS_LAST_10Y,
               a.ROOF_CONDITION_SCORE, a.ROOF_MATERIAL, a.STRUCTURAL_RISK_SCORE, a.VEGETATION_DENSITY,
               b.AVG_MARKET_PREMIUM, b.MIN_PREMIUM, b.MAX_PREMIUM
        FROM {FQ('PROPERTY_MASTER')} p
        JOIN {FQ('MODEL_OUTPUT')} m ON p.PROPERTY_ID = m.PROPERTY_ID
        JOIN {FQ('RISK_GEOSPATIAL')} r ON p.PROPERTY_ID = r.PROPERTY_ID
        JOIN {FQ('WEATHER_HISTORY')} w ON p.PROPERTY_ID = w.PROPERTY_ID
        JOIN {FQ('AERIAL_ANALYSIS')} a ON p.PROPERTY_ID = a.PROPERTY_ID
        JOIN {FQ('PREMIUM_BENCHMARK')} b ON p.PROPERTY_ID = b.PROPERTY_ID
    """).to_pandas()

# DATA LOADING — CLAIMS HISTORY
@st.cache_data(ttl=600)
def load_claims():
    return session.sql(f"SELECT * FROM {FQ('CLAIMS_HISTORY')}").to_pandas()


df = load_master_data()
claims_df = load_claims()

# ════════════════════════════════════════════════════════════════════════════════
# SIDEBAR — FILTERS & BRANDING
# ════════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    # SIDEBAR HEADER
    st.html(f'''
    <div style="text-align:center; padding:8px 0 4px 0;">
        <div style="font-size:1.3rem; font-weight:700; color:{ACCENT};">Smart P&C Advisor</div>
        <div style="font-size:0.75rem; color:{MUTED}; letter-spacing:0.05em; text-transform:uppercase;">Underwriting Intelligence</div>
    </div>
    <hr style="margin:8px 0; border:none; border-top:1px solid #e2e8f0;">
    ''')

    # FILTER CONTROLS
    sel_states = st.multiselect("State", sorted(df["STATE"].unique()), default=[])
    sel_type = st.multiselect("Property type", sorted(df["PROPERTY_TYPE"].unique()), default=[])
    sel_risk = st.multiselect("Risk category", ["High", "Medium", "Low"], default=[])
    sel_action = st.segmented_control("Recommendation", ["All", "Write", "Decline"], default="All")

    # SIDEBAR FOOTER — TEAM CREDITS
    st.html(f'''
    <hr style="margin:8px 0; border:none; border-top:1px solid #e2e8f0;">
    <div style="text-align:center; padding:4px 0;">
        <div style="font-size:0.65rem; color:{ACCENT}; font-weight:700; letter-spacing:0.08em; text-transform:uppercase; margin-bottom:4px;">TEAM HITL</div>
        <div style="font-size:0.75rem; color:{MUTED}; line-height:1.5;">Abhijeet Zagade<br>Krushik Reddy<br>Prakash Vinukonda</div>
    </div>
    ''')

# ─────────────────────────────────────────
# APPLY SIDEBAR FILTERS TO DATAFRAME
# ─────────────────────────────────────────
fdf = df.copy()
if sel_states:
    fdf = fdf[fdf["STATE"].isin(sel_states)]
if sel_type:
    fdf = fdf[fdf["PROPERTY_TYPE"].isin(sel_type)]
if sel_risk:
    fdf = fdf[fdf["RISK_CATEGORY"].isin(sel_risk)]
if sel_action and sel_action != "All":
    fdf = fdf[fdf["RECOMMENDED_ACTION"] == sel_action]
st.write(" ")

# ════════════════════════════════════════════════════════════════════════════════
# HORIZONTAL NAVIGATION BAR — 8 TABS
# ════════════════════════════════════════════════════════════════════════════════
selected = option_menu(
    menu_title=None,
    options=["Portfolio", "Property Lookup", "Geospatial", "Claims", "ML Classifier", "Image Analysis", "AI Assistant", "Data Explorer"],
    icons=["bar-chart-line", "search", "globe2", "file-earmark-text", "cpu", "camera", "robot", "table"],
    default_index=0,
    orientation="horizontal",
    styles={
        "container": {"padding": "0!important", "background-color": BG_CARD, "border-radius": "8px", "margin-bottom": "16px", "border": "1px solid #e2e8f0"},
        "icon": {"color": ACCENT_DARK, "font-size": "14px"},
        "nav-link": {"font-size": "13px", "text-align": "center", "margin": "0", "color": "#475569", "padding": "10px 12px", "border-radius": "6px"},
        "nav-link-selected": {"background-color": ACCENT, "color": "#ffffff", "border-radius": "6px", "font-weight": "600"},
    },
)
st.html('<hr style="margin:0 0 12px 0; border:none; border-top:1px solid #e2e8f0;">')

# ════════════════════════════════════════════════════════════════════════════════
# TOP KPI BANNER — 6 PORTFOLIO-LEVEL METRICS
# ════════════════════════════════════════════════════════════════════════════════
total = len(fdf)
write_pct = (fdf["RECOMMENDED_ACTION"] == "Write").mean() * 100 if total > 0 else 0
avg_premium = fdf["SUGGESTED_PREMIUM"].mean() if total > 0 else 0
high_pct = (fdf["RISK_CATEGORY"] == "High").mean() * 100 if total > 0 else 0
avg_risk = fdf["RISK_SCORE"].mean() if total > 0 else 0
total_insured = fdf["INSURED_VALUE"].sum() if total > 0 else 0

kpis = [
    ("Properties", f"{total:,}"),
    ("Write Rate", f"{write_pct:.1f}%"),
    ("Avg Premium", f"${avg_premium:,.0f}"),
    ("High Risk", f"{high_pct:.1f}%"),
    ("Avg Risk", f"{avg_risk:.3f}"),
    ("Total Insured", f"${total_insured / 1e9:.2f}B"),
]

# RENDER KPI CARDS AS HTML
kpi_html = '<div style="display:flex; gap:12px; margin-bottom:12px;">'
for label, value in kpis:
    kpi_html += f'''
    <div style="flex:1; background:#fff; border:1px solid #d1d5db; border-left:3px solid {ACCENT};
                border-radius:8px; padding:14px 16px; box-shadow:0 1px 3px rgba(0,0,0,0.06);">
        <div style="font-size:0.75rem; color:{MUTED}; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px;">{label}</div>
        <div style="font-size:1.25rem; font-weight:700; color:#1e293b;">{value}</div>
    </div>'''
kpi_html += '</div>'
st.html(kpi_html)

# ─────────────────────────────────────────
# ALTAIR COLOR SCALES — REUSED ACROSS CHARTS
# ─────────────────────────────────────────
color_scale = alt.Scale(domain=["High", "Medium", "Low"], range=[DANGER, WARN, ACCENT])
action_color = alt.Scale(domain=["Write", "Decline"], range=[ACCENT, DANGER])

# ════════════════════════════════════════════════════════════════════════════════
# TAB 1: PORTFOLIO — RISK ANALYTICS & CHARTS
# ════════════════════════════════════════════════════════════════════════════════
if selected == "Portfolio":

    # ROW 1: RISK DONUT + WRITE/DECLINE BY STATE
    row1a, row1b = st.columns(2)
    with row1a:
        with st.container(border=True):
            st.markdown("##### Risk Distribution")
            risk_counts = fdf["RISK_CATEGORY"].value_counts().reset_index()
            risk_counts.columns = ["Risk Category", "Count"]
            st.altair_chart(
                alt.Chart(risk_counts).mark_arc(innerRadius=60, outerRadius=110).encode(
                    theta="Count:Q",
                    color=alt.Color("Risk Category:N", scale=color_scale, legend=alt.Legend(orient="bottom")),
                    tooltip=["Risk Category", "Count"],
                ).properties(height=280),
                use_container_width=True,
            )
    with row1b:
        with st.container(border=True):
            st.markdown("##### Write vs Decline by State")
            state_action = fdf.groupby(["STATE", "RECOMMENDED_ACTION"]).size().reset_index(name="Count")
            st.altair_chart(
                alt.Chart(state_action).mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
                    x=alt.X("STATE:N", sort="-y", title=None), y="Count:Q",
                    color=alt.Color("RECOMMENDED_ACTION:N", scale=action_color, title="Action", legend=alt.Legend(orient="bottom")),
                    tooltip=["STATE", "RECOMMENDED_ACTION", "Count"],
                ).properties(height=280),
                use_container_width=True,
            )

    # ROW 2: SUGGESTED VS MARKET PREMIUM SCATTER + RISK SCORE HISTOGRAM
    row2a, row2b = st.columns(2)
    with row2a:
        with st.container(border=True):
            st.markdown("##### Suggested vs Market Premium")
            scatter_data = fdf[["SUGGESTED_PREMIUM", "AVG_MARKET_PREMIUM", "RISK_CATEGORY", "PROPERTY_ID"]].copy()
            line_data = pd.DataFrame({"x": [0, scatter_data["AVG_MARKET_PREMIUM"].max()], "y": [0, scatter_data["AVG_MARKET_PREMIUM"].max()]})
            points = alt.Chart(scatter_data).mark_circle(size=35, opacity=0.5).encode(
                x=alt.X("AVG_MARKET_PREMIUM:Q", title="Market ($)"), y=alt.Y("SUGGESTED_PREMIUM:Q", title="Suggested ($)"),
                color=alt.Color("RISK_CATEGORY:N", scale=color_scale, legend=alt.Legend(orient="bottom")),
                tooltip=["PROPERTY_ID", "SUGGESTED_PREMIUM", "AVG_MARKET_PREMIUM", "RISK_CATEGORY"],
            )
            ref_line = alt.Chart(line_data).mark_line(strokeDash=[5, 5], color="#cbd5e1").encode(x="x:Q", y="y:Q")
            st.altair_chart((points + ref_line).properties(height=280), use_container_width=True)
    with row2b:
        with st.container(border=True):
            st.markdown("##### Risk Score Distribution")
            st.altair_chart(
                alt.Chart(fdf).mark_bar(opacity=0.7, color=ACCENT, cornerRadiusTopLeft=2, cornerRadiusTopRight=2).encode(
                    x=alt.X("RISK_SCORE:Q", bin=alt.Bin(maxbins=40), title="Risk Score"), y=alt.Y("count():Q", title="Count"),
                ).properties(height=280),
                use_container_width=True,
            )

    # ROW 3: RISK BY CONSTRUCTION TYPE + HAZARD VS ROOF CONDITION
    row3a, row3b = st.columns(2)
    with row3a:
        with st.container(border=True):
            st.markdown("##### Risk by Construction Type")
            const_risk = fdf.groupby("CONSTRUCTION_TYPE")["RISK_SCORE"].mean().reset_index()
            const_risk.columns = ["Type", "Avg Risk"]
            st.altair_chart(
                alt.Chart(const_risk).mark_bar(color=ACCENT, cornerRadiusEnd=3).encode(
                    x="Avg Risk:Q", y=alt.Y("Type:N", sort="-x"), tooltip=["Type", alt.Tooltip("Avg Risk:Q", format=".4f")]
                ).properties(height=240),
                use_container_width=True,
            )
    with row3b:
        with st.container(border=True):
            st.markdown("##### Hazard vs Roof Condition")
            st.altair_chart(
                alt.Chart(fdf).mark_circle(size=25, opacity=0.4).encode(
                    x=alt.X("HAZARD_SCORE:Q", title="Hazard"), y=alt.Y("ROOF_CONDITION_SCORE:Q", title="Roof Condition"),
                    color=alt.Color("RECOMMENDED_ACTION:N", scale=action_color, legend=alt.Legend(orient="bottom")),
                    tooltip=["PROPERTY_ID", "HAZARD_SCORE", "ROOF_CONDITION_SCORE"],
                ).properties(height=240),
                use_container_width=True,
            )

    st.divider()

    # COASTAL EXPOSURE — DUAL-AXIS CHART (BAR + LINE) WITH DATA TABLE
    with st.container(border=True):
        st.markdown("##### Coastal Exposure")

        @st.cache_data(ttl=600)
        def load_coastal_data():
            return session.sql(f"""
                SELECT CASE WHEN r.DISTANCE_TO_COAST_KM < 10 THEN '0-10 km'
                            WHEN r.DISTANCE_TO_COAST_KM < 50 THEN '10-50 km'
                            WHEN r.DISTANCE_TO_COAST_KM < 100 THEN '50-100 km'
                            WHEN r.DISTANCE_TO_COAST_KM < 200 THEN '100-200 km'
                            ELSE '200+ km' END AS COAST_BAND,
                       COUNT(*) AS PROPERTY_COUNT, AVG(m.RISK_SCORE) AS AVG_RISK,
                       SUM(p.INSURED_VALUE) AS TOTAL_EXPOSURE, AVG(r.HAZARD_SCORE) AS AVG_HAZARD,
                       AVG(m.SUGGESTED_PREMIUM) AS AVG_PREMIUM,
                       SUM(CASE WHEN m.RISK_CATEGORY = 'High' THEN 1 ELSE 0 END) AS HIGH_RISK_COUNT,
                       SUM(CASE WHEN r.FLOOD_ZONE = 'High' THEN 1 ELSE 0 END) AS HIGH_FLOOD_COUNT
                FROM {FQ('PROPERTY_MASTER')} p
                JOIN {FQ('MODEL_OUTPUT')} m ON p.PROPERTY_ID = m.PROPERTY_ID
                JOIN {FQ('RISK_GEOSPATIAL')} r ON p.PROPERTY_ID = r.PROPERTY_ID
                GROUP BY COAST_BAND
                ORDER BY CASE COAST_BAND WHEN '0-10 km' THEN 1 WHEN '10-50 km' THEN 2 WHEN '50-100 km' THEN 3 WHEN '100-200 km' THEN 4 ELSE 5 END
            """).to_pandas()

        coastal_df = load_coastal_data()
        coast_order = ["0-10 km", "10-50 km", "50-100 km", "100-200 km", "200+ km"]
        bars = alt.Chart(coastal_df).mark_bar(color=ACCENT, cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
            x=alt.X("COAST_BAND:N", sort=coast_order, title="Distance to Coast"),
            y=alt.Y("TOTAL_EXPOSURE:Q", title="Total Exposure ($)"),
            tooltip=["COAST_BAND", "PROPERTY_COUNT", alt.Tooltip("AVG_RISK:Q", format=".4f"), alt.Tooltip("TOTAL_EXPOSURE:Q", format="$,.0f")],
        ).properties(height=280)
        risk_line = alt.Chart(coastal_df).mark_line(color=DANGER, strokeWidth=2, point=True).encode(
            x=alt.X("COAST_BAND:N", sort=coast_order), y=alt.Y("AVG_RISK:Q", title="Avg Risk", axis=alt.Axis(titleColor=DANGER)),
        )
        st.altair_chart(alt.layer(bars, risk_line).resolve_scale(y="independent"), use_container_width=True)

        st.dataframe(coastal_df, hide_index=True, column_config={
            "AVG_RISK": st.column_config.ProgressColumn("Avg Risk", min_value=0, max_value=1, format="%.4f"),
            "AVG_HAZARD": st.column_config.ProgressColumn("Avg Hazard", min_value=0, max_value=1, format="%.3f"),
            "TOTAL_EXPOSURE": st.column_config.NumberColumn("Total Exposure", format="$%d"),
            "AVG_PREMIUM": st.column_config.NumberColumn("Avg Premium", format="$%.0f"),
        }, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════════
# TAB 2: PROPERTY LOOKUP — SINGLE PROPERTY DEEP DIVE
# ════════════════════════════════════════════════════════════════════════════════
elif selected == "Property Lookup":

    # PROPERTY SELECTOR
    c_search, _ = st.columns([1.5, 3])
    with c_search:
        prop_id = st.selectbox("Select property", fdf["PROPERTY_ID"].sort_values().unique())

    if prop_id:
        row = fdf[fdf["PROPERTY_ID"] == prop_id].iloc[0]
        is_write = row["RECOMMENDED_ACTION"] == "Write"
        risk_color = {"High": "red", "Medium": "orange", "Low": "green"}.get(row["RISK_CATEGORY"], "gray")

        rec_col, detail_col = st.columns([1, 2])

        # UNDERWRITING DECISION PANEL
        with rec_col:
            with st.container(border=True):
                st.markdown("##### Underwriting Decision")
                if is_write:
                    st.success("**WRITE** this policy", icon=":material/check_circle:")
                else:
                    st.error("**DECLINE** this policy", icon=":material/cancel:")
                st.badge(f"Risk: {row['RISK_CATEGORY']}", icon=":material/warning:", color=risk_color)
                st.metric("Suggested Premium", f"${row['SUGGESTED_PREMIUM']:,.2f}")
                st.metric("Risk Score", f"{row['RISK_SCORE']:.4f}")
                mkt = row["AVG_MARKET_PREMIUM"]
                diff = ((row["SUGGESTED_PREMIUM"] - mkt) / mkt * 100) if mkt > 0 else 0
                st.metric("Market Benchmark", f"${mkt:,.2f}", delta=f"{diff:+.1f}% vs suggested")

        with detail_col:
            # PROPERTY DETAILS — POLICY, ADDRESS, CONSTRUCTION
            with st.container(border=True):
                st.markdown("##### Property Details")
                p0a, p0b, p0c = st.columns(3)
                p0a.markdown(f"**Policy #:** {row['POLICY_NUMBER']}")
                p0b.markdown(f"**Provider:** {row['PROVIDER_NAME']}")
                p0c.markdown(f"**Type:** {row['PROPERTY_TYPE']}")
                p1, p2, p3 = st.columns(3)
                p1.markdown(f"**Address:** {row['ADDRESS']}")
                p2.markdown(f"**City/State:** {row['CITY']}, {row['STATE']} {row['ZIP']}")
                p3.markdown(f"**Insured:** ${row['INSURED_VALUE']:,.0f}")
                p4, p5, p6 = st.columns(3)
                p4.markdown(f"**Year built:** {int(row['YEAR_BUILT'])}")
                p5.markdown(f"**Construction:** {row['CONSTRUCTION_TYPE']}")
                p6.markdown(f"**Roof Age:** {int(row['ROOF_AGE'])} yrs")

            # RISK FACTORS — GEOSPATIAL, WEATHER, AERIAL, BENCHMARK
            with st.container(border=True):
                st.markdown("##### Risk Factors")
                rf1, rf2, rf3, rf4 = st.columns(4)
                with rf1:
                    st.caption("Geospatial")
                    st.markdown(f"Flood: **{row['FLOOD_ZONE']}**")
                    st.markdown(f"Wildfire: **{row['WILDFIRE_RISK']}**")
                    st.markdown(f"Hazard: **{row['HAZARD_SCORE']:.3f}**")
                with rf2:
                    st.caption("Weather")
                    st.markdown(f"Storms/yr: **{int(row['STORM_FREQUENCY'])}**")
                    st.markdown(f"Hurricanes: **{int(row['HURRICANE_EVENTS_LAST_10Y'])}**")
                    st.markdown(f"Rainfall: **{row['AVG_ANNUAL_RAINFALL']:.1f}in**")
                with rf3:
                    st.caption("Aerial")
                    st.markdown(f"Roof: **{row['ROOF_CONDITION_SCORE']:.2f}**")
                    st.markdown(f"Material: **{row['ROOF_MATERIAL']}**")
                    st.markdown(f"Structural: **{row['STRUCTURAL_RISK_SCORE']:.2f}**")
                with rf4:
                    st.caption("Benchmark")
                    st.markdown(f"Market: **${row['AVG_MARKET_PREMIUM']:,.0f}**")
                    st.markdown(f"Range: **${row['MIN_PREMIUM']:,.0f}–${row['MAX_PREMIUM']:,.0f}**")

            # CLAIMS HISTORY FOR THIS PROPERTY
            prop_claims = claims_df[claims_df["PROPERTY_ID"] == prop_id]
            if len(prop_claims) > 0:
                with st.container(border=True):
                    st.markdown(f"##### Claims History ({len(prop_claims)})")
                    st.dataframe(
                        prop_claims.merge(df[["PROPERTY_ID", "POLICY_NUMBER", "PROVIDER_NAME"]], on="PROPERTY_ID", how="left")[["POLICY_NUMBER", "PROVIDER_NAME", "CLAIM_ID", "CLAIM_YEAR", "CLAIM_TYPE", "CLAIM_SEVERITY", "CLAIM_AMOUNT"]].sort_values("CLAIM_YEAR", ascending=False),
                        hide_index=True, column_config={"CLAIM_AMOUNT": st.column_config.NumberColumn(format="$%d")}, use_container_width=True,
                    )

# ════════════════════════════════════════════════════════════════════════════════
# TAB 3: GEOSPATIAL — MAPS, H3 HEXAGONS, PROXIMITY SEARCH
# ════════════════════════════════════════════════════════════════════════════════
elif selected == "Geospatial":

    # PROPERTY RISK MAP — COLORED BY RISK, SIZED BY INSURED VALUE
    with st.container(border=True):
        st.markdown("##### Property Risk Map")
        st.caption("Sized by insured value · Colored by risk")
        map_data = fdf[["LATITUDE", "LONGITUDE", "RISK_CATEGORY", "INSURED_VALUE", "PROPERTY_ID"]].copy()
        map_data = map_data.rename(columns={"LATITUDE": "lat", "LONGITUDE": "lon"})
        color_map_geo = {"High": [231, 76, 60, 160], "Medium": [243, 156, 18, 160], "Low": [179, 217, 67, 160]}
        map_data["color"] = map_data["RISK_CATEGORY"].map(color_map_geo)
        map_data["size"] = map_data["INSURED_VALUE"] / map_data["INSURED_VALUE"].max() * 800 + 100
        st.map(map_data, latitude="lat", longitude="lon", size="size", color="color")

    # FLOOD ZONE & WILDFIRE RISK BAR CHARTS
    gc1, gc2 = st.columns(2)
    with gc1:
        with st.container(border=True):
            st.markdown("##### Flood Zone")
            flood = fdf["FLOOD_ZONE"].value_counts().reset_index()
            flood.columns = ["Zone", "Count"]
            st.bar_chart(flood, x="Zone", y="Count", color=ACCENT)
    with gc2:
        with st.container(border=True):
            st.markdown("##### Wildfire Risk")
            fire = fdf["WILDFIRE_RISK"].value_counts().reset_index()
            fire.columns = ["Risk", "Count"]
            st.bar_chart(fire, x="Risk", y="Count", color=ACCENT)

    st.divider()

    # ─────────────────────────────────────────
    # H3 HEXAGONAL RISK AGGREGATION
    # ─────────────────────────────────────────
    st.markdown("##### H3 Hexagonal Risk Aggregation")
    h3_res = st.select_slider("H3 resolution", options=[3, 4, 5, 6, 7], value=5)

    @st.cache_data(ttl=600)
    def load_h3_data(resolution):
        return session.sql(f"""
            SELECT H3_INDEX, COUNT(*) AS PROPERTY_COUNT, AVG(RISK_SCORE) AS AVG_RISK,
                   SUM(INSURED_VALUE) AS TOTAL_INSURED, AVG(SUGGESTED_PREMIUM) AS AVG_PREMIUM,
                   SUM(CASE WHEN RISK_CATEGORY = 'High' THEN 1 ELSE 0 END) AS HIGH_RISK_COUNT,
                   AVG(HAZARD_SCORE) AS AVG_HAZARD, AVG(LATITUDE) AS CENTER_LAT, AVG(LONGITUDE) AS CENTER_LON
            FROM (
                SELECT H3_LATLNG_TO_CELL(p.LATITUDE, p.LONGITUDE, {resolution}) AS H3_INDEX,
                       p.LATITUDE, p.LONGITUDE, p.INSURED_VALUE,
                       m.RISK_SCORE, m.RISK_CATEGORY, m.SUGGESTED_PREMIUM, r.HAZARD_SCORE
                FROM {FQ('PROPERTY_MASTER')} p
                JOIN {FQ('MODEL_OUTPUT')} m ON p.PROPERTY_ID = m.PROPERTY_ID
                JOIN {FQ('RISK_GEOSPATIAL')} r ON p.PROPERTY_ID = r.PROPERTY_ID
            ) GROUP BY H3_INDEX ORDER BY AVG_RISK DESC
        """).to_pandas()

    h3_df = load_h3_data(h3_res)

    # H3 KPI METRICS
    hm1, hm2, hm3, hm4 = st.columns(4)
    hm1.metric("Hex Cells", f"{len(h3_df):,}")
    hm2.metric("Peak Risk", f"{h3_df['AVG_RISK'].max():.4f}" if len(h3_df) > 0 else "N/A")
    hm3.metric("Top Hex Props", f"{int(h3_df.iloc[0]['PROPERTY_COUNT']):,}" if len(h3_df) > 0 else "0")
    hm4.metric("Top Hex Insured", f"${h3_df.iloc[0]['TOTAL_INSURED'] / 1e6:.1f}M" if len(h3_df) > 0 else "$0")

    # PYDECK HEATMAP + SCATTERPLOT
    hex_map_data = h3_df[["CENTER_LAT", "CENTER_LON", "AVG_RISK", "PROPERTY_COUNT", "TOTAL_INSURED"]].copy()
    hex_map_data = hex_map_data.rename(columns={"CENTER_LAT": "lat", "CENTER_LON": "lon"})

    st.pydeck_chart(pdk.Deck(
        layers=[
            pdk.Layer("HeatmapLayer", data=hex_map_data, get_position=["lon", "lat"], get_weight="AVG_RISK",
                      radiusPixels=60, intensity=1, threshold=0.1,
                      color_range=[[179, 217, 67, 100], [243, 200, 18, 150], [243, 156, 18, 180], [231, 120, 60, 200], [231, 76, 60, 230]]),
            pdk.Layer("ScatterplotLayer", data=hex_map_data, get_position=["lon", "lat"],
                      get_radius="PROPERTY_COUNT * 2000", get_fill_color=[179, 217, 67, 140], pickable=True),
        ],
        initial_view_state=pdk.ViewState(
            latitude=hex_map_data["lat"].mean() if len(hex_map_data) > 0 else 39.0,
            longitude=hex_map_data["lon"].mean() if len(hex_map_data) > 0 else -98.0, zoom=3.5, pitch=45),
        tooltip={"text": "Properties: {PROPERTY_COUNT}\nAvg Risk: {AVG_RISK}\nInsured: ${TOTAL_INSURED}"},
    ))

    # TOP RISK HEXAGONS TABLE
    with st.expander("Top risk hexagons"):
        st.dataframe(
            h3_df[["H3_INDEX", "PROPERTY_COUNT", "AVG_RISK", "HIGH_RISK_COUNT", "AVG_HAZARD", "TOTAL_INSURED", "AVG_PREMIUM"]].head(15).reset_index(drop=True),
            hide_index=True,
            column_config={
                "AVG_RISK": st.column_config.ProgressColumn("Avg Risk", min_value=0, max_value=1, format="%.4f"),
                "AVG_HAZARD": st.column_config.ProgressColumn("Avg Hazard", min_value=0, max_value=1, format="%.3f"),
                "TOTAL_INSURED": st.column_config.NumberColumn("Total Insured", format="$%d"),
                "AVG_PREMIUM": st.column_config.NumberColumn("Avg Premium", format="$%.0f"),
            },
            use_container_width=True,
        )

    st.divider()

    # ─────────────────────────────────────────
    # PROXIMITY SEARCH — FIND PROPERTIES WITHIN RADIUS
    # ─────────────────────────────────────────
    st.markdown("##### Proximity Search")
    prox_col1, prox_col2, prox_col3 = st.columns([1, 1, 1])
    with prox_col1:
        ref_lat = st.number_input("Latitude", value=29.95, format="%.4f")
    with prox_col2:
        ref_lon = st.number_input("Longitude", value=-90.10, format="%.4f")
    with prox_col3:
        radius_km = st.slider("Radius (Miles)", 10, 500, 100)

    @st.cache_data(ttl=600)
    def load_proximity_data(lat, lon, radius):
        return session.sql(f"""
            SELECT p.PROPERTY_ID, p.ADDRESS, p.CITY, p.STATE, p.LATITUDE, p.LONGITUDE,
                   ROUND(ST_DISTANCE(ST_MAKEPOINT(p.LONGITUDE, p.LATITUDE), ST_MAKEPOINT({lon}, {lat})) / 1000, 2) AS DISTANCE_KM,
                   m.RISK_SCORE, m.RISK_CATEGORY, m.SUGGESTED_PREMIUM,
                   r.FLOOD_ZONE, r.WILDFIRE_RISK, r.HAZARD_SCORE, p.INSURED_VALUE
            FROM {FQ('PROPERTY_MASTER')} p
            JOIN {FQ('MODEL_OUTPUT')} m ON p.PROPERTY_ID = m.PROPERTY_ID
            JOIN {FQ('RISK_GEOSPATIAL')} r ON p.PROPERTY_ID = r.PROPERTY_ID
            WHERE ST_DISTANCE(ST_MAKEPOINT(p.LONGITUDE, p.LATITUDE), ST_MAKEPOINT({lon}, {lat})) / 1000 <= {radius}
            ORDER BY DISTANCE_KM
        """).to_pandas()

    if st.button("Search", type="primary", key="geo_prox_btn"):
        prox_df = load_proximity_data(ref_lat, ref_lon, radius_km)
        if len(prox_df) > 0:
            # PROXIMITY RESULTS KPIs
            pm1, pm2, pm3, pm4 = st.columns(4)
            pm1.metric("Found", f"{len(prox_df):,}")
            pm2.metric("Avg Distance", f"{prox_df['DISTANCE_KM'].mean():.1f} miles")
            pm3.metric("Avg Risk", f"{prox_df['RISK_SCORE'].mean():.4f}")
            pm4.metric("Exposure", f"${prox_df['INSURED_VALUE'].sum() / 1e6:.1f}M")

            # PROXIMITY MAP — PYDECK WITH REFERENCE POINT
            prox_map = prox_df[["LATITUDE", "LONGITUDE", "RISK_CATEGORY", "DISTANCE_KM", "PROPERTY_ID"]].copy()
            prox_map = prox_map.rename(columns={"LATITUDE": "lat", "LONGITUDE": "lon"})
            prox_color_map = {"High": [231, 76, 60, 200], "Medium": [243, 156, 18, 200], "Low": [179, 217, 67, 200]}
            prox_map["color"] = prox_map["RISK_CATEGORY"].map(prox_color_map)
            ref_point = pd.DataFrame({"lat": [ref_lat], "lon": [ref_lon], "color": [[30, 30, 30, 255]]})

            # AUTO-ZOOM CALCULATION
            lat_min, lat_max = prox_map["lat"].min(), prox_map["lat"].max()
            lon_min, lon_max = prox_map["lon"].min(), prox_map["lon"].max()
            max_spread = max(max(lat_max - lat_min, 0.01), max(lon_max - lon_min, 0.01))
            auto_zoom = max(4, min(14, int(math.log2(180 / max_spread)) + 1))
            auto_radius = max(200, min(2500, int(max_spread * 1200)))

            st.pydeck_chart(pdk.Deck(
                layers=[
                    pdk.Layer("ScatterplotLayer", data=prox_map, get_position=["lon", "lat"],
                              get_fill_color="color", get_radius=auto_radius, stroked=True,
                              get_line_color=[255, 255, 255, 160], line_width_min_pixels=1, pickable=True),
                    pdk.Layer("ScatterplotLayer", data=ref_point, get_position=["lon", "lat"],
                              get_fill_color="color", get_radius=auto_radius * 2.5),
                ],
                initial_view_state=pdk.ViewState(latitude=(lat_min + lat_max) / 2, longitude=(lon_min + lon_max) / 2, zoom=auto_zoom),
                tooltip={"text": "{PROPERTY_ID}\nDistance: {DISTANCE_KM} km\nRisk: {RISK_CATEGORY}"},
            ))

            # PROXIMITY RESULTS TABLE
            st.dataframe(
                prox_df[["PROPERTY_ID", "CITY", "STATE", "DISTANCE_KM", "RISK_SCORE", "RISK_CATEGORY", "FLOOD_ZONE", "WILDFIRE_RISK", "SUGGESTED_PREMIUM", "INSURED_VALUE"]].head(50),
                hide_index=True,
                column_config={
                    "DISTANCE_KM": st.column_config.NumberColumn("km", format="%.1f"),
                    "RISK_SCORE": st.column_config.ProgressColumn("Risk", min_value=0, max_value=1, format="%.4f"),
                    "SUGGESTED_PREMIUM": st.column_config.NumberColumn("Premium", format="$%.0f"),
                    "INSURED_VALUE": st.column_config.NumberColumn("Insured", format="$%d"),
                },
                use_container_width=True,
            )
        else:
            st.warning("No properties found within the specified radius.")


# ════════════════════════════════════════════════════════════════════════════════
# TAB 4: CLAIMS — CLAIMS ANALYTICS & TRENDS
# ════════════════════════════════════════════════════════════════════════════════
elif selected == "Claims":

    # FILTER CLAIMS TO MATCH SIDEBAR SELECTIONS
    fclaims = claims_df[claims_df["PROPERTY_ID"].isin(fdf["PROPERTY_ID"])]

    # CLAIMS KPIs
    cc1, cc2, cc3, cc4 = st.columns(4)
    cc1.metric("Total Claims", f"{len(fclaims):,}")
    cc2.metric("Total Paid", f"${fclaims['CLAIM_AMOUNT'].sum():,.0f}")
    cc3.metric("Avg Claim", f"${fclaims['CLAIM_AMOUNT'].mean():,.0f}" if len(fclaims) > 0 else "$0")
    cc4.metric("Claim Types", f"{fclaims['CLAIM_TYPE'].nunique()}")

    cr1, cr2 = st.columns(2)

    # CLAIMS BY YEAR — DUAL AXIS (COUNT BARS + AMOUNT LINE)
    with cr1:
        with st.container(border=True):
            st.markdown("##### Claims by Year")
            yearly = fclaims.groupby("CLAIM_YEAR").agg(Claims=("CLAIM_ID", "count"), Total_Amount=("CLAIM_AMOUNT", "sum")).reset_index()
            bars_c = alt.Chart(yearly).mark_bar(color=ACCENT, opacity=0.8, cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
                x=alt.X("CLAIM_YEAR:O", title="Year"), y=alt.Y("Claims:Q"),
                tooltip=["CLAIM_YEAR", "Claims", alt.Tooltip("Total_Amount:Q", format="$,.0f")],
            ).properties(height=280)
            line_c = alt.Chart(yearly).mark_line(color=ACCENT_DARK, strokeWidth=2).encode(
                x="CLAIM_YEAR:O", y=alt.Y("Total_Amount:Q", title="Total ($)", axis=alt.Axis(titleColor=ACCENT_DARK)),
            )
            st.altair_chart(alt.layer(bars_c, line_c).resolve_scale(y="independent"), use_container_width=True)

    # SEVERITY BREAKDOWN — DONUT CHART
    with cr2:
        with st.container(border=True):
            st.markdown("##### Severity Breakdown")
            sev = fclaims["CLAIM_SEVERITY"].value_counts().reset_index()
            sev.columns = ["Severity", "Count"]
            sev_colors = alt.Scale(domain=sev["Severity"].tolist(), range=[ACCENT, "#8fb532", "#d4ec8a", "#5a8a0f", "#e8f5c8"])
            st.altair_chart(
                alt.Chart(sev).mark_arc(innerRadius=60, outerRadius=110).encode(
                    theta="Count:Q", color=alt.Color("Severity:N", scale=sev_colors, legend=alt.Legend(orient="bottom")), tooltip=["Severity", "Count"],
                ).properties(height=280),
                use_container_width=True,
            )

    # CLAIM TYPE ANALYSIS TABLE
    with st.container(border=True):
        st.markdown("##### Claim Type Analysis")
        type_analysis = fclaims.groupby("CLAIM_TYPE").agg(
            Count=("CLAIM_ID", "count"), Avg_Amount=("CLAIM_AMOUNT", "mean"), Total_Amount=("CLAIM_AMOUNT", "sum"),
        ).reset_index().sort_values("Total_Amount", ascending=False)
        st.dataframe(type_analysis, hide_index=True, column_config={
            "Avg_Amount": st.column_config.NumberColumn("Avg Amount", format="$%.0f"),
            "Total_Amount": st.column_config.NumberColumn("Total", format="$%.0f"),
        }, use_container_width=True)

# ════════════════════════════════════════════════════════════════════════════════
# TAB 5: ML CLASSIFIER — SNOWFLAKE ML CLASSIFICATION
# ════════════════════════════════════════════════════════════════════════════════
elif selected == "ML Classifier":
    st.markdown("##### Snowflake ML Classification")
    st.caption("Predict **RISK_CATEGORY** (High / Medium / Low) for insurance properties using Snowflake's built-in ML Classification.")

    # FEATURE SELECTION
    feature_cols = st.multiselect(
        "Features", ["INSURED_VALUE", "HAZARD_SCORE", "DISTANCE_TO_COAST_KM", "CRIME_INDEX",
                     "STORM_FREQUENCY", "ROOF_CONDITION_SCORE", "STRUCTURAL_RISK_SCORE"],
        default=["HAZARD_SCORE", "DISTANCE_TO_COAST_KM", "CRIME_INDEX", "STORM_FREQUENCY",
                 "ROOF_CONDITION_SCORE", "STRUCTURAL_RISK_SCORE", "INSURED_VALUE"],
    )

    # TRAIN MODEL BUTTON
    if st.button("Train Model", type="primary", key="ml_train_btn"):
        if len(feature_cols) < 2:
            st.error("Select at least 2 features.")
        else:
            # STEP 1: CREATE TRAINING VIEW, TRAIN/TEST SPLIT, TRAIN CLASSIFIER
            with st.spinner("Training classifier..."):
                col_table_map = {"INSURED_VALUE": "t", "HAZARD_SCORE": "r", "DISTANCE_TO_COAST_KM": "r", "CRIME_INDEX": "r",
                                 "STORM_FREQUENCY": "w", "ROOF_CONDITION_SCORE": "a", "STRUCTURAL_RISK_SCORE": "a"}
                feature_list = ", ".join([f"{col_table_map.get(c, 't')}.{c}" for c in feature_cols])

                session.sql(f"""
                    CREATE OR REPLACE VIEW {FQ('ML_TRAINING_VIEW')} AS
                    SELECT t.PROPERTY_ID, {feature_list}, m.RISK_CATEGORY
                    FROM {FQ('PROPERTY_MASTER')} t JOIN {FQ('MODEL_OUTPUT')} m ON t.PROPERTY_ID = m.PROPERTY_ID
                    JOIN {FQ('RISK_GEOSPATIAL')} r ON t.PROPERTY_ID = r.PROPERTY_ID
                    JOIN {FQ('WEATHER_HISTORY')} w ON t.PROPERTY_ID = w.PROPERTY_ID
                    JOIN {FQ('AERIAL_ANALYSIS')} a ON t.PROPERTY_ID = a.PROPERTY_ID
                """).collect()
                session.sql(f"CREATE OR REPLACE VIEW {FQ('ML_TRAIN_SPLIT')} AS SELECT * EXCLUDE (PROPERTY_ID) FROM {FQ('ML_TRAINING_VIEW')} SAMPLE (80)").collect()
                session.sql(f"CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION {FQ('RISK_CLASSIFIER')}(INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{FQ('ML_TRAIN_SPLIT')}'), TARGET_COLNAME => 'RISK_CATEGORY')").collect()

            st.success("Model trained!")

            # STEP 2: PREDICT RISK FOR ALL PROPERTIES
            with st.spinner("Predicting risk for all properties..."):
                session.sql(f"""
                    CREATE OR REPLACE TABLE {FQ('ML_ALL_PREDICTIONS')} AS
                    SELECT PROPERTY_ID,
                           RISK_CATEGORY AS ACTUAL_RISK,
                           {FQ('RISK_CLASSIFIER')}!PREDICT(INPUT_DATA => OBJECT_CONSTRUCT(
                               {', '.join([f"'{c}', {c}" for c in feature_cols])}
                           )) AS PREDICTION
                    FROM {FQ('ML_TRAINING_VIEW')}
                """).collect()

                pred_df = session.sql(f"""
                    SELECT PROPERTY_ID,
                           ACTUAL_RISK,
                           PREDICTION:"class"::VARCHAR AS PREDICTED_RISK,
                           ROUND(PREDICTION['probability']['High']::FLOAT, 4) AS PROB_HIGH,
                           ROUND(PREDICTION['probability']['Medium']::FLOAT, 4) AS PROB_MEDIUM,
                           ROUND(PREDICTION['probability']['Low']::FLOAT, 4) AS PROB_LOW
                    FROM {FQ('ML_ALL_PREDICTIONS')}
                """).to_pandas()

            # STEP 3: DISPLAY RESULTS — ACCURACY, DISTRIBUTION, CONFUSION MATRIX
            if len(pred_df) > 0:
                correct = (pred_df["PREDICTED_RISK"] == pred_df["ACTUAL_RISK"]).sum()
                total = len(pred_df)

                # ACCURACY KPIs
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Total Properties", f"{total:,}")
                k2.metric("Accuracy", f"{correct/total:.1%}")
                k3.metric("Correct", f"{correct:,}")
                k4.metric("Misclassified", f"{total - correct:,}")

                r1, r2 = st.columns(2)

                # PREDICTED RISK DISTRIBUTION DONUT
                with r1:
                    dist = pred_df["PREDICTED_RISK"].value_counts().reset_index()
                    dist.columns = ["Risk Category", "Count"]
                    st.altair_chart(
                        alt.Chart(dist).mark_arc(innerRadius=40).encode(
                            theta="Count:Q",
                            color=alt.Color("Risk Category:N", scale=alt.Scale(domain=["High", "Medium", "Low"], range=["#ef4444", "#f59e0b", "#22c55e"])),
                            tooltip=["Risk Category", "Count"],
                        ).properties(height=250, title="Predicted Risk Distribution"),
                        use_container_width=True,
                    )

                # CONFUSION MATRIX HEATMAP
                with r2:
                    compare = pred_df.groupby(["ACTUAL_RISK", "PREDICTED_RISK"]).size().reset_index(name="Count")
                    st.altair_chart(
                        alt.Chart(compare).mark_rect().encode(
                            x=alt.X("PREDICTED_RISK:N", title="Predicted"),
                            y=alt.Y("ACTUAL_RISK:N", title="Actual"),
                            color=alt.Color("Count:Q", scale=alt.Scale(scheme="blues")),
                            tooltip=["ACTUAL_RISK", "PREDICTED_RISK", "Count"],
                        ).properties(height=250, title="Confusion Matrix"),
                        use_container_width=True,
                    )

                # FULL PREDICTIONS TABLE
                st.markdown("##### All Property Risk Predictions")
                st.dataframe(
                    pred_df.sort_values("PROB_HIGH", ascending=False),
                    hide_index=True, use_container_width=True,
                    column_config={
                        "PROB_HIGH": st.column_config.ProgressColumn("P(High)", min_value=0, max_value=1, format="%.4f"),
                        "PROB_MEDIUM": st.column_config.ProgressColumn("P(Medium)", min_value=0, max_value=1, format="%.4f"),
                        "PROB_LOW": st.column_config.ProgressColumn("P(Low)", min_value=0, max_value=1, format="%.4f"),
                    },
                )

    st.divider()

    # ─────────────────────────────────────────
    # AD-HOC SINGLE PROPERTY PREDICTION
    # ─────────────────────────────────────────
    with st.container(border=True):
        st.markdown("##### Ad-hoc Prediction")
        st.caption("Enter property attributes to get an instant risk classification.")
        ah1, ah2, ah3 = st.columns(3)
        with ah1:
            ah_hazard = st.number_input("Hazard Score", 0.0, 1.0, 0.5, 0.01)
            ah_coast = st.number_input("Coast Distance (km)", 0.0, 1000.0, 100.0, 10.0)
            ah_crime = st.number_input("Crime Index", 0, 100, 50, 1)
        with ah2:
            ah_storms = st.number_input("Storm Frequency", 0, 30, 5, 1)
            ah_roof = st.number_input("Roof Score", 0.0, 1.0, 0.5, 0.01)
            ah_structural = st.number_input("Structural Risk", 0.0, 1.0, 0.3, 0.01)
        with ah3:
            ah_insured = st.number_input("Insured Value ($)", 50000, 5000000, 500000, 50000)

        if st.button("Predict", type="primary", key="ml_adhoc_btn"):
            try:
                with st.spinner("Predicting..."):
                    adhoc_result = session.sql(f"""
                        SELECT {FQ('RISK_CLASSIFIER')}!PREDICT(INPUT_DATA => OBJECT_CONSTRUCT(
                            'HAZARD_SCORE', {ah_hazard}, 'DISTANCE_TO_COAST_KM', {ah_coast}, 'CRIME_INDEX', {ah_crime},
                            'STORM_FREQUENCY', {ah_storms}, 'ROOF_CONDITION_SCORE', {ah_roof},
                            'STRUCTURAL_RISK_SCORE', {ah_structural}, 'INSURED_VALUE', {ah_insured}
                        )) AS PREDICTION
                    """).to_pandas()
                if len(adhoc_result) > 0:
                    pred_json = json.loads(adhoc_result.iloc[0]["PREDICTION"])
                    pred_class = pred_json.get("class", "Unknown")
                    pred_probs = pred_json.get("probability", {})
                    risk_col = {"High": "red", "Medium": "orange", "Low": "green"}.get(pred_class, "gray")
                    rc1, rc2 = st.columns([1, 2])
                    with rc1:
                        st.badge(f"Predicted Risk: {pred_class}", icon=":material/analytics:", color=risk_col)
                        for cat in ["High", "Medium", "Low"]:
                            if cat in pred_probs:
                                st.caption(f"{cat}: {pred_probs[cat]:.1%}")
                    with rc2:
                        prob_df = pd.DataFrame([{"Category": k, "Probability": v} for k, v in pred_probs.items()])
                        if len(prob_df) > 0:
                            st.altair_chart(
                                alt.Chart(prob_df).mark_bar(color=ACCENT).encode(
                                    x=alt.X("Probability:Q", scale=alt.Scale(domain=[0, 1])),
                                    y=alt.Y("Category:N", sort="-x"),
                                ).properties(height=120),
                                use_container_width=True,
                            )
            except Exception:
                st.warning("Train the model first.")


# ════════════════════════════════════════════════════════════════════════════════
# TAB 6: IMAGE ANALYSIS — CORTEX AI VISION (PIXTRAL-LARGE & LLAMA)
# ════════════════════════════════════════════════════════════════════════════════
elif selected == "Image Analysis":
    STAGE_NAME = f"{DB}.{SCH}.IMAGE_ANALYSIS_STAGE"

    # ENSURE INTERNAL STAGE EXISTS
    @st.cache_resource
    def ensure_stage():
        session.sql(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()
        return True

    ensure_stage()

    # ─────────────────────────────────────────
    # HELPER: PARSE AI JSON RESPONSE
    # ─────────────────────────────────────────
    def parse_ai_json(raw):
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1].rsplit("```", 1)[0]
        return json.loads(cleaned)

    # ─────────────────────────────────────────
    # HELPER: RENDER AI ASSESSMENT KPI DASHBOARD
    # ─────────────────────────────────────────
    def render_kpi_dashboard(ai, image_source=None):
        cond = ai.get("overall_condition", "N/A")
        risk = ai.get("risk_category", "N/A")
        rec = ai.get("recommendation", "N/A")
        cond_clr = {"Excellent": "green", "Good": "green", "Fair": "orange", "Poor": "red"}.get(cond, "gray")
        risk_clr = {"High": "red", "Medium": "orange", "Low": "green"}.get(risk, "gray")
        rec_clr = "green" if rec == "Write" else "red"

        if image_source:
            img_col, kpi_col = st.columns([1, 2])
            with img_col:
                st.image(image_source, use_container_width=True)
        else:
            kpi_col = st.container()

        with kpi_col:
            # CONDITION / RISK / ACTION BADGES
            b1, b2, b3 = st.columns(3)
            b1.badge(f"Condition: {cond}", icon=":material/home:", color=cond_clr)
            b2.badge(f"Risk: {risk}", icon=":material/warning:", color=risk_clr)
            b3.badge(f"Action: {rec}", icon=":material/gavel:", color=rec_clr)

            # ROOF & STRUCTURAL METRICS
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Roof Score", f"{ai.get('roof_condition_score', 0):.2f}")
            m2.metric("Structural", f"{ai.get('structural_risk_score', 0):.2f}")
            m3.metric("Material", str(ai.get("roof_material", "N/A"))[:20])
            m4.metric("Roof Age", str(ai.get("estimated_roof_age", "N/A"))[:15])

            # ENVIRONMENTAL RISK METRICS
            m5, m6, m7, m8 = st.columns(4)
            m5.metric("Vegetation", ai.get("vegetation_risk", "N/A"))
            m6.metric("Flood", ai.get("flood_risk", "N/A"))
            m7.metric("Wildfire", ai.get("wildfire_risk", "N/A"))
            m8.metric("Maintenance", str(ai.get("maintenance_level", "N/A"))[:18])

        # RISK FACTORS LIST
        risk_factors = ai.get("risk_factors", [])
        if risk_factors:
            with st.container(border=True):
                st.markdown("**Risk Factors**")
                cols = st.columns(min(len(risk_factors), 3))
                for i, rf in enumerate(risk_factors):
                    cols[i % len(cols)].markdown(f"- {rf}")

        # RECOMMENDATION CALLOUT
        reason = ai.get("recommendation_reason", "")
        if reason:
            if rec == "Write":
                st.success(f"**Recommendation:** {reason}", icon=":material/check_circle:")
            else:
                st.error(f"**Recommendation:** {reason}", icon=":material/cancel:")

        # DETAILED ANALYSIS EXPANDERS
        with st.expander("Roof Analysis"):
            st.markdown(ai.get("detailed_roof_analysis", "N/A"))
        with st.expander("Structural Analysis"):
            st.markdown(ai.get("detailed_structural_analysis", "N/A"))
        with st.expander("Vegetation Analysis"):
            st.markdown(ai.get("detailed_vegetation_analysis", "N/A"))
        with st.expander("Exterior Analysis"):
            st.markdown(ai.get("detailed_exterior_analysis", "N/A"))

    # ─────────────────────────────────────────
    # PROMPT TEMPLATES FOR CORTEX VISION AI
    # ─────────────────────────────────────────

    # PROPERTY IMAGE ANALYSIS PROMPT
    STRUCTURED_PROMPT = """Analyze this property image as an insurance underwriter. Return your analysis STRICTLY as a valid JSON object with these exact keys (no markdown, no extra text, ONLY the raw JSON):
{
  "overall_condition": "Excellent" or "Good" or "Fair" or "Poor",
  "risk_category": "High" or "Medium" or "Low",
  "recommendation": "Write" or "Decline",
  "roof_condition_score": float 0.0 to 1.0,
  "structural_risk_score": float 0.0 to 1.0,
  "vegetation_risk": "Low" or "Medium" or "High",
  "roof_material": "string",
  "estimated_roof_age": "string like 5-10 years",
  "flood_risk": "Low" or "Medium" or "High",
  "wildfire_risk": "Low" or "Medium" or "High",
  "exterior_condition": "Excellent" or "Good" or "Fair" or "Poor",
  "maintenance_level": "Well-maintained" or "Average" or "Needs attention" or "Poor",
  "risk_factors": ["list of identified risk factors"],
  "recommendation_reason": "brief justification string",
  "detailed_roof_analysis": "paragraph string",
  "detailed_structural_analysis": "paragraph string",
  "detailed_vegetation_analysis": "paragraph string",
  "detailed_exterior_analysis": "paragraph string"
}"""

    # AERIAL/SATELLITE IMAGE ANALYSIS PROMPT
    AERIAL_STRUCTURED_PROMPT = """You are an expert insurance underwriter analyzing aerial/satellite imagery.
Analyze this aerial view and return your analysis STRICTLY as a valid JSON object with these exact keys (no markdown, no extra text, ONLY the raw JSON):
{
  "overall_condition": "Excellent" or "Good" or "Fair" or "Poor",
  "risk_category": "High" or "Medium" or "Low",
  "recommendation": "Write" or "Decline",
  "roof_condition_score": float 0.0 to 1.0,
  "structural_risk_score": float 0.0 to 1.0,
  "vegetation_risk": "Low" or "Medium" or "High",
  "roof_material": "string identified from aerial view",
  "estimated_roof_age": "string like 5-10 years",
  "flood_risk": "Low" or "Medium" or "High",
  "wildfire_risk": "Low" or "Medium" or "High",
  "exterior_condition": "Excellent" or "Good" or "Fair" or "Poor",
  "maintenance_level": "Well-maintained" or "Average" or "Needs attention" or "Poor",
  "risk_factors": ["list of identified risk factors from aerial view"],
  "recommendation_reason": "brief justification string",
  "detailed_roof_analysis": "paragraph about roof from aerial perspective",
  "detailed_structural_analysis": "paragraph about structure and neighborhood from aerial view",
  "detailed_vegetation_analysis": "paragraph about vegetation density, defensible space, wildfire risk",
  "detailed_exterior_analysis": "paragraph about surroundings, water proximity, land use, access roads"
}"""

    # ANALYSIS MODE SELECTOR
    analysis_mode = st.segmented_control("Analysis Mode", ["Property Upload", "Aerial Upload", "Portfolio Data"], default="Property Upload")

    # ─────────────────────────────────────────
    # MODE 1: PROPERTY IMAGE UPLOAD
    # ─────────────────────────────────────────
    if analysis_mode == "Property Upload":
        with st.container(border=True):
            st.markdown("##### Property Condition Assessment")
            st.caption("Upload a property image for AI-powered analysis via pixtral-large.")
            uploaded_file = st.file_uploader("Upload image", type=["jpg", "jpeg", "png", "bmp", "gif"], key="img_upload")
            if st.button("Analyze", type="primary", key="img_analyze_btn"):
                if not uploaded_file:
                    st.warning("Upload an image first.")
                else:
                    with st.spinner("Analyzing..."):
                        try:
                            file_name = uploaded_file.name.replace(" ", "_")
                            stage_path = f"@{STAGE_NAME}/uploads/{file_name}"
                            session.file.put_stream(uploaded_file, stage_path, auto_compress=False, overwrite=True)
                            result = session.sql(f"""
                                SELECT SNOWFLAKE.CORTEX.COMPLETE('pixtral-large', '{STRUCTURED_PROMPT.replace(chr(39), chr(39)+chr(39))}', TO_FILE('{stage_path}')) AS RESPONSE
                            """).collect()[0]["RESPONSE"]
                            try:
                                ai = parse_ai_json(result)
                                render_kpi_dashboard(ai, image_source=uploaded_file)
                            except (json.JSONDecodeError, KeyError):
                                st.image(uploaded_file, use_container_width=True)
                                st.markdown(result)
                        except Exception as e:
                            st.error(f"Analysis failed: {str(e)}")

    # ─────────────────────────────────────────
    # MODE 2: AERIAL/SATELLITE IMAGE UPLOAD
    # ─────────────────────────────────────────
    elif analysis_mode == "Aerial Upload":
        with st.container(border=True):
            st.markdown("##### Aerial / Satellite Risk Analysis")
            aerial_file = st.file_uploader("Upload aerial image", type=["jpg", "jpeg", "png", "bmp", "gif"], key="aerial_upload")
            aerial_context = st.text_area("Context (optional)", placeholder="e.g., Coastal Louisiana, insured $500K...", key="aerial_ctx")
            if st.button("Analyze Aerial", type="primary", key="aerial_analyze_btn"):
                if not aerial_file:
                    st.warning("Upload an aerial image first.")
                else:
                    with st.spinner("Analyzing..."):
                        try:
                            file_name = aerial_file.name.replace(" ", "_")
                            stage_path = f"@{STAGE_NAME}/aerial/{file_name}"
                            session.file.put_stream(aerial_file, stage_path, auto_compress=False, overwrite=True)
                            context_line = f"\nAdditional context: {aerial_context}" if aerial_context else ""
                            aerial_prompt_full = AERIAL_STRUCTURED_PROMPT + context_line
                            result = session.sql(f"""
                                SELECT SNOWFLAKE.CORTEX.COMPLETE('pixtral-large', '{aerial_prompt_full.replace(chr(39), chr(39)+chr(39))}', TO_FILE('{stage_path}')) AS RESPONSE
                            """).collect()[0]["RESPONSE"]
                            try:
                                ai = parse_ai_json(result)
                                render_kpi_dashboard(ai, image_source=aerial_file)
                                st.divider()
                                # STATE COMPARISON FOR AERIAL RESULTS
                                comp_state = st.selectbox("Compare with state", sorted(fdf["STATE"].unique()), key="aerial_comp")
                                if comp_state:
                                    sd = fdf[fdf["STATE"] == comp_state]
                                    ac1, ac2, ac3, ac4 = st.columns(4)
                                    ac1.metric(f"{comp_state} Avg Risk", f"{sd['RISK_SCORE'].mean():.4f}")
                                    ac2.metric(f"{comp_state} Avg Hazard", f"{sd['HAZARD_SCORE'].mean():.3f}")
                                    ac3.metric(f"{comp_state} Avg Premium", f"${sd['SUGGESTED_PREMIUM'].mean():,.0f}")
                                    ac4.metric(f"{comp_state} High Risk", f"{(sd['RISK_CATEGORY'] == 'High').mean() * 100:.1f}%")
                            except (json.JSONDecodeError, KeyError):
                                st.image(aerial_file, use_container_width=True)
                                st.markdown(result)
                        except Exception as e:
                            st.error(f"Analysis failed: {str(e)}")

    # ─────────────────────────────────────────
    # MODE 3: PORTFOLIO DATA ASSESSMENT (NO IMAGE NEEDED)
    # ─────────────────────────────────────────
    else:
        with st.container(border=True):
            st.markdown("##### Portfolio Aerial Insights")
            st.caption("AI assessment from existing portfolio data — no upload needed.")
            prop_id_img = st.selectbox("Property", fdf["PROPERTY_ID"].sort_values().unique(), key="img_prop_sel")
            if prop_id_img:
                row_img = fdf[fdf["PROPERTY_ID"] == prop_id_img].iloc[0]

                # PROPERTY SCORE SUMMARY
                pc1, pc2, pc3, pc4 = st.columns(4)
                pc1.metric("Roof", f"{row_img['ROOF_CONDITION_SCORE']:.2f}")
                pc2.metric("Structural", f"{row_img['STRUCTURAL_RISK_SCORE']:.2f}")
                pc3.metric("Vegetation", row_img["VEGETATION_DENSITY"])
                pc4.metric("Risk", row_img["RISK_CATEGORY"])

                # FULL PROPERTY DETAILS EXPANDER
                with st.expander("Full Details"):
                    d1, d2 = st.columns(2)
                    with d1:
                        st.markdown(f"**Address:** {row_img['ADDRESS']}, {row_img['CITY']}, {row_img['STATE']} {row_img['ZIP']}")
                        st.markdown(f"**Type:** {row_img['PROPERTY_TYPE']} · Built {int(row_img['YEAR_BUILT'])} · {row_img['CONSTRUCTION_TYPE']}")
                        st.markdown(f"**Premium:** ${row_img['SUGGESTED_PREMIUM']:,.2f} · Market ${row_img['AVG_MARKET_PREMIUM']:,.0f}")
                    with d2:
                        st.markdown(f"**Flood:** {row_img['FLOOD_ZONE']} · **Wildfire:** {row_img['WILDFIRE_RISK']}")
                        st.markdown(f"**Coast:** {row_img['DISTANCE_TO_COAST_KM']:.1f} km · **Crime:** {row_img['CRIME_INDEX']}")
                        st.markdown(f"**Storms:** {int(row_img['STORM_FREQUENCY'])}/yr · **Hurricanes:** {int(row_img['HURRICANE_EVENTS_LAST_10Y'])}")

                # GENERATE AI ASSESSMENT FROM PORTFOLIO DATA VIA LLAMA
                if st.button("Generate AI Assessment", type="primary", key="img_portfolio_btn"):
                    with st.spinner("Generating..."):
                        portfolio_prompt = f"""You are an expert P&C insurance underwriter. Based on the following data, return a JSON assessment.
Return STRICTLY a valid JSON object with these exact keys (no markdown, no extra text, ONLY the raw JSON):
{{
  "overall_condition": "Excellent" or "Good" or "Fair" or "Poor",
  "risk_category": "High" or "Medium" or "Low",
  "recommendation": "Write" or "Decline",
  "roof_condition_score": {row_img['ROOF_CONDITION_SCORE']:.2f},
  "structural_risk_score": {row_img['STRUCTURAL_RISK_SCORE']:.2f},
  "vegetation_risk": based on vegetation density "{row_img['VEGETATION_DENSITY']}",
  "roof_material": "{row_img['ROOF_MATERIAL']}",
  "estimated_roof_age": "{int(row_img['ROOF_AGE'])} years",
  "flood_risk": based on flood zone "{row_img['FLOOD_ZONE']}",
  "wildfire_risk": "{row_img['WILDFIRE_RISK']}",
  "exterior_condition": assess from data,
  "maintenance_level": assess from scores,
  "risk_factors": [list of key risk factors],
  "recommendation_reason": "justification based on data",
  "detailed_roof_analysis": "analysis of roof material {row_img['ROOF_MATERIAL']}, age {int(row_img['ROOF_AGE'])}yrs, condition {row_img['ROOF_CONDITION_SCORE']:.2f}",
  "detailed_structural_analysis": "analysis of structural risk {row_img['STRUCTURAL_RISK_SCORE']:.2f}, construction type {row_img['CONSTRUCTION_TYPE']}, built {int(row_img['YEAR_BUILT'])}",
  "detailed_vegetation_analysis": "analysis of vegetation density {row_img['VEGETATION_DENSITY']}, wildfire risk {row_img['WILDFIRE_RISK']}, hazard {row_img['HAZARD_SCORE']:.3f}",
  "detailed_exterior_analysis": "analysis of flood zone {row_img['FLOOD_ZONE']}, coast {row_img['DISTANCE_TO_COAST_KM']:.1f}km, storms {int(row_img['STORM_FREQUENCY'])}/yr, hurricanes {int(row_img['HURRICANE_EVENTS_LAST_10Y'])}"
}}

PROPERTY: {row_img['PROPERTY_ID']} at {row_img['ADDRESS']}, {row_img['CITY']}, {row_img['STATE']}
Type: {row_img['PROPERTY_TYPE']} | Insured: ${row_img['INSURED_VALUE']:,.0f} | Premium: ${row_img['SUGGESTED_PREMIUM']:,.2f} vs Market ${row_img['AVG_MARKET_PREMIUM']:,.0f}
Risk Score: {row_img['RISK_SCORE']:.4f} | Crime: {row_img['CRIME_INDEX']} | Rainfall: {row_img['AVG_ANNUAL_RAINFALL']:.1f}in | Temp: {row_img['AVG_TEMPERATURE']:.1f}F"""
                        try:
                            result = session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS RESPONSE", params=["llama3.1-70b", portfolio_prompt]).collect()[0]["RESPONSE"]
                            try:
                                ai = parse_ai_json(result)
                                render_kpi_dashboard(ai)
                            except (json.JSONDecodeError, KeyError):
                                st.markdown(result)
                        except Exception as e:
                            st.error(f"Assessment failed: {str(e)}")


# ════════════════════════════════════════════════════════════════════════════════
# TAB 7: AI ASSISTANT — CHAT INTERFACE WITH CORTEX LLM
# ════════════════════════════════════════════════════════════════════════════════
elif selected == "AI Assistant":
    st.markdown("##### P&C Underwriting Assistant")
    st.caption("Ask about properties, risk, premiums, claims, or strategy.")

    # SQL TOGGLE — OPTIONALLY INCLUDE SQL IN RESPONSES
    show_sql = st.toggle("Include SQL", value=False)

    # ─────────────────────────────────────────
    # IMAGE UPLOAD & ANALYSIS VIA CLAUDE
    # ─────────────────────────────────────────
    with st.expander("Upload & Analyze Image", expanded=False):
        st.caption("Upload a property image for AI-powered analysis (roof, structural, vegetation, etc.)")
        uploaded_file = st.file_uploader("Upload an image", type=["jpg", "jpeg", "png", "webp", "gif"], key="ai_img_upload")
        img_prompt = st.text_input("What would you like to know about this image?",
            value="Analyze this property image for insurance underwriting. Assess roof condition, structural integrity, vegetation risk, and overall property condition. Provide a JSON response.",
            key="img_prompt_input")

        if uploaded_file is not None:
            st.image(uploaded_file, caption=uploaded_file.name, use_container_width=True)

            if st.button("Analyze Image", type="primary", key="analyze_img_btn"):
                with st.spinner("Processing image with AI..."):
                    try:
                        file_bytes = uploaded_file.getvalue()
                        file_name = uploaded_file.name.replace(" ", "_")
                        stage_path = f"@HITL_SBA_DB.PC_INSURANCE.IMAGE_STAGE/{file_name}"

                        session.sql("CREATE STAGE IF NOT EXISTS HITL_SBA_DB.PC_INSURANCE.IMAGE_STAGE ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')").collect()

                        session.file.put_stream(
                            input_stream=io.BytesIO(file_bytes),
                            stage_location=stage_path,
                            auto_compress=False,
                            overwrite=True
                        )

                        result = session.sql(
                            "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?, TO_FILE(?, ?)) AS RESPONSE",
                            params=["claude-3-5-sonnet", img_prompt, "@HITL_SBA_DB.PC_INSURANCE.IMAGE_STAGE", file_name]
                        ).collect()[0]["RESPONSE"]

                        try:
                            ai = parse_ai_json(result)
                            render_kpi_dashboard(ai)
                        except (json.JSONDecodeError, KeyError, Exception):
                            st.markdown(result)

                    except Exception as e:
                        st.error(f"Image analysis failed: {str(e)}")

    # ─────────────────────────────────────────
    # BUILD SYSTEM PROMPT WITH FULL PORTFOLIO CONTEXT
    # ─────────────────────────────────────────
    fclaims = claims_df[claims_df["PROPERTY_ID"].isin(fdf["PROPERTY_ID"])]

    # AGGREGATE PORTFOLIO STATISTICS FOR LLM CONTEXT
    state_stats = fdf.groupby("STATE").agg(
        count=("PROPERTY_ID", "count"), avg_risk=("RISK_SCORE", "mean"),
        high_risk=("RISK_CATEGORY", lambda x: (x == "High").sum()),
        write_rate=("RECOMMENDED_ACTION", lambda x: (x == "Write").mean() * 100),
        avg_premium=("SUGGESTED_PREMIUM", "mean"), avg_market=("AVG_MARKET_PREMIUM", "mean"),
        avg_insured=("INSURED_VALUE", "mean"),
    ).round(2).to_string()

    claims_by_type = fclaims.groupby("CLAIM_TYPE").agg(count=("CLAIM_ID", "count"), avg_amount=("CLAIM_AMOUNT", "mean"), total=("CLAIM_AMOUNT", "sum")).round(0).to_string()
    claims_by_severity = fclaims["CLAIM_SEVERITY"].value_counts().to_string()
    geo_summary = fdf.groupby("FLOOD_ZONE").agg(count=("PROPERTY_ID", "count"), avg_risk=("RISK_SCORE", "mean"), decline_pct=("RECOMMENDED_ACTION", lambda x: (x == "Decline").mean() * 100)).round(2).to_string()
    wildfire_summary = fdf.groupby("WILDFIRE_RISK").agg(count=("PROPERTY_ID", "count"), avg_risk=("RISK_SCORE", "mean"), avg_hazard=("HAZARD_SCORE", "mean")).round(3).to_string()
    weather_by_state = fdf.groupby("STATE").agg(avg_storms=("STORM_FREQUENCY", "mean"), avg_hurricanes=("HURRICANE_EVENTS_LAST_10Y", "mean"), avg_rainfall=("AVG_ANNUAL_RAINFALL", "mean")).round(1).to_string()
    aerial_summary = fdf.groupby("ROOF_MATERIAL").agg(count=("PROPERTY_ID", "count"), avg_roof_score=("ROOF_CONDITION_SCORE", "mean"), avg_structural=("STRUCTURAL_RISK_SCORE", "mean"), avg_risk=("RISK_SCORE", "mean")).round(3).to_string()
    veg_summary = fdf.groupby("VEGETATION_DENSITY").agg(count=("PROPERTY_ID", "count"), avg_risk=("RISK_SCORE", "mean")).round(3).to_string()
    construction_stats = fdf.groupby("CONSTRUCTION_TYPE").agg(count=("PROPERTY_ID", "count"), avg_risk=("RISK_SCORE", "mean"), avg_premium=("SUGGESTED_PREMIUM", "mean"), write_rate=("RECOMMENDED_ACTION", lambda x: (x == "Write").mean() * 100)).round(2).to_string()
    top_risky = fdf.nlargest(10, "RISK_SCORE")[["PROPERTY_ID", "CITY", "STATE", "RISK_SCORE", "RISK_CATEGORY", "RECOMMENDED_ACTION", "SUGGESTED_PREMIUM", "FLOOD_ZONE", "WILDFIRE_RISK", "HAZARD_SCORE"]].to_string(index=False)
    premium_stats = fdf.groupby("RISK_CATEGORY").agg(avg_suggested=("SUGGESTED_PREMIUM", "mean"), avg_market=("AVG_MARKET_PREMIUM", "mean"), min_premium=("MIN_PREMIUM", "mean"), max_premium=("MAX_PREMIUM", "mean"), avg_insured=("INSURED_VALUE", "mean")).round(0).to_string()

    # TABLE SCHEMAS FOR SQL GENERATION MODE
    TABLE_SCHEMAS = """
TABLE SCHEMAS (use ONLY these exact column names in SQL):
HITL_SBA_DB.PC_INSURANCE.PROPERTY_MASTER: PROPERTY_ID, ADDRESS, CITY, STATE, ZIP, LATITUDE, LONGITUDE, PROPERTY_TYPE, YEAR_BUILT, ROOF_AGE, CONSTRUCTION_TYPE, INSURED_VALUE
HITL_SBA_DB.PC_INSURANCE.MODEL_OUTPUT: PROPERTY_ID, RISK_SCORE, RISK_CATEGORY, RECOMMENDED_ACTION, SUGGESTED_PREMIUM
HITL_SBA_DB.PC_INSURANCE.CLAIMS_HISTORY: CLAIM_ID, PROPERTY_ID, CLAIM_YEAR, CLAIM_AMOUNT, CLAIM_TYPE, CLAIM_SEVERITY
HITL_SBA_DB.PC_INSURANCE.RISK_GEOSPATIAL: PROPERTY_ID, FLOOD_ZONE, WILDFIRE_RISK, CRIME_INDEX, DISTANCE_TO_COAST_KM, HAZARD_SCORE
HITL_SBA_DB.PC_INSURANCE.WEATHER_HISTORY: PROPERTY_ID, AVG_ANNUAL_RAINFALL, STORM_FREQUENCY, AVG_TEMPERATURE, HURRICANE_EVENTS_LAST_10Y
HITL_SBA_DB.PC_INSURANCE.AERIAL_ANALYSIS: PROPERTY_ID, ROOF_CONDITION_SCORE, ROOF_MATERIAL, STRUCTURAL_RISK_SCORE, VEGETATION_DENSITY
HITL_SBA_DB.PC_INSURANCE.PREMIUM_BENCHMARK: PROPERTY_ID, AVG_MARKET_PREMIUM, MIN_PREMIUM, MAX_PREMIUM
SQL RULES: Always use aliases, qualify all columns in JOINs."""

    # CONDITIONAL SQL INSTRUCTION BASED ON TOGGLE
    sql_instruction = ""
    if show_sql:
        sql_instruction = f"""
{TABLE_SCHEMAS}
IMPORTANT: At the END of every response, include a Snowflake SQL validation query.
Format as:
**Validation Query:**
```sql
SELECT ...
```"""
    else:
        sql_instruction = "\nDo NOT include any SQL queries in your response."

    # ASSEMBLE FULL SYSTEM PROMPT WITH PORTFOLIO CONTEXT
    SYSTEM_PROMPT = f"""You are a Smart Business Advisor for P&C insurance underwriting.
All tables in HITL_SBA_DB.PC_INSURANCE, joined on PROPERTY_ID.

PORTFOLIO ({len(fdf):,} properties, {fdf['STATE'].nunique()} states):
Write rate: {(fdf['RECOMMENDED_ACTION']=='Write').mean()*100:.1f}% | Avg risk: {fdf['RISK_SCORE'].mean():.4f}
Avg premium: ${fdf['SUGGESTED_PREMIUM'].mean():,.0f} | Market: ${fdf['AVG_MARKET_PREMIUM'].mean():,.0f}
Total insured: ${fdf['INSURED_VALUE'].sum():,.0f}

STATE ANALYSIS: {state_stats}
CLAIMS ({len(fclaims):,}): By Type: {claims_by_type} By Severity: {claims_by_severity}
GEO: Flood: {geo_summary} Wildfire: {wildfire_summary}
WEATHER: {weather_by_state}
AERIAL: Roof: {aerial_summary} Vegetation: {veg_summary}
CONSTRUCTION: {construction_stats}
PREMIUM: {premium_stats}
TOP 10 RISKY: {top_risky}

Be precise, cite numbers.{sql_instruction}"""

    # ─────────────────────────────────────────
    # CHAT INTERFACE — MESSAGE HISTORY & INPUT
    # ─────────────────────────────────────────
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # RENDER EXISTING MESSAGES
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"], avatar=":material/smart_toy:" if msg["role"] == "assistant" else ":material/person:"):
            st.markdown(msg["content"])

    # HANDLE NEW USER INPUT
    if prompt := st.chat_input("Ask about risk, premiums, claims, strategy..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar=":material/person:"):
            st.markdown(prompt)

        # BUILD CONVERSATION HISTORY (LAST 10 MESSAGES)
        history = ""
        for msg in st.session_state.messages[-10:]:
            role = "User" if msg["role"] == "user" else "Assistant"
            history += f"{role}: {msg['content']}\n\n"

        full_prompt = f"""{SYSTEM_PROMPT}\n\nConversation:\n{history}\nAssistant:"""

        # CALL CORTEX COMPLETE (LLAMA 3.1 70B)
        with st.chat_message("assistant", avatar=":material/smart_toy:"):
            with st.spinner("Thinking..."):
                response = session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)", params=["llama3.1-70b", full_prompt]).collect()[0][0]
            st.markdown(response)
        st.session_state.messages.append({"role": "assistant", "content": response})


# ════════════════════════════════════════════════════════════════════════════════
# TAB 8: DATA EXPLORER — FULL PORTFOLIO TABLE VIEW
# ════════════════════════════════════════════════════════════════════════════════
elif selected == "Data Explorer":
    st.markdown("##### Portfolio Data")
    display_cols = ["PROPERTY_ID", "POLICY_NUMBER", "PROVIDER_NAME", "CITY", "STATE", "PROPERTY_TYPE", "INSURED_VALUE", "RISK_SCORE", "RISK_CATEGORY",
                    "RECOMMENDED_ACTION", "SUGGESTED_PREMIUM", "FLOOD_ZONE", "WILDFIRE_RISK", "HAZARD_SCORE",
                    "ROOF_CONDITION_SCORE", "AVG_MARKET_PREMIUM"]
    st.dataframe(
        fdf[display_cols].sort_values("RISK_SCORE", ascending=False), hide_index=True,
        column_config={
            "INSURED_VALUE": st.column_config.NumberColumn("Insured", format="$%d"),
            "SUGGESTED_PREMIUM": st.column_config.NumberColumn("Premium", format="$%.2f"),
            "AVG_MARKET_PREMIUM": st.column_config.NumberColumn("Market", format="$%.2f"),
            "RISK_SCORE": st.column_config.ProgressColumn("Risk", min_value=0, max_value=1, format="%.4f"),
            "HAZARD_SCORE": st.column_config.ProgressColumn("Hazard", min_value=0, max_value=1, format="%.3f"),
            "ROOF_CONDITION_SCORE": st.column_config.ProgressColumn("Roof", min_value=0, max_value=1, format="%.2f"),
        }, use_container_width=True,
    )
