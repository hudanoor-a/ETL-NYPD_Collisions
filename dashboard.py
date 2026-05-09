import streamlit as st
import pandas as pd
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go

# ----------------------------
# CONFIG
# ----------------------------
st.set_page_config(
    page_title="NYC Collision Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Paths
OUTPUT_DIR = Path("m3_outputs")
CHART_DIR = OUTPUT_DIR / "charts"
CSV_DIR = OUTPUT_DIR / "csv"

# Colors
BG_COLOR = "#000000"
ACCENT_COLOR = "#aa0144"
TEXT_COLOR = "#ffffff"

# ----------------------------
# SIDEBAR
# ----------------------------
st.sidebar.title("NYC Collision Analytics")
vis_option = st.sidebar.selectbox(
    "Choose Visualization",
    [
        "Q1: Borough Fatality Rates",
        "Q2: Pedestrian Injuries Heatmap",
        "Q3: Vehicle Fatal vs Non-Fatal",
        "Q4: Cyclist Injury Trends",
        "Q5: Multi-Vehicle 3+ Injury Factors",
        "Geographic Crash Density"
    ]
)

# ----------------------------
# STYLE
# ----------------------------
st.markdown(f"""
    <style>
    .stApp, .css-1d391kg {{
        background-color: {BG_COLOR};
        color: {TEXT_COLOR};
    }}
    .stButton>button {{
        background-color: {ACCENT_COLOR};
        color: {TEXT_COLOR};
    }}
    .stSidebar {{
        background-color: {BG_COLOR};
        color: {TEXT_COLOR};
    }}
    </style>
""", unsafe_allow_html=True)

# ----------------------------
# HELPER FUNCTIONS
# ----------------------------
def load_csv(name):
    path = CSV_DIR / name
    if path.exists():
        return pd.read_csv(path)
    else:
        st.warning(f"CSV {name} not found!")
        return pd.DataFrame()

def plotly_bar(df, x_col, y_col, title, highlight=None):
    fig = px.bar(df, x=x_col, y=y_col, color_discrete_sequence=[ACCENT_COLOR])
    if highlight is not None and highlight in df[x_col].values:
        fig.add_trace(go.Bar(
            x=[highlight],
            y=df.loc[df[x_col] == highlight, y_col],
            marker_color="cyan",
            name="Selected"
        ))
    fig.update_layout(
        plot_bgcolor=BG_COLOR,
        paper_bgcolor=BG_COLOR,
        font_color=TEXT_COLOR,
        title={"text": title, "x":0.5},
    )
    fig.update_xaxes(tickangle=90)
    return fig

def plotly_line(df, x_col, y_col, title, highlight_x=None):
    fig = px.line(df, x=x_col, y=y_col, markers=True, line_shape="linear")
    fig.update_traces(line_color=ACCENT_COLOR)
    if highlight_x is not None and highlight_x in df[x_col].values:
        fig.add_trace(go.Scatter(
            x=[highlight_x],
            y=df.loc[df[x_col] == highlight_x, y_col],
            mode='markers',
            marker=dict(color='cyan', size=12),
            name='Selected'
        ))
    fig.update_layout(
        plot_bgcolor=BG_COLOR,
        paper_bgcolor=BG_COLOR,
        font_color=TEXT_COLOR,
        title={"text": title, "x":0.5}
    )
    return fig

# ----------------------------
# MAIN LOGIC
# ----------------------------
st.title("NYC Collision Dashboard")
st.markdown(f"<h4 style='color:{ACCENT_COLOR}'>Interactive Dashboard</h4>", unsafe_allow_html=True)

if vis_option == "Q1: Borough Fatality Rates":
    df = load_csv("q1_borough_fatality_rates.csv")
    highlight = st.selectbox("Select Borough-Year to Highlight:", df["borough"].astype(str) + "-" + df["year"].astype(str))
    df["borough_year"] = df["borough"].astype(str) + "-" + df["year"].astype(str)
    st.plotly_chart(plotly_bar(df, "borough_year", "fatality_rate_per_1000_crashes",
                               "Fatality Rate per 1000 Crashes", highlight=highlight))

elif vis_option == "Q2: Pedestrian Injuries Heatmap":
    df = load_csv("q2_hour_day_heatmap.csv")
    fig = px.imshow(df.pivot(index="day_of_week", columns="hour", values="total_pedestrian_injuries"),
                    color_continuous_scale="inferno")
    fig.update_layout(
        plot_bgcolor=BG_COLOR, paper_bgcolor=BG_COLOR, font_color=TEXT_COLOR,
        title={"text": "Pedestrian Injuries Heatmap", "x":0.5}
    )
    st.plotly_chart(fig)

elif vis_option == "Q3: Vehicle Fatal vs Non-Fatal":
    df = load_csv("q3_vehicle_fatal_nonfatal_by_borough.csv")
    borough = st.selectbox("Select Borough to Highlight:", df["borough"].unique())
    df_highlight = df[df["borough"] == borough]
    st.plotly_chart(plotly_bar(df, "vehicle_type_1", "crash_count",
                               "Vehicle Type vs Crash Severity", highlight=None))

elif vis_option == "Q4: Cyclist Injury Trends":
    df = load_csv("q4_cyclist_injury_trends_monthly.csv")
    # Only show yearly ticks to reduce clutter
    df["year_only"] = df["year"].astype(str)
    year = st.selectbox("Select Year to Highlight:", df["year_only"].unique())
    st.plotly_chart(plotly_line(df.groupby("year_only").sum().reset_index(), "year_only", "total_cyclist_injuries",
                                "Cyclist Injury Trends by Year", highlight_x=year))

elif vis_option == "Q5: Multi-Vehicle 3+ Injury Factors":
    df = load_csv("q5_multi_vehicle_3plus_injuries_factors.csv")
    factor = st.selectbox("Select Factor to Highlight:", df["contributing_factor"].unique())
    st.plotly_chart(plotly_bar(df, "contributing_factor", "crash_count",
                               "Top Factors in Multi-Vehicle Crashes", highlight=factor))

elif vis_option == "Geographic Crash Density":
    df = load_csv("bonus_geo_density_points.csv")
    st.map(df.rename(columns={"latitude":"lat","longitude":"lon"}))