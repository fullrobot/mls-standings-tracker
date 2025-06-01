import json

import duckdb
import pandas as pd
import streamlit as st

DATA_DIR = "data"
DB_URI = "db/mls.db"

st.title("MLS Standings Dashboard")
st.write("This dashboard displays the current standings of Major League Soccer (MLS) teams.")
st.write("Data is sourced from the MLS API and processed to show team standings based on game results.")


def get_db():
    return duckdb.connect(DB_URI, read_only=False)


@st.cache_data
def load_data():
    db = get_db()
    query = "SELECT * from cumulative_points"
    data = db.execute(query).df()
    return pd.DataFrame(data)


data_load_state = st.text("Loading data...")

data = load_data()

data_load_state.text("")


st.subheader("Current Standings")

st.selectbox(
    "Select a team to view its details:",
    options=data["team_name"].unique(),
    key="team_selection",
)

st.multiselect(
    "Select a season to view its details:",
    options=sorted(data["season_name"].unique(), reverse=True),
    key="season_selection",
)


st.line_chart(
    data[
        (data["team_name"] == st.session_state.get("team_selection"))
        & (data["season_name"].isin(st.session_state.get("season_selection", [])))
    ],
    use_container_width=True,
    height=500,
    x="game_number",
    y="cumulative_points",
    color="season_name",
    width=800,
)
