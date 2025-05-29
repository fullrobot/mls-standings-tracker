import streamlit as st
import pandas as pd
import json

DATA_DIR = "data"

st.title("MLS Standings Dashboard")
st.write("This dashboard displays the current standings of Major League Soccer (MLS) teams.")
st.write("Data is sourced from the MLS API and processed to show team standings based on game results.")


@st.cache_data
def load_data():
    with open(DATA_DIR + "/cumulative_stats.json", "r") as f:
        data = json.load(f)
    data = pd.DataFrame(data)
    return data


data_load_state = st.text("Loading data...")

data = load_data()


st.subheader("Current Standings")

st.selectbox(
    "Select a team to view its details:",
    options=data["team_name"].unique(),
    key="team_selection",
)


st.line_chart(
    data[(data["team_name"] == st.session_state.get("team_selection"))],
    use_container_width=True,
    height=500,
    x="matchday",
    y="cumulative_points",
    color="season_name",
    width=800,
)
