from httpx import Client
from prefect import flow, task
import duckdb
from pandas import DataFrame
import json

from schemas import Team, Game

DATA_DIR = "data"
DB_URI = "db/mls.db"
MLS_API_URL = "https://app.americansocceranalysis.com/api/"

# POINTS
WIN = 3
DRAW = 1
LOSE = 0


def get_db():
    return duckdb.connect(DB_URI, read_only=False)


def get_client():
    return Client(base_url=MLS_API_URL, timeout=10.0)


def get_team_data(client: Client) -> list[dict]:
    response = client.get("v1/mls/teams")
    response.raise_for_status()
    teams_data = response.json()
    return [Team(**team).model_dump() for team in teams_data]


def get_game_data(client: Client) -> list[dict]:
    response = client.get("v1/mls/games")
    response.raise_for_status()
    games_data = response.json()
    return [Game(**game).model_dump() for game in games_data]


@task
def extract():
    client = get_client()
    teams = get_team_data(client)
    games = get_game_data(client)
    with open(f"{DATA_DIR}/raw_teams.json", "w") as f:
        json.dump(obj=teams, fp=f, indent=2)
    with open(f"{DATA_DIR}/raw_games.json", "w") as f:
        json.dump(obj=games, fp=f, indent=2)
    return {"teams": teams, "games": games}


@task
def transform(data):
    teams_df = DataFrame(data["teams"])
    games_df = DataFrame(data["games"])

    # NOTE: filter out games that are not full-time
    merged_df = games_df.merge(
        teams_df,
        left_on="home_team_id",
        right_on="id",
        suffixes=("", "_home"),
    ).merge(
        teams_df,
        left_on="away_team_id",
        right_on="id",
        suffixes=("", "_away"),
    )
    merged_df = merged_df[merged_df["status"] == "FullTime"]
    merged_df["home_team_points"] = merged_df.apply(
        lambda row: WIN
        if row["home_score"] > row["away_score"]
        else (DRAW if row["home_score"] == row["away_score"] else LOSE),
        axis=1,
    )
    merged_df["away_team_points"] = merged_df.apply(
        lambda row: WIN
        if row["away_score"] > row["home_score"]
        else (DRAW if row["away_score"] == row["home_score"] else LOSE),
        axis=1,
    )

    merged_df = merged_df.rename(
        columns={
            "id": "game_id",
            "date_time_utc": "date_time_utc",
            "name": "home_team_name",
            "home_score": "home_team_score",
            "home_team_points": "home_team_points",
            "name_away": "away_team_name",
            "away_score": "away_team_score",
            "away_team_points": "away_team_points",
        }
    )
    merged_df = merged_df[
        [
            "game_id",
            "date_time_utc",
            "season_name",
            "matchday",
            "home_team_name",
            "home_team_score",
            "home_team_points",
            "away_team_name",
            "away_team_score",
            "away_team_points",
        ]
    ]

    with open(f"{DATA_DIR}/transformed_games.json", "w") as f:
        json.dump(obj=merged_df.to_dict(orient="records"), fp=f, indent=2)

    return merged_df.to_json(orient="records", indent=2)


@task
def load(data):
    db = get_db()
    db.execute("CREATE TABLE IF NOT EXISTS games AS SELECT * FROM read_json_auto(?)", (data,))
    db.commit()
    print("Data loaded into the database.")


@flow(name="ETL Flow")
def etl_flow():
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)


if __name__ == "__main__":
    etl_flow()
