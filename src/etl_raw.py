import json

import duckdb
from httpx import Client
from pandas import DataFrame
from prefect import flow, task

from schemas import Game, Team

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


def get_team_data(client: Client, params: dict | None = None) -> list[dict]:
    if params is None:
        params = {}
    response = client.get("v1/mls/teams", params=params)
    response.raise_for_status()
    teams_data = response.json()
    return [Team(**team).model_dump() for team in teams_data]


def get_game_data(client: Client, params: dict | None = None) -> list[dict]:
    if params is None:
        params = {}
    response = client.get("v1/mls/games", params=params)
    response.raise_for_status()
    games_data = response.json()
    return [Game(**game).model_dump() for game in games_data]


@task
def extract():
    client = get_client()
    teams = get_team_data(client)

    games = []

    for year in range(2010, 2026):
        games.extend(
            get_game_data(
                client,
                params={
                    "season": str(year),
                    "stage_name": "Regular Season",
                },
            )
        )
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
            "knockout_game",
        ]
    ]
    return merged_df


@task
def load(df: DataFrame) -> bool:
    db = get_db()
    db.sql("DROP TABLE IF EXISTS stg_games")
    db.sql("CREATE TABLE stg_games AS SELECT * FROM df")
    db.commit()
    print("Data loaded into the database.")
    db.close()
    return True


@flow(name="ETL Raw Data Flow")
def etl_flow():
    data = extract()
    transformed_df = transform(data)
    load(transformed_df)


if __name__ == "__main__":
    etl_flow()
    print("ETL process completed successfully.")
