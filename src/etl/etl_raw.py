from pandas import DataFrame
from prefect import flow, task

from constants import DRAW, LOSE, WIN
from utils import get_client, get_db, get_game_data, get_team_data


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
    # NOTE: filter out games that are not full-time
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
