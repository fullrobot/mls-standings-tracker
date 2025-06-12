from constants import DRAW, LOSE, WIN
from pandas import DataFrame
from prefect import flow, task
from utils import get_client, get_game_data, get_team_data


@task
def extract(year: str) -> dict:
    client = get_client()
    teams = get_team_data(client)

    # games = []

    # for year in range(2010, 2026):
    #     games.extend(
    #         get_game_data(
    #             client,
    #             params={
    #                 "season": str(year),
    #                 "stage_name": "Regular Season",
    #             },
    #         )
    #     )
    games = get_game_data(
        client,
        params={
            "season": year,
            "stage_name": "Regular Season",
        },
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
    df.to_parquet(
        "data/games",
        index=False,
        compression="snappy",
        partition_cols=["season_name", "matchday"],
    )
    print("Data loaded into the parquet file.")


@flow(name="ETL Raw Data Flow")
def etl_flow(year: str) -> None:
    """
    Main ETL flow for processing raw MLS data.
    Args:
        year (str): The year for which to process the data. Default is "2023".
    """
    client = get_client()
    teams = get_team_data(client)
    print(f"Starting ETL flow for year: {year}")
    games = get_game_data(
        client,
        params={
            "season": year,
            "stage_name": "Regular Season",
        },
    )
    data = {"teams": teams, "games": games}
    print(f"Extracted data for year: {year}")
    print(f"Number of teams: {len(teams)}")
    print(f"Number of games: {len(games)}")
    print("Transforming data...")
    transformed_df = transform(data)
    print("Data transformed successfully.")
    print("Loading data into parquet file...")
    load(transformed_df)
    print("ETL flow completed for year:", year)
