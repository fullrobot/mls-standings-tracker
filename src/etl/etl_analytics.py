from prefect import flow

from utils import get_db


@flow(name="ETL Analytics Flow", description="Create analytics tables for MLS standings")
def create_analytics_tables():
    db = get_db()
    db.execute("DROP TABLE IF EXISTS team_points")
    db.execute("DROP TABLE IF EXISTS cumulative_points")
    db.execute("DROP TABLE IF EXISTS stg_games")
    db.execute("CREATE TABLE IF NOT EXISTS stg_games AS SELECT * FROM 'data/games/**/*.parquet'")

    create_team_points_query = """create table if not exists team_points as  
    select
        season_name
        , matchday
        , home_team_name as team_name
        , home_team_score as score
        , home_team_points as points
        , away_team_name as opponent_name
        , away_team_score as opponent_score
    from stg_games
    union
    select
        season_name
        , matchday
        , away_team_name as team_name
        , away_team_score as score
        , away_team_points as points
        , home_team_name as opponent_name
        , home_team_score as opponent_score
    from stg_games"""
    db.execute(create_team_points_query)

    create_cumulative_points_query = """create table if not exists cumulative_points as 
    select
        season_name,
        matchday,
        row_number() over (
            partition by season_name, team_name
            order by matchday
        ) as game_number,
        team_name,
        score,
        opponent_name,
        opponent_score,
        points,
        SUM(points) OVER (
            PARTITION BY team_name, season_name
            ORDER BY matchday
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_points,
        SUM(score) OVER (
            PARTITION BY team_name, season_name
            ORDER BY matchday
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_goals
    from team_points
    order by team_name, season_name, game_number"""
    db.execute(create_cumulative_points_query)
    db.commit()
