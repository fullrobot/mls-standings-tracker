from pydantic import BaseModel, Field


class Team(BaseModel):
    id: str = Field(alias="team_id", description="Unique identifier for the team")
    name: str = Field(alias="team_name", description="Name of the team")
    abbreviation: str = Field(alias="team_abbreviation", description="Abbreviation of the team name")


class Game(BaseModel):
    id: str = Field(alias="game_id", description="Unique identifier for the game")
    date_time_utc: str = Field("date_time_utc", description="Date and time of the game in UTC")
    home_score: int = Field("home_score", description="Score of the home team")
    away_score: int = Field("away_score", description="Score of the away team")
    home_team_id: str = Field("home_team_id", description="Home team details")
    away_team_id: str = Field("away_team_id", description="Away team details")
    season_name: str = Field("season_name", description="Name of the season during which the game was played")
    matchday: int = Field("matchday", description="Matchday number in the season")
    status: str = Field("status", description="Current status of the game (e.g., FullTime)")
