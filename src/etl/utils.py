import duckdb
from httpx import Client

from constants import DB_URI, MLS_API_URL
from schemas import Game, Team


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
