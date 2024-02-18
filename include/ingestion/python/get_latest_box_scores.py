#!/usr/bin/env python

"""
Get the latest box scores and load into a temporary CSV.
"""
import os
import requests
import duckdb
from ingestion_utils import handle_nulls, write_to_csv

def get_games_to_query() -> list[int]:
    """
    Get the game_id's to query to box score endpoint.
    """
    games = duckdb.sql(
        """
            SELECT
                DISTINCT game_id
            FROM read_csv('temp_games.csv', AUTO_DETECT=TRUE)
        """
    ).fetchall()
    games = [game[0] for game in games]
    return games


@handle_nulls
def format_box_score(
    data: dict[str, str | int | dict[str, str | int]]
) -> dict[str, str | int]:
    """
    Format each row of box score data retrieved from the API.

    Parameters:
        data: A dictionary representing box score data about one NBA game.
    """
    formatted = {}
    formatted["box_score_id"] = data["id"]

    if data["game"] is None:
        formatted["game_id"] = None
    else:
        formatted["game_id"] = data["game"]["id"]

    if data["player"] is None:
        formatted["player_id"] = None
    else:
        formatted["player_id"] = data["player"]["id"]

    if data["team"] is None:
        formatted["team_id"] = None
    else:
        formatted["team_id"] = data["team"]["id"]

    formatted["pts"] = data["pts"]
    formatted["reb"] = data["reb"]
    formatted["ast"] = data["ast"]
    formatted["blk"] = data["blk"]
    formatted["stl"] = data["stl"]
    formatted["turnover"] = data["turnover"]
    formatted["oreb"] = data["oreb"]
    formatted["dreb"] = data["dreb"]
    formatted["fg3_pct"] = data["fg3_pct"]
    formatted["fg3a"] = data["fg3a"]
    formatted["fg3m"] = data["fg3m"]
    formatted["fg_pct"] = data["fg_pct"]
    formatted["fga"] = data["fga"]
    formatted["fgm"] = data["fgm"]
    formatted["ft_pct"] = data["ft_pct"]
    formatted["fta"] = data["fta"]
    formatted["ftm"] = data["ftm"]
    formatted["min"] = data["min"]
    formatted["pf"] = data["pf"]

    return formatted


def get_box_scores(
        api_key: str,
        url: str,
        game_ids: list[int] | int = None,
        per_page: int = 100,
        cursor: int | None = None,
        truncate: bool = True,
        csv_header: bool = False,
):
    """
    Query the data from the box score endpoint recursively. Format the data and write it
    to a temporary csv file.
    """
    headers = {"Authorization": api_key}
    params = {
        "per_page": per_page,
        "cursor": cursor,
        "game_ids[]": game_ids
    }
    response = requests.get(url, headers=headers, params=params, timeout=60)
    if response.status_code == 200:
        data = response.json()["data"]
        meta = response.json()["meta"]
        print(len(data))
        print(meta)

    return

if __name__ == '__main__':
    get_box_scores(
        api_key=os.environ.get("BALLDONTLIE_API_KEY"),
        url="https://api.balldontlie.io/v1/stats",
        per_page=100,
        game_ids=[1038168,1038382],
    )
