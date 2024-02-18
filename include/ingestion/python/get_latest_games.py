#!/usr/bin/env python

"""
Get the latest games and load into a temporary CSV.
"""

import os
from datetime import datetime, timedelta
import requests


from nba_pg_ingestion_utils import query_postgres, write_to_csv


def get_start_and_end_dates(look_back) -> tuple[str, str]:
    """
    Return the start and end dates for the API query parameters.

    Parameters:
        look_back: A look back window in days of data to query, for a factor of safety
            or when a bigger backfill is needed.
    """
    print(f"look_back days: {look_back}")
    query = """ SELECT MAX(game_date) FROM nba_basketball.game WHERE status = 'Final';
    """
    max_game_date = query_postgres(query)[0]
    start_date = (max_game_date - timedelta(days=look_back)).strftime("%Y-%m-%d")
    end_date = datetime.now().date().strftime("%Y-%m-%d")

    return start_date, end_date


def format_games_data(
    game: dict[str, str | int | dict[str, str | int]]
) -> dict[str, str | int]:
    """
    Format each row of game data retrieved from the API.

    Parameters:
        game: A dictionary representing data about one NBA game.
    """
    formatted = {}
    formatted["game_id"] = game["id"]
    formatted["game_date"] = game["date"][:10]
    formatted["home_team_id"] = game["home_team"]["id"]
    formatted["home_team_score"] = game["home_team_score"]
    formatted["visitor_team_id"] = game["visitor_team"]["id"]
    formatted["visitor_team_score"] = game["visitor_team_score"]
    formatted["season"] = game["season"]
    formatted["post_season"] = game["postseason"]
    formatted["status"] = game["status"]

    return formatted


# pylint: disable=R0913, R1710, W0719
def get_games(
    api_key: str,
    url: str,
    per_page: int = 100,
    look_back: int = 2,
    cursor: int | None = None,
    truncate: bool = True,
    csv_header: bool = False,
) -> list[str]:
    """
    Query the data from the game endpoint recursively. Format the data and write it to a
    temporary csv file.

    Parameters:
        api_key: API key from balldontlie.io.
        url: The url of the games endpoint.
        per_page: The number of items to return in each page of the API response.
        look_back: Number of days to look back when querying the API.
        cursor: The key that points to the relevant pagination.
        truncate: To truncate the existing CSV.
        csv_header: To add the column names at the top of CSV.
    """
    start_date, end_date = get_start_and_end_dates(look_back)
    headers = {"Authorization": api_key}
    params = {
        "per_page": per_page,
        "cursor": cursor,
        "start_date": start_date,
        "end_date": end_date,
    }
    if csv_header:
        column_names = "'game_id','game_date','home_team_id','home_team_score',"
        column_names += (
            "'visitor_team_id','visitor_team_score','season','post_season','status'"
        )
    else:
        column_names = None

    response = requests.get(url, params=params, headers=headers, timeout=60)
    if response.status_code == 200:
        data = response.json()["data"]
        meta = response.json()["meta"]
        data = [format_games_data(i) for i in data]
        write_to_csv(
            path="temp_games.csv", data=data, truncate=truncate, header=column_names
        )

        if "next_cursor" not in meta:  # base case: last page
            return None

        cursor = meta["next_cursor"]  # loop to next page
        get_games(
            api_key=os.environ.get("BALLDONTLIE_API_KEY"),
            url="http://api.balldontlie.io/v1/games",
            look_back=2,
            cursor=cursor,
            truncate=False,  # Never truncate when looping to the next page
            csv_header=False,  # Don't add header again when looping to the next page
        )

    else:
        raise Exception(f"API request failed: {response.status_code}:{response.reason}")


if __name__ == "__main__":
    get_games(
        api_key=os.environ.get("BALLDONTLIE_API_KEY"),
        url="http://api.balldontlie.io/v1/games",
        look_back=2,
        csv_header=True,
        per_page=8,
    )
