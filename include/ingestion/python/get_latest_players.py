#!/usr/bin/env python

"""
Get the players from the latest games and load into a temporary CSV.
"""
import os
import requests
import duckdb

from ingestion_utils import write_to_csv, handle_apostrophes, handle_nulls


def get_player_ids() -> list[int]:
    """
    Get the player_id's from the temporary box_score csv, to query the players endpoint.
    """
    players = duckdb.sql(
        """
            SELECT
                DISTINCT player_id
            FROM read_csv('temp/4_temp_box_scores.csv', AUTO_DETECT=TRUE)
        """
    ).fetchall()
    players = [player[0] for player in players]

    return players


@handle_nulls
@handle_apostrophes
def format_player_data(
    player: dict[str, str | int | dict[str, str | int]]
) -> dict[str, str | int]:
    """
    Format each row of player data retrieved from the API.

    Parameters:
        player: A dictionary representing data about an NBA player.
    """
    formatted = {}
    formatted["player_id"] = player["id"]
    formatted["position"] = player["position"]
    formatted["team_id"] = player["team"]["id"]
    formatted["first_name"] = player["first_name"]
    formatted["last_name"] = player["last_name"]
    formatted["height_feet"] = player["height"].split("-")[0]
    formatted["height_inches"] = player["height"].split("-")[1]
    formatted["weight_pounds"] = player["weight"]

    return formatted


# pylint: disable=R0913, R1710, W0719
def get_players(
    api_key: str,
    url: str,
    player_ids: list[int] | int = None,
    per_page: int = 100,
    cursor: int | None = None,
    truncate: bool = True,
    csv_header: bool = False,
) -> None:
    """
    Query the player endpoint recursively. Format the data and write it to a
    temporary csv file.

    Parameters:
        api_key: API key from balldontlie.io.
        url: The url of the players endpoint.
        per_page: The number of items to return in each page of the API response.
        cursor: The key that points to the relevant pagination.
        truncate: To truncate the existing CSV.
        csv_header: To add the column names at the top of CSV.
    """
    headers = {"Authorization": api_key}
    params = {"per_page": per_page, "cursor": cursor, "player_ids[]": player_ids}
    if csv_header:
        column_names = "player_id,position,team_id,first_name,last_name,height_feet,"
        column_names += "height_inches,weight_pounds"
    else:
        column_names = None

    response = requests.get(url=url, headers=headers, params=params, timeout=60)
    if response.status_code == 200:
        data = response.json()["data"]
        meta = response.json()["meta"]
        data = [format_player_data(i) for i in data]
        write_to_csv(
            path="temp/2_temp_players.csv",
            data=data,
            truncate=truncate,
            header=column_names,
        )

        if "next_cursor" not in meta:  # base case: last page
            return None

        cursor = meta["next_cursor"]  # loop to next page
        print(f"looping to next cursor: {cursor}")
        get_players(
            api_key=api_key,
            url=url,
            player_ids=get_player_ids(),
            per_page=100,
            cursor=cursor,
            truncate=False,  # Never truncate when looping to the next page
            csv_header=False,  # Don't add header again when looping to the next page
        )

    else:
        raise Exception(f"API request failed: {response.status_code}:{response.reason}")


if __name__ == "__main__":
    get_players(
        api_key=os.environ.get("BALLDONTLIE_API_KEY"),
        url="https://api.balldontlie.io/v1/players",
        player_ids=get_player_ids(),
        per_page=100,
        csv_header=True,
    )
