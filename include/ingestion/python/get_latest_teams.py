#!/usr/bin/env python

"""
Get the latest teams and load into a temporary CSV.
"""

import os
import requests
from ingestion_utils import write_to_csv, handle_nulls


@handle_nulls
def format_team_data(
    team: dict[str, str | int | dict[str, str | int]]
) -> dict[str, str | int]:
    """
    Format each row of team data retrieved from the API.

    Parameters:
        team: A dictionary representing data about an NBA team.
    """
    formatted = {}
    formatted["team_id"] = team["id"]
    formatted["team_name_abbreviation"] = team["abbreviation"]
    formatted["city"] = team["city"]
    formatted["conference"] = team["conference"]
    formatted["division"] = team["division"]
    formatted["team_full_name"] = team["full_name"]
    formatted["team_name"] = team["name"]

    return formatted


# pylint: disable=W0719
def get_teams(api_key: str, url: str) -> None:
    """
    Query the team endpoint to get all teams. Format the data and write it to a
    temporary csv file.

    Parameters:
        api_key: API key from balldontlie.io.
        url: The url of the players endpoint.
    """
    headers = {"Authorization": api_key}
    params = {"per_page": 100}
    response = requests.get(url=url, headers=headers, params=params, timeout=60)
    column_names = "team_id,team_name_abbreviation,city,conference,division,"
    column_names += "team_full_name,team_name"

    if response.status_code == 200:
        data = response.json()["data"]
        data = [format_team_data(team) for team in data]
        write_to_csv(
            path="temp/1_temp_teams.csv",
            data=data,
            truncate=True,
            header=column_names,
        )

    else:
        raise Exception(f"API request failed: {response.status_code}:{response.reason}")


if __name__ == "__main__":
    get_teams(
        api_key=os.environ.get("BALLDONTLIE_API_KEY"),
        url="http://api.balldontlie.io/v1/teams",
    )
