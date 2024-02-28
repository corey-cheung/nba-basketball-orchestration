#!/usr/bin/env python

"""
Update the NBA teams, games, players and box score tables from the temporary CSVs.
"""

import os
import csv
from ingestion_utils import generate_db_objects


def get_columns_and_values(file: str) -> tuple[list[str], str]:
    """
    Read in a CSV file and return the columns and a string containing formatted values
    to be used in a SQL upsert statement.

    Parameters:
        file: The file name of the CSV.
    """
    with open(file,"r") as table:
        table = csv.reader(table)
        columns = next(table)

        values_str = ""
        for line in table:
        #     values = [
        #     (
        #         str(i)
        #         if (  # don't put numbers, floats, booleans, or nulls in quotes
        #             str(i).isnumeric()
        #             or str(i).lower() in ("true", "false")
        #             or str(i) == "NULL"
        #         )
        #         else "'" + str(i) + "'"
        #     )
        #     for i in line
        # ]  # non-integers and non-bools will need a literal "'" in the insert DML
            values = ",".join(line)
            values_str += f"({values}),\n"
        values_str = values_str[:-2]
        return columns, values_str


def create_upsert_query(
    table: str,
    columns: list[str],
    primary_key: str,
    values: str,
) -> str:
    """
    Create an upsert query to be run against Postgres.

    Parameters:
        table: The table to update.
        columns: The columns of the table.
        primary_key: The primary key of the table, used for the upsert.
        values: The values to upsert.
    """

    query = f"""INSERT INTO nba_basketball.{table}
({",".join(columns)})
VALUES
{values}
ON CONFLICT ({primary_key}) DO UPDATE SET
"""

    for i in range(len(columns)):
        if i != len(columns) - 1:
            query += f"{columns[i]} = EXCLUDED.{columns[i]},\n"
        else:
            query += f"{columns[i]} = EXCLUDED.{columns[i]}\n;"

    return query


def main():
    """

    """
    temp_table_files = [file for file in os.listdir(".") if file.endswith(".csv")]
    file = temp_table_files[0]

    columns, values = get_columns_and_values(file)
    query = create_upsert_query(
        table="team", columns=columns, values=values, primary_key="team_id"
    )
    print(query)
    generate_db_objects(query)

if __name__ == "__main__":
    main()
