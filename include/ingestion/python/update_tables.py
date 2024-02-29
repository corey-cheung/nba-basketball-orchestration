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
    with open(f"./temp/{file}", "r", encoding="UTF-8") as table:
        table = csv.reader(table)
        columns = next(table)
        values_str = ""

        for line in table:
            values = ",".join(line)
            values_str += f"({values}),\n"
        values_str = values_str[:-2]
        return columns, values_str


# pylint: disable=C0200
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
    Loop through all the CSVs in the directory and upsert the relevant tables.
    """
    temp_table_files = [file for file in os.listdir("./temp") if file.endswith(".csv")]
    temp_table_files.sort()

    for file in temp_table_files:
        print(f"Updating with {file}")
        columns, values = get_columns_and_values(file)
        primary_key = columns[0]
        table = primary_key.replace("_id", "")
        query = create_upsert_query(
            table=table, columns=columns, values=values, primary_key=primary_key
        )
        generate_db_objects(query)


if __name__ == "__main__":
    main()
