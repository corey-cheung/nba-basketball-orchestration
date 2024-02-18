"""
Utility functions to be used in the ingestion of data from different "Ball Don't Lie"
endpoints to postgres.
"""

import os

import psycopg2


def get_row_to_insert(data: dict[str, str | int]) -> str:
    """
    Break down the json results from API and return a string
    to be used in the SQL insert statement.

    Parameters:
        data: A dictionary that represents one row of the table we want to insert into.
    """
    values = [
        (
            str(i)
            if (  # don't put numbers, floats, booleans, or nulls in quotes
                str(i).isnumeric()
                or str(i).lower() in ("true", "false")
                or str(i) == "NULL"
            )
            else "'" + str(i) + "'"
        )
        for i in data.values()
    ]  # non-integers and non-bools will need a literal "'" in the insert DML
    row = ",".join(values)
    return row


def write_to_csv(
    path: str, data: list[dict[str, str | int]], truncate: bool, header: str = None
):
    """
    Format data received from the ball don't lie API and write it to a CSV.

    Parameters:
        path: The path of the csv file.
        data: A list of dictionaries, each dictionary is a row to insert into the csv.
        truncate: Should the csv file be truncated before inserting.
    """
    # Check if CSV exists
    csv_exists = os.path.isfile(path)
    if truncate and csv_exists:
        os.remove(path)
    with open(path, "a", encoding="UTF-8") as csv:
        if header:
            csv.write(f"{header}\n")
        for row in data:
            row_to_insert = get_row_to_insert(row)
            csv.write(f"{row_to_insert}\n")


def generate_db_objects(query: str) -> None:
    """
    Create postgres database objects from the provided DDL and DML.

    Parameters:
        query: The query to be executed against postgres.
    """

    try:
        conn = psycopg2.connect(
            dbname="dev",
            user=os.environ.get("PG_USER"),
            password=os.environ.get("PG_PASSWORD"),
            host="127.0.0.1",  # localhost
            port="5432",
        )
        cursor = conn.cursor()
        print("connected to postgres!")

        cursor.execute(query)
        conn.commit()

    finally:
        if conn is None:
            conn.close()


def query_postgres(query: str, fetchall: bool = False) -> list[tuple]:
    """
    Query postgres and return a single row or all the rows in a list of tuples.

    Parameters:
        query: The select query to run against postgres
        fetchall: Should the query return all rows, if false one row will be returned
    """
    try:
        conn = psycopg2.connect(
            dbname="dev",
            user=os.environ.get("PG_USER"),
            password=os.environ.get("PG_PASSWORD"),
            host="127.0.0.1",  # localhost
            port="5432",
        )
        cursor = conn.cursor()
        print("connected to postgres!")

        cursor.execute(query)
        if fetchall:
            result = cursor.fetchall()
        else:
            result = list(cursor.fetchone())

        return result

    finally:
        if conn is None:
            conn.close()


def handle_nulls(func):
    """
    Wrapper function to handle nulls when cleaning the API response. To be used as a
    decorator in the 'format data' functions.
    """

    def wrapper(*args, **kwargs):
        data = func(*args, **kwargs)
        data = {k: ("NULL" if v is None or v == "" else v) for (k, v) in data.items()}
        return data

    return wrapper


def handle_apostrophes(func):
    """
    Wrapper function to handle apostrophes when cleaning the API response. To be used as
    a decorator in the 'format data' functions.
    """

    def wrapper(*args, **kwargs):
        data = func(*args, **kwargs)
        data = {
            k: (str(v).replace("'", "''") if "'" in str(v) else v)
            for (k, v) in data.items()
        }
        return data

    return wrapper
