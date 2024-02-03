#!/usr/bin/env python

import duckdb
import psycopg2
import os
from airflow.models import Variable

def query_duckdb(query: str) -> list[tuple]:
    """
    Query duck db and return the result in list of tuples.

    Parameters:
        query: The select query to run against postgres
    """
    duckdb_path = Variable.get("DUCKDB_PATH")
    with duckdb.connect(duckdb_path) as conn:
        cursor = conn.cursor()
        print("connected to duckdb!")
        cursor.execute(query)
        result = cursor.fetchall()

    return result

def query_postgres(query: str) -> list[tuple]:
    """
    Query postgres and return the result in list of tuples.

    Parameters:
        query: The select query to run against postgres
    """
    with psycopg2.connect(
            dbname="dev",
            user=os.environ.get("PG_USER"),
            password=os.environ.get("PG_PASSWORD"),
            host="127.0.0.1",  # localhost
            port="5432",
        ) as conn:
            cursor = conn.cursor()
            print("connected to postgres!")
            cursor.execute(query)
            result = cursor.fetchall()

    return result

def split_queries_to_list(path: str) -> list[str]:
    """
    For a given SQL file with multiple queries ending with a semicolon, return a list of
    each individual query.
    """
    with open(path,  encoding="UTF-8") as sql_file:
        queries = sql_file.read()
        query_list = queries.split(';')[:-1]
        query_list.sort() #  in place

    return query_list
