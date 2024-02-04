#!/usr/bin/env python

import os

import pendulum

from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.models import Variable
from airflow.decorators import task
from task_utils import query_duckdb, query_postgres, split_queries_to_list


with DAG(
    dag_id="nba_basketball_elt",
    start_date=datetime(2024, 1, 1, tz=pendulum.timezone("Australia/Brisbane")),
    schedule="0 18 * * *",
    catchup=False,
):
    home_dir = os.environ.get("HOME_DIR")
    conda_setup = "source ~/miniconda3/etc/profile.d/conda.sh && "
    ingest_setup = conda_setup + "conda activate nba-basketball-ingestion && "
    ingest_setup += f"cd {home_dir}/dev/nba-basketball-ingestion"
    dbt_setup = conda_setup + "conda activate nba-basketball-dbt && "
    dbt_setup += f"cd {home_dir}/dev/nba-basketball-dbt"

    update_players = BashOperator(
        task_id="update_players_from_api",
        bash_command=f"{ingest_setup} && src/ingest_postgres/update_nba_players.py"
    )

    update_games = BashOperator(
        task_id="update_games_from_api",
        bash_command=f"{ingest_setup} && src/ingest_postgres/update_nba_games.py"
    )

    update_box_score = BashOperator(
        task_id="update_box_score_from_api",
        bash_command=f"{ingest_setup} && src/ingest_postgres/update_nba_box_score.py"
    )

    test_postgres = BashOperator(
        task_id="test_postgres_tables",
        bash_command=f"{ingest_setup} && src/tests/test_postgres.py"
    )

    airbyte_sync_duckdb = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync_postgres_to_duckdb",
        airbyte_conn_id="airbyte",
        connection_id="080b8185-e4a2-4c06-9fec-bb0f9e62f108",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    dbt_build = BashOperator(
        task_id="run_dbt_build",
        bash_command=f"{dbt_setup} && dbt build"
    )

    @task
    def check_row_count():
        """
        Test that the row count for our raw tables (Postgres) is the same as the mart tables
        that are created by dbt (duckdb).
        """
        nba_elt_dir = Variable.get("NBA_ELT_DIR")
        test_postgres_path = f"{nba_elt_dir}/include/sql/test_postgres_rows.sql"
        test_duckdb_path = f"{nba_elt_dir}/include/sql/test_duckdb_rows.sql"
        test_postgres_queries = split_queries_to_list(test_postgres_path)
        test_duckdb_queries = split_queries_to_list(test_duckdb_path)
        postgres_rows = [query_postgres(query) for query in test_postgres_queries]
        duckdb_rows = [query_duckdb(query) for query in test_duckdb_queries]
        print(postgres_rows)
        print(duckdb_rows)
        assert postgres_rows == duckdb_rows

        return postgres_rows == duckdb_rows

    update_players >> [update_games, update_box_score] >> test_postgres >> airbyte_sync_duckdb >> dbt_build >> check_row_count()
