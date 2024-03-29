#!/usr/bin/env python

import os
import subprocess

import pendulum

from pendulum import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.models import Variable
from airflow.decorators import task
from task_utils import query_duckdb, query_postgres, split_queries_to_list


with DAG(
    dag_id="nba_basketball_elt",
    start_date=datetime(2024, 1, 1, tz=pendulum.timezone("Australia/Brisbane")),
    schedule="0 22 * * *",
    catchup=False,
):
    home_dir = os.environ.get("HOME_DIR")
    nba_elt_dir = Variable.get("NBA_ELT_DIR")
    conda_setup = "source ~/miniconda3/etc/profile.d/conda.sh && "

    teams = BashOperator(
        task_id="get_latest_teams",
        bash_command=(
            conda_setup
            + "conda activate nba-basketball-orchestration && "
            + f"cd {nba_elt_dir}/include/ingestion/python"
            + " && ./get_latest_teams.py"
        ),
        retries = 3,
        retry_delay = timedelta(minutes=2),
    )

    games = BashOperator(
        task_id="get_latest_games",
        bash_command=(
            conda_setup
            + "conda activate nba-basketball-orchestration && "
            + f"cd {nba_elt_dir}/include/ingestion/python"
            + " && ./get_latest_games.py"
        ),
        retries = 3,
        retry_delay = timedelta(minutes=2),
    )

    box_score = BashOperator(
        task_id="get_latest_box_scores",
        bash_command=(
            conda_setup
            + "conda activate nba-basketball-orchestration && "
            + f"cd {nba_elt_dir}/include/ingestion/python"
            + " && ./get_latest_box_scores.py"
        ),
        retries = 3,
        retry_delay = timedelta(minutes=2),
    )

    players = BashOperator(
        task_id="get_latest_players",
        bash_command=(
            conda_setup
            + "conda activate nba-basketball-orchestration && "
            + f"cd {nba_elt_dir}/include/ingestion/python"
            + " && ./get_latest_players.py"
        ),
        retries = 3,
        retry_delay = timedelta(minutes=2),
    )

    update_tables = BashOperator(
        task_id="update_postgres_tables",
        bash_command=(
            conda_setup
            + "conda activate nba-basketball-orchestration && "
            + f"cd {nba_elt_dir}/include/ingestion/python"
            + " && ./update_tables.py"
        ),
    )

    test_postgres = BashOperator(
        task_id="test_postgres_tables",
        bash_command=(
            conda_setup
            + "conda activate nba-basketball-orchestration && "
            + f"cd {nba_elt_dir} && include/tests/python/test_ingestion.py"
        ),
    )

    airbyte_sync_duckdb = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync_postgres_to_duckdb",
        airbyte_conn_id="airbyte",
        connection_id="64d7a580-40a5-4fb3-8151-0982f30e7abc",
        asynchronous=False,
        timeout=3600,
        wait_seconds=3,
    )

    dbt_build = BashOperator(
        task_id="run_dbt_build",
        bash_command=(
            conda_setup
            + "conda activate nba-basketball-dbt && "
            + f"cd {home_dir}/dev/nba-basketball-dbt && "
            + "dbt build"
        ),
    )

    @task
    def check_row_count():
        """
        Test that the row count for our raw tables (Postgres) is the same as the mart tables
        that are created by dbt (duckdb).
        """
        test_postgres_path = f"{nba_elt_dir}/include/tests/sql/test_postgres_rows.sql"
        test_duckdb_path = f"{nba_elt_dir}/include/tests/sql/test_duckdb_rows.sql"
        test_postgres_queries = split_queries_to_list(test_postgres_path)
        test_duckdb_queries = split_queries_to_list(test_duckdb_path)
        postgres_rows = [query_postgres(query) for query in test_postgres_queries]
        duckdb_rows = [query_duckdb(query) for query in test_duckdb_queries]
        print(postgres_rows)
        print(duckdb_rows)
        assert postgres_rows == duckdb_rows

        return postgres_rows == duckdb_rows

    @task
    def merge_latest_csvs():
        """
        Merge the latest csvs into origin master.
        """
        result = subprocess.run(
            f"""
        git -C {nba_elt_dir} stash &&
        git -C {nba_elt_dir} checkout master &&
        git -C {nba_elt_dir} pull origin master &&
        git -C {nba_elt_dir} stash pop &&
        git -C {nba_elt_dir} add include/ingestion/python/temp/1_temp_teams.csv \
        include/ingestion/python/temp/2_temp_players.csv \
        include/ingestion/python/temp/3_temp_games.csv \
        include/ingestion/python/temp/4_temp_box_scores.csv &&
        git -C {nba_elt_dir} commit -m 'upload latest CSVs' &&
        git -C {nba_elt_dir} push
        """,
            shell=True,
            capture_output=True,
            text=True,
        )
        print(result.stdout)

    streamlit_queries = BashOperator(
        task_id="run_streamlit_queries",
        bash_command=(
            conda_setup
            + "conda activate nba-basketball-analytics && "
            + f"cd {home_dir}/dev/nba-basketball-analytics && "
            + "src/run_queries.py"
        ),
    )

    @task
    def merge_streamlit_query_results():
        """
        Merge the new results of the streamlit queries into origin master.
        """
        nba_analytics_dir = f"{home_dir}/dev/nba-basketball-analytics"
        result = subprocess.run(
            f"""
        git -C {nba_analytics_dir} stash &&
        git -C {nba_analytics_dir} checkout master &&
        git -C {nba_analytics_dir} pull origin master &&
        git -C {nba_analytics_dir} stash pop &&
        git -C {nba_analytics_dir} add src/datasets/last_10_games.csv \
        src/datasets/latest_games.csv \
        src/datasets/season_stats.csv &&
        git -C {nba_analytics_dir} commit -m 'upload latest query results' &&
        git -C {nba_analytics_dir} push
        """,
            shell=True,
            capture_output=True,
            text=True,
        )
        print(result.stdout)

    (
        [teams, games]
        >> box_score
        >> players
        >> update_tables
        >> [test_postgres, airbyte_sync_duckdb]
        >> dbt_build
        >> [check_row_count(), merge_latest_csvs()]
        >> streamlit_queries
        >> merge_streamlit_query_results()
    )
