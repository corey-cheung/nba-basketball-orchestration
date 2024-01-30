import os

import pendulum
from pendulum import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator


local_tz = pendulum.timezone("Australia/Brisbane")

nba_dag = DAG(
    dag_id="nba_basketball",
    start_date=datetime(2024, 1, 1, tz=local_tz),
    schedule="0 18 * * *",
    catchup=False,
)

home_dir = os.environ.get("HOME_DIR")
conda_setup = "source ~/miniconda3/etc/profile.d/conda.sh && "
ingest_setup = conda_setup + "conda activate nba-basketball-ingestion && "
ingest_setup += f"cd {home_dir}/dev/nba-basketball-ingestion"
dbt_setup = conda_setup + "conda activate nba-basketball-dbt && "
dbt_setup += f"cd {home_dir}/dev/nba-basketball-dbt"


update_games = BashOperator(
    task_id="update_games_from_api",
    bash_command=f"{ingest_setup} && src/ingest_postgres/update_nba_games.py",
    dag=nba_dag
)

update_box_score = BashOperator(
    task_id="update_box_score_from_api",
    bash_command=f"{ingest_setup} && src/ingest_postgres/update_nba_box_score.py",
    dag=nba_dag
)

test_postgres = BashOperator(
    task_id="test_postgres_tables",
    bash_command=f"{ingest_setup} && src/tests/test_postgres.py",
    dag=nba_dag
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

[update_games, update_box_score] >> test_postgres >> airbyte_sync_duckdb >> dbt_build
