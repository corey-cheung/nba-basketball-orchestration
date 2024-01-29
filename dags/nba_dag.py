import os

import pendulum
from pendulum import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

local_tz = pendulum.timezone("Australia/Brisbane")

nba_dag = DAG(
    dag_id="nba_basketball",
    start_date=datetime(2024, 1, 1, tz=local_tz),
    schedule="0 18 * * *",
    catchup=False,
)
home_dir = os.environ.get("HOME_DIR")
ingest_setup = "source ~/miniconda3/etc/profile.d/conda.sh && "
ingest_setup += "conda activate nba-basketball-ingestion && "
ingest_setup += f"cd {home_dir}/dev/nba-basketball-ingestion"


update_games = BashOperator(
    task_id='update_games',
    bash_command=f"{ingest_setup} && src/ingest_postgres/update_nba_games.py",
    dag=nba_dag
)

update_box_score = BashOperator(
    task_id='update_box_score',
    bash_command=f"{ingest_setup} && src/ingest_postgres/update_nba_box_score.py",
    dag=nba_dag
)

test_postgres = BashOperator(
    task_id='test_postgres',
    bash_command=f"{ingest_setup} && src/tests/test_postgres.py",
    dag=nba_dag
)

[update_games, update_box_score] >> test_postgres
