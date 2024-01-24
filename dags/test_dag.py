from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

my_dag = DAG(
    dag_id="my_dag_name",
    start_date=datetime(2024, 1, 24),
    schedule="@daily",
    catchup=False,
)

task_1 = EmptyOperator(task_id="task_1", dag=my_dag)

task_2 = EmptyOperator(task_id="task_2", dag=my_dag)

task_3 = EmptyOperator(task_id="task_3", dag=my_dag)

task_4 = EmptyOperator(task_id="task_4", dag=my_dag)

[task_1, task_2] >> task_3 >> task_4
