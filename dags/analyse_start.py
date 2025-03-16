from airflow import DAG
import datetime as dt
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id='analyse_start',
    start_date=dt.datetime(2025,2,13),
    schedule_interval="*/5 * * * *",
)

analyse_operation = BashOperator(
    task_id='analyseStartScript',
    bash_command='python scripts/DBVerification_v.2/src/__run_analyse__.py',
    dag=dag,
)
