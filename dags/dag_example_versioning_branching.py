from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

def choose_version(**kwargs):
    # Logique pour choisir la version
    if kwargs['execution_date'].day % 2 == 0:
        return 'version_1'
    else:
        return 'version_2'

with DAG(
    dag_id='dag_versioning_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    choose_version_task = BranchPythonOperator(
        task_id='choose_version',
        python_callable=choose_version,
    )

    version_1_task = DummyOperator(
        task_id='version_1',
    )

    version_2_task = DummyOperator(
        task_id='version_2',
    )

    choose_version_task >> [version_1_task, version_2_task]
