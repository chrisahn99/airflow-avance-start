from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_example_external_task_sensor',
    default_args=default_args,
    description='An example DAG that waits for a task in another DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 25),
    catchup=False,
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

wait_for_task = ExternalTaskSensor(
    task_id='wait_for_external_task',
    external_dag_id='other_dag',  # ID du DAG externe
    external_task_id='task_in_other_dag',  # ID de la tâche dans le DAG externe
    mode='poke',
    timeout=600,  # Timeout après 10 minutes
    poke_interval=60,  # Intervalle de vérification toutes les minutes
    dag=dag,
)

continue_task = DummyOperator(
    task_id='continue',
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> wait_for_task >> continue_task >> end
