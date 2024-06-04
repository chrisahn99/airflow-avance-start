from airflow.decorators import dag, task
from datetime import datetime, timedelta
import time

@dag(dag_id="dag_sla_viz", schedule="*/2 * * * *", start_date=datetime(2024,6,3))
def taskflow():
    @task(task_id="sleep_sla", sla=timedelta(seconds=10))
    def task_sleep_sla():
        time.sleep(20)

    @task(task_id="sleep")
    def task_sleep():
        time.sleep(10)

    task_sleep_sla() >> task_sleep() 
taskflow()