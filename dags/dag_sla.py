import datetime
import pendulum
import time

from airflow.decorators import dag, task

@dag(
    dag_id="dag_sla",
    schedule="*/2 * * * *",
    start_date=pendulum.datetime(2024, 5, 1, tz="UTC"),
)
def dag_sla():
    @task(task_id="first_sleep", sla=datetime.timedelta(seconds=10))
    def first_sleep():
        time.sleep(20)
    
    @task
    def second_sleep():
        time.sleep(10)

    first_sleep() >> second_sleep()

dag_sla() 

    
