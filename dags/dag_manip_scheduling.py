from datetime import timedelta
import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=5),
}

def fetch_and_process_data(dag_id):
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM dag_runs;")
    rows = cursor.fetchall()

    df = pd.DataFrame(rows, columns=["dt", "dag_id"])
    print(df)
    filtered_df = df[df["dag_id"] == "dag_manip"]
    print(filtered_df)


with DAG(
    dag_id="dag_manip_scheduling",
    start_date=pendulum.datetime(2024, 5, 1, tz="Europe/Paris"),
    end_date=pendulum.datetime(2024, 7, 1, tz="Europe/Paris"),
    schedule_interval="0 12 * * *",
    catchup=True,
    dagrun_timeout=timedelta(hours=1),
    ) as dag:
    task_1 = PostgresOperator(
        task_id="create_postgres_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            create table if not exists dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )

    task_2 = PostgresOperator(
        task_id="delete_data_from_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            delete from dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id}}';
        """
    )

    task_3 = PostgresOperator(
        task_id="insert_into_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            insert into dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id}}')
        """
    )

    task_4 = PythonOperator(
        task_id="fetch_and_process_data",
        python_callable=fetch_and_process_data,
        op_kwargs={"dag_id": dag.dag_id}
    )
 
    task_1 >> task_2 >> task_3 >> task_4