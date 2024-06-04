from datetime import datetime, timedelta
import pendulum
import logging
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(minutes=5),
}

def postgres_to_pandas(dag_id):

    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        "select * from dag_runs"
    )

    data = cursor.fetchall()
    columns = [description[0] for description in cursor.description]

    df = pd.DataFrame(data, columns=columns)
    filtered_df = df[df["dag_id"]==dag_id]

    logging.info(filtered_df.to_string())

    cursor.close()
    conn.close()


with DAG(
    dag_id='dag_runs',
    default_args=default_args,
    start_date=pendulum.datetime(2024, 5, 1, tz="Europe/Paris"),
    end_date=pendulum.datetime(2024, 7, 1, tz="Europe/Paris"),
    catchup=True,
    schedule_interval='0 12 * * *',
    dagrun_timeout=timedelta(minutes=60)
) as dag:
    create_task = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            create table if not exists dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )

    insert_task = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            insert into dag_runs (dt, dag_id) values ('{{ ds }}', '{{ dag.dag_id }}')
        """
    )

    delete_task = PostgresOperator(
        task_id='delete_data_from_table',
        postgres_conn_id='postgres_localhost',
        sql="""
            delete from dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}';
        """
    )

    cursor_task = PythonOperator(
        task_id='filter_and_log',
        python_callable=postgres_to_pandas,
        op_kwargs={"dag_id": dag.dag_id}
    )
    
    create_task >> delete_task >> insert_task >> cursor_task