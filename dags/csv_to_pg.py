from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def insert_to_postgres_supermarket():
    # Read CSV file
    df = pd.read_csv("/opt/airflow/data/supermarket.csv")
    
    # Establish Postgres connection
    pg_hook = PostgresHook(postgres_conn_id='airflow_db')
    engine = create_engine(pg_hook.get_uri())
    
    # Insert data into Postgres
    df.to_sql('transaction_supermarket', con=engine, if_exists='append', index=False)
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 15),
    'retries': 1,
}

dag = DAG(
    'insert_to_postgres_supermarket',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

insert_task = PythonOperator(
    task_id='insert_to_postgres',
    python_callable=insert_to_postgres_supermarket,
    dag=dag,
)

insert_task