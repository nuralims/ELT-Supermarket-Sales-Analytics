from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import logging

default_args = {
    "owner": "Muhammad Nuralimsyah",
    "start_date": datetime(2025, 11, 13),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "alimsyah071002@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def postgres_connect():
    try:
        pg_hook = PostgresHook(postgres_conn_id='airflow_db')
        engine = create_engine(pg_hook.get_uri())
        logging.info("Successfully connected to Postgres")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to Postgres: {e}")
        raise

def bigquery_connect():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            '/opt/airflow/data/terraform-demo-475203-42699b6cec5f.json'
        )
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        logging.info("Successfully connected to BigQuery")
        return client
    except Exception as e:
        logging.error(f"Error connecting to BigQuery: {e}")
        raise

def transfer_data_postgres_to_bigquery():
    try:
        # Connect to Postgres
        pg_engine = postgres_connect()
        
        # Read data from Postgres
        query = "SELECT * FROM transaction_supermarket"
        df = pd.read_sql(query, con=pg_engine)
        logging.info(f"Read {len(df)} records from Postgres")
        
        # Connect to BigQuery
        bq_client = bigquery_connect()
        
        # Define BigQuery table
        table_id = "terraform-demo-475203.transaction_supermarket.transaction_supermarket"
        
        # Load data to BigQuery
        job = bq_client.load_table_from_dataframe(df, table_id)
        job.result()  # Wait for the job to complete
        
        logging.info(f"Successfully transferred {len(df)} records to BigQuery")
    except Exception as e:
        logging.error(f"Error transferring data: {e}")
        raise
    
with DAG(
    'pg_to_bigquery',
    default_args=default_args,
    description='Transfer data from Postgres to BigQuery',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    transfer_task = PythonOperator(
        task_id='transfer_data_postgres_to_bigquery',
        python_callable=transfer_data_postgres_to_bigquery,
    )

    transfer_task