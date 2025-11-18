from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
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

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Membersihkan nama kolom: ganti spasi, tanda %, dan konversi ke snake_case."""
    column_mapping = {
        'Invoice ID': 'invoice_id',
        'Branch': 'branch',
        'City': 'city',
        'Customer type': 'customer_type',
        'Gender': 'gender',
        'Product line': 'product_line',
        'Unit price': 'unit_price',
        'Quantity': 'quantity',
        'Tax 5%': 'tax_5_percent',
        'Total': 'total',
        'Date': 'order_date',    
        'Time': 'order_time',    
        'Payment': 'payment',
        'cogs': 'cogs', 
        'gross margin percentage': 'gross_margin_percentage',
        'gross income': 'gross_income',
        'Rating': 'rating'
    }
    
    df = df.rename(columns=column_mapping)
    
    return df

def insert_to_postgres_supermarket():
    try:
        df = pd.read_csv("/opt/airflow/data/supermarket.csv")
        
        df = clean_column_names(df)
        
        postgres_connection = postgres_connect()
        df.to_sql('transaction_supermarket', con=postgres_connection, if_exists='append', index=False)
    except Exception as e:
        logging.error(f"Error inserting data to Postgres: {e}")
        raise

def transfer_data_postgres_to_bigquery():
    try:
        pg_engine = postgres_connect()
        
        query = "SELECT * FROM transaction_supermarket"
        df = pd.read_sql(query, con=pg_engine)
        logging.info(f"Read {len(df)} records from Postgres")
        
        bq_client = bigquery_connect()
        
        table_id = "terraform-demo-475203.supermarket.raw_transaction_supermarket"
        
        job = bq_client.load_table_from_dataframe(df, table_id)
        job.result()
        
        logging.info(f"Successfully transferred {len(df)} records to BigQuery")
    except Exception as e:
        logging.error(f"Error transferring data: {e}")
        raise
    
with DAG(
    'csv_to_warehouse',
    default_args=default_args,
    description='Transfer data from CSV to BigQuery',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    csv_to_pg_task = PythonOperator(
        task_id='insert_to_postgres',
        python_callable=insert_to_postgres_supermarket,
    )
    
    pg_to_warehouse_task = PythonOperator(
        task_id='transfer_data_postgres_to_bigquery',
        python_callable=transfer_data_postgres_to_bigquery,
    )
    
    dbt_run_staging = BashOperator(
    task_id="dbt_run_staging",
    bash_command=(
        "cd /opt/airflow/dbt && "
        "dbt run --select stg_supermarket_sales"
    ),
)

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select dim_supermarket_outlet fact_supermarket_sales"
        ),
    )

    # dbt_test = BashOperator(
    #     task_id="dbt_test",
    #     bash_command=(
    #         "cd /opt/airflow/dbt && "
    #         "dbt test"
    #     ),
    # )


    csv_to_pg_task >> pg_to_warehouse_task >> dbt_run_staging >> dbt_run_marts