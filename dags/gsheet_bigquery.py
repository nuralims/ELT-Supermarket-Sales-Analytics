from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import logging

import pandas as pd
import gspread
from google.oauth2 import service_account
from google.cloud import bigquery
from pandasql import sqldf

# ------------------------
# Konfigurasi global
# ------------------------

log = logging.getLogger(__name__)

CREDENTIALS_PATH = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "/opt/airflow/data/terraform-demo-475203-42699b6cec5f.json",
)

GSHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1Nga1qHF1W0Jh932fxT0aTX9ZJonCSdxOIQr-yOzu9Mw/edit#gid=598930547"
)

# Sesuaikan dengan project:dataset.table kamu
BQ_TABLE = "terraform-demo-475203.transaction_supermarket.supermarket_2025"

default_args = {
    "owner": "Muhammad Nuralimsyah",
    "start_date": datetime(2025, 11, 13),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "alimsyah071002@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ------------------------
# Helper functions
# ------------------------

def _ensure_credentials_exist() -> None:
    """Pastikan file service account ada di path yang diharapkan."""
    if not os.path.exists(CREDENTIALS_PATH):
        msg = (
            f"Kredensial tidak ditemukan di {CREDENTIALS_PATH}. "
            "Pastikan file dimount ke container Docker Airflow."
        )
        log.error(msg)
        raise FileNotFoundError(msg)


def bigquery_connection() -> bigquery.Client:
    """Membuat client BigQuery menggunakan service account JSON."""
    try:
        _ensure_credentials_exist()
        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH
        )
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
        log.info("Koneksi ke BigQuery berhasil (project=%s)", credentials.project_id)
        return client
    except Exception as e:
        log.error("Gagal koneksi ke BigQuery: %s", e)
        raise


def google_sheet_to_dataframe() -> pd.DataFrame:
    """Membaca data dari Google Sheets menjadi DataFrame pandas."""
    try:
        _ensure_credentials_exist()
        gc = gspread.service_account(filename=CREDENTIALS_PATH)
        sh = gc.open_by_url(GSHEET_URL)
        # kalau mau sheet tertentu, bisa pakai sh.worksheet("nama_sheet")
        ws = sh.sheet1
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        log.info("Data dari Google Sheets berhasil diambil, jumlah baris: %d", len(df))
        return df
    except Exception as e:
        log.error("Kesalahan saat mengambil data dari Google Sheets: %s", e)
        raise


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Membersihkan & meng-cast kolom dataframe.
    Sekaligus rename kolom supaya valid di BigQuery (tanpa spasi & %).
    """
    try:
        # Normalisasi nama kolom awal: hapus spasi depan/belakang dan ke lower
        df.columns = [c.strip().lower() for c in df.columns]

        required = [
            "invoice id",
            "branch",
            "city",
            "customer type",
            "gender",
            "product line",
            "unit price",
            "quantity",
            "tax 5%",
            "total",
            "date",
            "time",
            "payment",
            "cogs",
            "gross margin percentage",
            "gross income",
            "rating",
        ]

        missing = [c for c in required if c not in df.columns]
        if missing:
            raise KeyError(f"Kolom hilang di Google Sheet: {missing}")

        # Tipe tanggal
        df["date"] = pd.to_datetime(df["date"], errors="coerce", format="%Y-%m-%d")

        # Numerik integer
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)

        # Numerik float
        float_cols = [
            "unit price",
            "tax 5%",
            "total",
            "cogs",
            "gross margin percentage",
            "gross income",
            "rating",
        ]
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

        # String
        str_cols = [
            "invoice id",
            "branch",
            "city",
            "customer type",
            "gender",
            "product line",
            "time",
            "payment",
        ]
        for col in str_cols:
            df[col] = df[col].astype(str).fillna("")

        # Rename kolom supaya valid di BigQuery (tidak pakai spasi & simbol)
        rename_map = {
            "invoice id": "invoice_id",
            "customer type": "customer_type",
            "product line": "product_line",
            "unit price": "unit_price",
            "tax 5%": "tax_5",
            "gross margin percentage": "gross_margin_percentage",
            "gross income": "gross_income",
        }
        df = df.rename(columns=rename_map)

        return df
    except Exception as e:
        log.error("Error dalam membersihkan dataframe: %s", e)
        raise


def etl_data(**_):
    """Task utama ETL: ambil dari Google Sheets → transform → load ke BigQuery."""
    try:
        # BigQuery client
        bq_client = bigquery_connection()

        # Extract
        df_google_sheets = google_sheet_to_dataframe()

        # Transform (contoh pakai SQL; sekarang masih SELECT *)
        df_transform = sqldf("SELECT * FROM df_google_sheets", locals())

        # Clean & cast tipe data
        df_clean = clean_dataframe(df_transform)

        # Schema BigQuery (nama kolom sudah disesuaikan dengan rename di atas)
        schema = [
            bigquery.SchemaField("quantity", "INTEGER"),
            bigquery.SchemaField("unit_price", "FLOAT"),
            bigquery.SchemaField("tax_5", "FLOAT"),
            bigquery.SchemaField("total", "FLOAT"),
            bigquery.SchemaField("cogs", "FLOAT"),
            bigquery.SchemaField("gross_margin_percentage", "FLOAT"),
            bigquery.SchemaField("gross_income", "FLOAT"),
            bigquery.SchemaField("rating", "FLOAT"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("invoice_id", "STRING"),
            bigquery.SchemaField("branch", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("customer_type", "STRING"),
            bigquery.SchemaField("gender", "STRING"),
            bigquery.SchemaField("product_line", "STRING"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("payment", "STRING"),
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_TRUNCATE",
        )

        load_job = bq_client.load_table_from_dataframe(
            df_clean,
            BQ_TABLE,
            job_config=job_config,
        )
        load_job.result()

        log.info("Data berhasil di-upload ke BigQuery pada tabel %s", BQ_TABLE)
    except Exception as e:
        log.error("Proses ETL gagal: %s", e)
        raise


def welcome(**_):
    print("Welcome to Apache Airflow!")


def end_task(**_):
    print("ETL selesai!")


# ------------------------
# Definisi DAG
# ------------------------

with DAG(
    dag_id="google_sheet_to_bigquery",
    schedule="@daily",  # pakai argumen baru 'schedule' (pengganti schedule_interval)
    default_args=default_args,
    catchup=False,
    tags=["gsheet", "bigquery"],
) as dag:
    task_welcome = PythonOperator(
        task_id="say_welcome",
        python_callable=welcome,
    )

    task_etl = PythonOperator(
        task_id="task_etl",
        python_callable=etl_data,
    )

    task_end = PythonOperator(
        task_id="say_end",
        python_callable=end_task,
    )

    task_welcome >> task_etl >> task_end
