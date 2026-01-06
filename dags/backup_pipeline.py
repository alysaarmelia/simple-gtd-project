from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import io
import logging

# --- KONFIGURASI ---
# Menggunakan Connection ID yang sama dengan gtd_pipeline.py
POSTGRES_CONN_ID = 'postgres_conn' 
MINIO_CONN_ID = 'minio_conn'

# Bucket tempat menyimpan hasil backup
BACKUP_BUCKET_NAME = 'system-backups'
SOURCE_BUCKET_NAME = 'raw-data'

# Daftar Tabel yang akan di-backup (Schema.Table)
# Kita backup tabel Warehouse (hasil olahan) dan Mart (hasil akhir)
TARGET_TABLES = [
    "warehouse.dim_country",
    "warehouse.dim_date",
    "warehouse.dim_location",
    "warehouse.dim_attack",
    "warehouse.dim_target",
    "warehouse.dim_perpetrator",
    "warehouse.dim_weapon",
    "warehouse.dim_narrative",
    "warehouse.dim_economy",
    "warehouse.fact_attacks",
    "mart.mart_risk_analysis",
    "public.investment_risk_predictions"
]

def create_backup_bucket(**kwargs):
    """Memastikan bucket 'system-backups' tersedia di MinIO"""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    if not s3_hook.check_for_bucket(BACKUP_BUCKET_NAME):
        s3_hook.create_bucket(bucket_name=BACKUP_BUCKET_NAME)
        logging.info(f"Bucket '{BACKUP_BUCKET_NAME}' berhasil dibuat.")
    else:
        logging.info(f"Bucket '{BACKUP_BUCKET_NAME}' sudah ada.")

def backup_postgres_to_minio(**kwargs):
    """
    Ekstrak tabel Postgres -> CSV -> Upload ke MinIO
    """
    execution_date = kwargs['ds'] # Format: YYYY-MM-DD
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    for table in TARGET_TABLES:
        try:
            logging.info(f"Backing up table: {table}...")
            
            # 1. Baca data dari Postgres
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, engine)

            if df.empty:
                logging.warning(f"Tabel {table} kosong, skip.")
                continue

            # 2. Convert ke CSV di Memory
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            # 3. Upload ke MinIO
            # Path: backups/2023-01-01/postgres/warehouse.dim_country.csv
            s3_key = f"backups/{execution_date}/postgres/{table}.csv"
            
            s3_hook.load_string(
                string_data=csv_buffer.getvalue(),
                key=s3_key,
                bucket_name=BACKUP_BUCKET_NAME,
                replace=True
            )
            logging.info(f"✅ Success: {table} -> {BACKUP_BUCKET_NAME}/{s3_key}")

        except Exception as e:
            logging.error(f"❌ Gagal backup tabel {table}: {e}")
            # Kita tidak raise error agar tabel lain tetap ter-backup

def backup_raw_files_minio(**kwargs):
    """
    Copy file raw (sumber data) ke folder backup untuk snapshot
    """
    execution_date = kwargs['ds']
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

    # List semua file di bucket raw-data
    keys = s3_hook.list_keys(bucket_name=SOURCE_BUCKET_NAME)
    
    if not keys:
        logging.info("Tidak ada file di raw-data bucket.")
        return

    for key in keys:
        # Copy Logic
        source_key = key
        # Path: backups/2023-01-01/raw_files/gtd_raw.csv
        dest_key = f"backups/{execution_date}/raw_files/{key}"
        
        logging.info(f"Copying {source_key} to {dest_key}...")
        
        s3_hook.copy_object(
            source_bucket_key=source_key,
            dest_bucket_key=dest_key,
            source_bucket_name=SOURCE_BUCKET_NAME,
            dest_bucket_name=BACKUP_BUCKET_NAME
        )
    logging.info("✅ Semua Raw Files berhasil di-snapshot.")

# --- DEFINISI DAG ---
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

with DAG(
    'system_disaster_recovery',
    default_args=default_args,
    description='Backup otomatis Postgres (DWH) dan Raw Data ke MinIO',
    schedule_interval='@weekly',  # Running setiap Minggu tengah malam
    catchup=False,
    tags=['maintenance', 'backup']
) as dag:

    # Task 1: Buat Bucket Backup jika belum ada
    task_init_bucket = PythonOperator(
        task_id='init_backup_bucket',
        python_callable=create_backup_bucket
    )

    # Task 2: Backup Database Warehouse
    task_backup_db = PythonOperator(
        task_id='backup_database_tables',
        python_callable=backup_postgres_to_minio
    )

    # Task 3: Snapshot Raw Files
    task_backup_files = PythonOperator(
        task_id='snapshot_raw_files',
        python_callable=backup_raw_files_minio
    )

    # Alur Eksekusi
    task_init_bucket >> [task_backup_db, task_backup_files]