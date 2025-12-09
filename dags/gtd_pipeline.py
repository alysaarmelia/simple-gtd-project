from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
import pandas as pd
import io
import logging
import sys
import os

# --- 1. IMPORT THE ML SCRIPT ---
# We tell Airflow where to find your risk_model.py file
sys.path.append('/opt/airflow/ml_script')
try:
    from risk_model import run_risk_prediction
except ImportError as e:
    logging.error(f"Could not import risk_model: {e}")
    # Dummy function prevents DAG from crashing if file is missing
    def run_risk_prediction():
        logging.error("Model script not found!")

# --- 2. CONFIGURATION ---
MINIO_CONN_ID = 'minio_conn'      # Name of connection in Airflow Admin
POSTGRES_CONN_ID = 'postgres_conn'# Name of connection in Airflow Admin
BUCKET_NAME = 'raw-data'          # MinIO Bucket
FILE_KEY = 'gtd_raw.csv'          # File name
TABLE_NAME = 'raw_gtd'            # Target Raw Table

# --- 3. EXTRACT & LOAD FUNCTION ---
def load_minio_to_postgres(**kwargs):
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    logging.info(f"Downloading {FILE_KEY}...")
    file_obj = s3_hook.get_key(key=FILE_KEY, bucket_name=BUCKET_NAME)
    file_content = file_obj.get()['Body'].read()
    
    # Load CSV
    df = pd.read_csv(io.BytesIO(file_content), encoding='ISO-8859-1', low_memory=False)
    
    # Connect to Postgres
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    
    # --- FIX STARTS HERE ---
    # We must drop the table manually with CASCADE to remove the dependent View
    logging.info("Dropping old table and views...")
    with engine.connect() as conn:
        conn.execute("DROP TABLE IF EXISTS raw_gtd CASCADE;")
        
    logging.info(f"Loading {len(df)} rows...")
    # Now we can write safely. 'replace' works because the table is gone.
    df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
    logging.info("Data loaded successfully.")

# --- 4. THE DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0
}

with DAG(
    'gtd_ingestion_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Ingest (Python)
    # Grabs data from MinIO and dumps it into Postgres "raw_gtd"
    ingest_task = PythonOperator(
        task_id='load_minio_to_postgres',
        python_callable=load_minio_to_postgres
    )

    # Task 2: Transform (Bash/dbt)
    # Runs the SQL models in your dbt_project folder
    transform_task = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .'
    )

    # Task 3: Predict (Python ML)
    # Runs the risk_model.py script to generate scores
    ml_task = PythonOperator(
        task_id='train_risk_model',
        python_callable=run_risk_prediction
    )

    # --- 5. DEPENDENCIES ---
    # Load -> then Transform -> then Predict
    ingest_task >> transform_task >> ml_task