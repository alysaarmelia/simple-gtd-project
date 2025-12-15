from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import io
import logging
import sys
import os

# --- 1. IMPORT UTILS & ML SCRIPT ---
sys.path.append('/opt/airflow/ml_script')
# Tambahkan path folder dags agar bisa import utils_alerting
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import Callback Lokal
try:
    from utils_alerting import audit_success_callback, audit_failure_callback
except ImportError as e:
    logging.error(f"Gagal import utils_alerting: {e}")
    # Dummy function agar tidak crash jika file hilang
    def audit_success_callback(context): pass
    def audit_failure_callback(context): pass

try:
    from risk_model import run_risk_prediction
except ImportError as e:
    logging.error(f"Could not import risk_model: {e}")
    def run_risk_prediction():
        logging.error("Model script not found!")

# --- 2. CONFIGURATION ---
MINIO_CONN_ID = 'minio_conn'
POSTGRES_CONN_ID = 'postgres_conn'
BUCKET_NAME = 'raw-data'

FILE_KEY_GTD = 'gtd_raw.csv'
FILE_KEY_PROP = 'oecd_property.csv'

TABLE_NAME_GTD = 'raw_gtd'            
TABLE_NAME_PROP = 'raw_property_index' 

# --- 3. EXTRACT & LOAD FUNCTIONS ---

def load_minio_to_postgres_gtd(**kwargs):
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    logging.info(f"Downloading {FILE_KEY_GTD} from MinIO...")
    file_obj = s3_hook.get_key(key=FILE_KEY_GTD, bucket_name=BUCKET_NAME)
    file_content = file_obj.get()['Body'].read()
    
    df = pd.read_csv(io.BytesIO(file_content), encoding='ISO-8859-1', low_memory=False)
    
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    
    logging.info("Dropping old GTD table...")
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME_GTD} CASCADE;")
        
    logging.info(f"Loading {len(df)} rows to Postgres...")
    df.to_sql(TABLE_NAME_GTD, engine, if_exists='append', index=False)
    logging.info("GTD Data loaded successfully.")

def ingest_oecd_property_data(**kwargs):
    logging.info("=== EXTRACT: DOWNLOAD DATA OECD API ===")
    url = "https://sdmx.oecd.org/public/rest/data/OECD.ECO.MPD,DSD_AN_HOUSE_PRICES@DF_HOUSE_PRICES,/.Q.RHP."
    params = {
        "startPeriod": "1970-Q1",
        "endPeriod": "2020-Q4",
        "format": "sdmx-json"
    }
    headers = {"Accept": "application/json"}
    
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    logging.info(f"Fetching data from: {url}")
    response = session.get(url, params=params, headers=headers, timeout=120)
    response.raise_for_status()
    data = response.json()
    
    logging.info("Parsing JSON response...")
    series = data["dataSets"][0]["series"]
    series_dims = data["structure"]["dimensions"]["series"]
    obs_dims = data["structure"]["dimensions"]["observation"]
    countries = series_dims[0]["values"]
    time_periods = obs_dims[0]["values"]

    records = []
    for series_key, series_value in series.items():
        country_idx = int(series_key.split(":")[0])
        country_name = countries[country_idx]["name"]
        for obs_key, obs_val in series_value["observations"].items():
            time_idx = int(obs_key)
            period = time_periods[time_idx]["id"]
            records.append({
                "country": country_name,
                "period": period,
                "real_house_price_index": obs_val[0],
                "year": int(period[:4])
            })
    
    df = pd.DataFrame(records)
    logging.info(f"Extracted {len(df)} rows.")

    logging.info(f"=== LOAD: UPLOADING TO MINIO ({FILE_KEY_PROP}) ===")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=FILE_KEY_PROP,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    logging.info("Success! Data saved to MinIO Raw Bucket.")

    logging.info(f"=== LOAD: SAVING TO POSTGRES ({TABLE_NAME_PROP}) ===")
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql(TABLE_NAME_PROP, engine, if_exists='replace', index=False)
    logging.info("Success! Data saved to Postgres.")

# --- 4. THE DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    # --- PASANG CALLBACK LOKAL DISINI ---
    'on_success_callback': audit_success_callback,
    'on_failure_callback': audit_failure_callback
}

with DAG(
    'gtd_ingestion_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    ingest_gtd_task = PythonOperator(
        task_id='load_minio_to_postgres',
        python_callable=load_minio_to_postgres_gtd
    )

    ingest_property_task = PythonOperator(
        task_id='load_oecd_property_to_postgres',
        python_callable=ingest_oecd_property_data
    )

    transform_task = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .'
    )

    ml_task = PythonOperator(
        task_id='train_risk_model',
        python_callable=run_risk_prediction
    )

    [ingest_gtd_task, ingest_property_task] >> transform_task >> ml_task