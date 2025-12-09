@echo off
REM ==========================================
REM GTD Analytics Platform - ML Setup Script (FIXED)
REM Run this INSIDE the 'simple-gtd-project' folder
REM ==========================================

echo Setting up Machine Learning scripts...

REM --- Check if we are in the right place ---
if not exist "ml_script" (
    echo ERROR: Could not find 'ml_script' folder.
    echo Please make sure you are running this inside 'simple-gtd-project'.
    pause
    exit /b
)

REM ==========================================
REM 1. Create risk_model.py (Line-by-Line to avoid errors)
REM ==========================================
echo Creating ml_script/risk_model.py...

REM Note: We use '>' for the first line to overwrite, and '>>' for the rest to append.

echo import pandas as pd > ml_script\risk_model.py
echo from sqlalchemy import create_engine >> ml_script\risk_model.py
echo from sklearn.ensemble import RandomForestRegressor >> ml_script\risk_model.py
echo import logging >> ml_script\risk_model.py
echo. >> ml_script\risk_model.py
echo # --- CONFIGURATION --- >> ml_script\risk_model.py
echo DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow" >> ml_script\risk_model.py
echo. >> ml_script\risk_model.py
echo def run_risk_prediction(): >> ml_script\risk_model.py
echo     print("Starting Risk Prediction Model...") >> ml_script\risk_model.py
echo     # 1. CONNECT TO WAREHOUSE >> ml_script\risk_model.py
echo     engine = create_engine(DB_CONN) >> ml_script\risk_model.py
echo     # 2. FETCH DATA (Aggregate attacks per country/year) >> ml_script\risk_model.py
echo     query = """ >> ml_script\risk_model.py
echo     SELECT >> ml_script\risk_model.py
echo         country_name, >> ml_script\risk_model.py
echo         year, >> ml_script\risk_model.py
echo         count(event_id) as attack_count, >> ml_script\risk_model.py
echo         sum(killed) as total_killed >> ml_script\risk_model.py
echo     FROM public_staging.stg_attacks >> ml_script\risk_model.py
echo     GROUP BY country_name, year >> ml_script\risk_model.py
echo     ORDER BY country_name, year; >> ml_script\risk_model.py
echo     """ >> ml_script\risk_model.py
echo     print("Fetching training data from Postgres...") >> ml_script\risk_model.py
echo     df = pd.read_sql(query, engine) >> ml_script\risk_model.py
echo     # 3. PREPARE DATA FOR ML >> ml_script\risk_model.py
echo     countries = df['country_name'].unique() >> ml_script\risk_model.py
echo     print(f"Training models for {len(countries)} countries...") >> ml_script\risk_model.py
echo     final_predictions = [] >> ml_script\risk_model.py
echo     for country in countries: >> ml_script\risk_model.py
echo         country_data = df[df['country_name'] == country].copy() >> ml_script\risk_model.py
echo         if len(country_data) ^< 5: continue >> ml_script\risk_model.py
echo         X = country_data[['year']] >> ml_script\risk_model.py
echo         y = country_data['attack_count'] >> ml_script\risk_model.py
echo         model = RandomForestRegressor(n_estimators=100, random_state=42) >> ml_script\risk_model.py
echo         model.fit(X, y) >> ml_script\risk_model.py
echo         last_year = country_data['year'].max() >> ml_script\risk_model.py
echo         next_year = last_year + 1 >> ml_script\risk_model.py
echo         prediction = model.predict([[next_year]])[0] >> ml_script\risk_model.py
echo         risk_score = min(max(prediction, 0), 100) >> ml_script\risk_model.py
echo         if prediction ^> 100: risk_score = 100 >> ml_script\risk_model.py
echo         final_predictions.append({ >> ml_script\risk_model.py
echo             'country_name': country, >> ml_script\risk_model.py
echo             'prediction_year': int(next_year), >> ml_script\risk_model.py
echo             'predicted_attacks': round(prediction, 2), >> ml_script\risk_model.py
echo             'risk_score': round(risk_score, 2) >> ml_script\risk_model.py
echo         }) >> ml_script\risk_model.py
echo     # 4. SAVE PREDICTIONS >> ml_script\risk_model.py
echo     pred_df = pd.DataFrame(final_predictions) >> ml_script\risk_model.py
echo     pred_df = pred_df.sort_values(by='risk_score', ascending=True) >> ml_script\risk_model.py
echo     print("Saving predictions to table 'investment_risk_predictions'...") >> ml_script\risk_model.py
echo     pred_df.to_sql('investment_risk_predictions', engine, if_exists='replace', index=False) >> ml_script\risk_model.py
echo     print("SUCCESS: ML Pipeline Finished.") >> ml_script\risk_model.py
echo if __name__ == "__main__": >> ml_script\risk_model.py
echo     run_risk_prediction() >> ml_script\risk_model.py

REM ==========================================
REM 2. Update gtd_pipeline.py
REM ==========================================
echo Updating dags/gtd_pipeline.py...

echo from airflow import DAG > dags\gtd_pipeline.py
echo from airflow.operators.python import PythonOperator >> dags\gtd_pipeline.py
echo from airflow.operators.bash import BashOperator >> dags\gtd_pipeline.py
echo from airflow.providers.postgres.hooks.postgres import PostgresHook >> dags\gtd_pipeline.py
echo from airflow.providers.amazon.aws.hooks.s3 import S3Hook >> dags\gtd_pipeline.py
echo from airflow.utils.dates import days_ago >> dags\gtd_pipeline.py
echo import pandas as pd >> dags\gtd_pipeline.py
echo import io >> dags\gtd_pipeline.py
echo import logging >> dags\gtd_pipeline.py
echo import sys >> dags\gtd_pipeline.py
echo import os >> dags\gtd_pipeline.py
echo. >> dags\gtd_pipeline.py
echo # Define Paths and Import ML Script >> dags\gtd_pipeline.py
echo AIRFLOW_HOME = os.getenv('AIRFLOW_HOME') >> dags\gtd_pipeline.py
echo sys.path.append(f'{AIRFLOW_HOME}/ml_script') >> dags\gtd_pipeline.py
echo from risk_model import run_risk_prediction >> dags\gtd_pipeline.py
echo. >> dags\gtd_pipeline.py
echo # --- CONFIGURATION --- >> dags\gtd_pipeline.py
echo MINIO_CONN_ID = 'minio_conn' >> dags\gtd_pipeline.py
echo POSTGRES_CONN_ID = 'postgres_conn' >> dags\gtd_pipeline.py
echo BUCKET_NAME = 'raw-data' >> dags\gtd_pipeline.py
echo FILE_KEY = 'gtd_raw.csv' >> dags\gtd_pipeline.py
echo TABLE_NAME = 'raw_gtd' >> dags\gtd_pipeline.py
echo. >> dags\gtd_pipeline.py
echo def load_minio_to_postgres(**kwargs): >> dags\gtd_pipeline.py
echo     s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID) >> dags\gtd_pipeline.py
echo     logging.info(f"Downloading {FILE_KEY}...") >> dags\gtd_pipeline.py
echo     file_obj = s3_hook.get_key(key=FILE_KEY, bucket_name=BUCKET_NAME) >> dags\gtd_pipeline.py
echo     file_content = file_obj.get()['Body'].read() >> dags\gtd_pipeline.py
echo     df = pd.read_csv(io.BytesIO(file_content), encoding='ISO-8859-1', low_memory=False) >> dags\gtd_pipeline.py
echo     pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID) >> dags\gtd_pipeline.py
echo     engine = pg_hook.get_sqlalchemy_engine() >> dags\gtd_pipeline.py
echo     df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False) >> dags\gtd_pipeline.py
echo. >> dags\gtd_pipeline.py
echo default_args = { >> dags\gtd_pipeline.py
echo     'owner': 'airflow', >> dags\gtd_pipeline.py
echo     'start_date': days_ago(1), >> dags\gtd_pipeline.py
echo     'retries': 0 >> dags\gtd_pipeline.py
echo } >> dags\gtd_pipeline.py
echo. >> dags\gtd_pipeline.py
echo with DAG( >> dags\gtd_pipeline.py
echo     'gtd_ingestion_pipeline', >> dags\gtd_pipeline.py
echo     default_args=default_args, >> dags\gtd_pipeline.py
echo     schedule_interval='@daily', >> dags\gtd_pipeline.py
echo     catchup=False >> dags\gtd_pipeline.py
echo ) as dag: >> dags\gtd_pipeline.py
echo. >> dags\gtd_pipeline.py
echo     ingest_task = PythonOperator( >> dags\gtd_pipeline.py
echo         task_id='load_minio_to_postgres', >> dags\gtd_pipeline.py
echo         python_callable=load_minio_to_postgres >> dags\gtd_pipeline.py
echo     ) >> dags\gtd_pipeline.py
echo. >> dags\gtd_pipeline.py
echo     transform_task = BashOperator( >> dags\gtd_pipeline.py
echo         task_id='dbt_run', >> dags\gtd_pipeline.py
echo         bash_command='cd /opt/airflow/dbt_project ^&^& dbt run --profiles-dir .' >> dags\gtd_pipeline.py
echo     ) >> dags\gtd_pipeline.py
echo. >> dags\gtd_pipeline.py
echo     ml_task = PythonOperator( >> dags\gtd_pipeline.py
echo         task_id='train_risk_model', >> dags\gtd_pipeline.py
echo         python_callable=run_risk_prediction >> dags\gtd_pipeline.py
echo     ) >> dags\gtd_pipeline.py
echo. >> dags\gtd_pipeline.py
echo     ingest_task ^>^> transform_task ^>^> ml_task >> dags\gtd_pipeline.py

echo.
echo ==========================================
echo SUCCESS! ML Pipeline created correctly.
echo ==========================================
pause