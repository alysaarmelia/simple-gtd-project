import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Konfigurasi Tabel
AUDIT_TABLE_NAME = "etl_audit_logs"
CONN_ID = "postgres_conn"  # ID koneksi yang sama dengan pipeline Anda

def ensure_audit_table_exists():
    """Membuat tabel audit jika belum ada (Run once logic)"""
    sql_create = f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_TABLE_NAME} (
        log_id SERIAL PRIMARY KEY,
        dag_id VARCHAR(100),
        task_id VARCHAR(100),
        status VARCHAR(20),
        execution_date TIMESTAMP,
        duration_seconds FLOAT,
        try_number INT,
        error_message TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        pg_hook.run(sql_create)
    except Exception as e:
        logging.error(f"⚠ Gagal membuat tabel audit: {e}")

def log_to_postgres(context, status):
    """Fungsi inti untuk mencatat status task ke Database"""
    
    # 1. Pastikan tabel ada dulu
    ensure_audit_table_exists()

    # 2. Ambil data dari Context Airflow
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    try_number = context['task_instance'].try_number
    
    # Ambil durasi (kadang None jika error instan)
    duration = context['task_instance'].duration
    if duration is None:
        duration = 0.0

    # Ambil pesan error jika ada
    exception = context.get('exception')
    error_message = str(exception) if exception else None

    # 3. Query Insert
    sql_insert = f"""
    INSERT INTO {AUDIT_TABLE_NAME} 
    (dag_id, task_id, status, execution_date, duration_seconds, try_number, error_message)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    # 4. Eksekusi
    try:
        pg_hook = PostgresHook(postgres_conn_id=CONN_ID)
        pg_hook.run(sql_insert, parameters=(
            dag_id, 
            task_id, 
            status, 
            execution_date, 
            duration, 
            try_number,
            error_message
        ))
        logging.info(f"✅ [AUDIT] Log tersimpan ke DB: {task_id} = {status}")
    except Exception as e:
        # Fallback: Jika DB mati, catat ke terminal saja
        logging.error(f"❌ [AUDIT] Gagal menyimpan log ke DB: {e}")

# --- FUNGSI PEMBUNGKUS (WRAPPER) ---
# Ini yang akan dipanggil oleh DAG
def audit_success_callback(context):
    log_to_postgres(context, 'SUCCESS')

def audit_failure_callback(context):
    log_to_postgres(context, 'FAILED')