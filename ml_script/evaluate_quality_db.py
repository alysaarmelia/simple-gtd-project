import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import logging
import warnings

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# KONFIGURASI KONEKSI DATABASE
# Gunakan 'localhost' karena script ini dijalankan di komputer host (laptop Anda), 
# bukan di dalam container Docker. Port 5432 sudah di-expose di docker-compose.yaml.
DB_CONN = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"

def get_data_from_db():
    """Mengambil data asli dari Data Warehouse"""
    logging.info("Menghubungkan ke database lokal...")
    try:
        engine = create_engine(DB_CONN)
        
        # Query sama persis dengan risk_model.py
        query = """
        SELECT 
            l.country_name,
            d.year,
            COUNT(f.incident_count) AS attack_count
        FROM public_warehouse.fact_attacks f
        JOIN public_warehouse.dim_location l ON f.location_id = l.location_id
        JOIN public_warehouse.dim_date d ON f.date_id = d.date_id
        GROUP BY l.country_name, d.year
        ORDER BY l.country_name, d.year;
        """
        df = pd.read_sql(query, engine)
        logging.info(f"Berhasil mengambil {len(df)} baris data.")
        return df
    except Exception as e:
        logging.error(f"Gagal konek database: {e}")
        logging.error("Pastikan Docker menyala dan port 5432 terbuka.")
        return pd.DataFrame()

def feature_engineering(df):
    """Logic Feature Engineering disamakan dengan risk_model.py"""
    df = df.sort_values("year")
    df = df.rename(columns={"attack_count": "annual_attacks"})
    
    # Feature Engineering
    df["annual_attacks"] = df["annual_attacks"].astype(float)
    df["year"] = df["year"].astype(int)

    df["attacks_lag1"] = df["annual_attacks"].shift(1)
    df["attacks_lag2"] = df["annual_attacks"].shift(2)
    df["attacks_lag3"] = df["annual_attacks"].shift(3)
    df["mean_3y"] = df["annual_attacks"].rolling(3).mean()
    df["std_3y"] = df["annual_attacks"].rolling(3).std()
    df["trend_1y"] = df["annual_attacks"].shift(1) - df["annual_attacks"].shift(2)
    df["year_scaled"] = (df["year"] - df["year"].min()) / (df["year"].max() - df["year"].min())
    
    return df.dropna()

def run_evaluation():
    # 1. AMBIL DATA DARI DB
    df_raw = get_data_from_db()
    
    if df_raw.empty:
        return

    countries = df_raw['country_name'].unique()
    results = []
    
    logging.info(f"Memulai evaluasi untuk {len(countries)} negara...")

    for country in countries:
        c_data = df_raw[df_raw["country_name"] == country].copy()
        
        # Skip jika data terlalu sedikit
        if len(c_data) < 10: continue

        df_processed = feature_engineering(c_data)
        if len(df_processed) < 5: continue

        features = ["attacks_lag1", "attacks_lag2", "attacks_lag3", "mean_3y", "std_3y", "trend_1y", "year_scaled"]
        
        X = df_processed[features]
        y = np.log1p(df_processed["annual_attacks"].values)

        # Split Train/Test
        train_size = int(len(X) * 0.8)
        X_train, X_test = X.iloc[:train_size], X.iloc[train_size:]
        y_train, y_test = y[:train_size], y[train_size:]
        
        if len(X_test) < 1: continue
        
        y_test_real = np.expm1(y_test)

        # --- MODEL 1: XGBoost ---
        xgb = XGBRegressor(n_estimators=1000, max_depth=3, learning_rate=0.01, random_state=42, n_jobs=-1)
        xgb.fit(X_train, y_train)
        pred_xgb = np.expm1(xgb.predict(X_test))
        
        # --- MODEL 2: Random Forest ---
        rf = RandomForestRegressor(n_estimators=500, max_depth=5, min_samples_leaf=2, random_state=42, n_jobs=-1)
        rf.fit(X_train, y_train)
        pred_rf = np.expm1(rf.predict(X_test))

        # Simpan Metrics
        results.append({
            'Country': country,
            'Model': 'XGBoost',
            'MAE': mean_absolute_error(y_test_real, pred_xgb),
            'RMSE': np.sqrt(mean_squared_error(y_test_real, pred_xgb)),
            'R2': r2_score(y_test_real, pred_xgb)
        })
        results.append({
            'Country': country,
            'Model': 'Random Forest',
            'MAE': mean_absolute_error(y_test_real, pred_rf),
            'RMSE': np.sqrt(mean_squared_error(y_test_real, pred_rf)),
            'R2': r2_score(y_test_real, pred_rf)
        })

    # 3. SUMMARY REPORT
    if not results:
        logging.warning("Data tidak cukup untuk evaluasi.")
        return

    df_res = pd.DataFrame(results)
    
    # Hitung rata-rata global
    summary = df_res.groupby('Model')[['MAE', 'RMSE', 'R2']].mean().reset_index()

    print("\n" + "="*50)
    print("📊 LAPORAN KUALITAS MODEL (DATA REAL)")
    print("="*50)
    print(summary.to_string(index=False))
    print("\n")
    
    # Opsional: Simpan ke CSV untuk dilihat nanti
    summary.to_csv("local_model_metrics.csv", index=False)
    logging.info("Laporan disimpan ke 'local_model_metrics.csv'")

if __name__ == "__main__":
    run_evaluation()