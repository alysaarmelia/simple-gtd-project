import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import logging
import warnings

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO)

# ====== CONFIG ==================================================================
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
TARGET_TABLE = "investment_risk_predictions"
# ================================================================================


def run_risk_prediction_xgboost_yearly():
    logging.info("=" * 80)
    logging.info("XGBoost Forecast – Yearly Attacks per Country (Data Warehouse)")
    logging.info("=" * 80)

    engine = create_engine(DB_CONN)

    # ======================================================================
    # STEP 1: LOAD DATA FROM WAREHOUSE (PER COUNTRY PER YEAR)
    # ======================================================================
    logging.info("\n[STEP 1] Loading aggregated attacks per country-year from warehouse...")

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

    if df.empty:
        logging.error("No data found from warehouse!")
        return

    logging.info(f"✓ Loaded {len(df)} rows (country-year level)")
    logging.info("Sample:")
    logging.info(df.head().to_string(index=False))

    # ======================================================================
    # STEP 2: LOOP PER COUNTRY – MIRIP gtd_xgboost_yearly, TAPI PER NEGARA
    # ======================================================================
    logging.info("\n[STEP 2] Running XGBoost yearly forecast per country...")

    countries = df["country_name"].unique()
    final_predictions = []
    global_accuracies = []

    for country in countries:
        c_data = df[df["country_name"] == country].copy()

        # minimal 10 tahun data
        if len(c_data) < 10:
            logging.info(f"[SKIP] {country} - kurang dari 10 tahun data.")
            continue

        # sort by year
        c_data = c_data.sort_values("year")
        logging.info(f"\n[COUNTRY] {country} – Years {c_data['year'].min()}–{c_data['year'].max()} ({len(c_data)} rows)")

        # ==================================================================
        # FEATURE ENGINEERING (mirip gtd_xgboost_yearly tapi per-country)
        # ==================================================================
        dfc = c_data.rename(columns={"attack_count": "annual_attacks"}).copy()

        # Lags
        dfc["attacks_lag1"] = dfc["annual_attacks"].shift(1)
        dfc["attacks_lag2"] = dfc["annual_attacks"].shift(2)
        dfc["attacks_lag3"] = dfc["annual_attacks"].shift(3)
        dfc["attacks_lag5"] = dfc["annual_attacks"].shift(5)

        # Rolling stats
        dfc["mean_attacks_3y"] = dfc["annual_attacks"].rolling(3).mean()
        dfc["mean_attacks_5y"] = dfc["annual_attacks"].rolling(5).mean()
        dfc["std_attacks_3y"] = dfc["annual_attacks"].rolling(3).std()
        dfc["std_attacks_5y"] = dfc["annual_attacks"].rolling(5).std()

        # Trends
        dfc["trend_2y"] = dfc["annual_attacks"] - dfc["annual_attacks"].shift(2)
        dfc["trend_3y"] = dfc["annual_attacks"] - dfc["annual_attacks"].shift(3)

        # Time encodings
        dfc["year_scaled"] = (dfc["year"] - dfc["year"].min()) / (dfc["year"].max() - dfc["year"].min())
        dfc["year_sin"] = np.sin(2 * np.pi * dfc["year_scaled"])
        dfc["year_cos"] = np.cos(2 * np.pi * dfc["year_scaled"])

        # Era flags sederhana global (boleh dihapus kalau tidak relevan)
        dfc["post_911"] = (dfc["year"] >= 2001).astype(int)
        dfc["post_invasions"] = (dfc["year"] >= 2003).astype(int)
        dfc["isis_era"] = ((dfc["year"] >= 2013) & (dfc["year"] <= 2019)).astype(int)

        # Data quality
        logging.info("[DATA QUALITY] NaN sebelum cleaning: %d", dfc.isna().sum().sum())
        dfc = dfc.replace([np.inf, -np.inf], np.nan)
        dfc = dfc.fillna(method="ffill").fillna(method="bfill")
        dfc = dfc.dropna()
        logging.info("[DATA QUALITY] NaN setelah cleaning: %d, rows: %d", dfc.isna().sum().sum(), len(dfc))

        if len(dfc) < 10:
            logging.info(f"[SKIP] {country} - data tersisa kurang dari 10 tahun setelah feature engineering.")
            continue

        feature_cols = [
            "attacks_lag1","attacks_lag2","attacks_lag3","attacks_lag5",
            "mean_attacks_3y","mean_attacks_5y","std_attacks_3y","std_attacks_5y",
            "trend_2y","trend_3y",
            "year_scaled","year_sin","year_cos",
            "post_911","post_invasions","isis_era"
        ]

        # ==================================================================
        # TARGET & TRAIN/TEST SPLIT MIRIP: TRAIN ≤ 2004, TEST > 2004
        # Tapi di sini threshold mengikuti negara tsb (misal train sampai 80% terakhir)
        # ==================================================================
        y_original = dfc["annual_attacks"].values.astype(float)
        y = np.log1p(y_original)  # log(1 + y) seperti script VS Code
        X = dfc[feature_cols]
        years = dfc["year"].values.astype(int)

        # Untuk konsistensi, kalau negara punya tahun 1970–2020:
        # Train: sampai 80% pertama, Test: 20% terakhir (time-based)
        n = len(dfc)
        train_size = int(n * 0.8)
        X_train = X.iloc[:train_size]
        y_train = y[:train_size]
        X_test = X.iloc[train_size:]
        y_test = y[train_size:]
        train_years = years[:train_size]
        test_years = years[train_size:]
        y_train_orig = y_original[:train_size]
        y_test_orig = y_original[train_size:]

        logging.info(f"Train years: {train_years.min()}–{train_years.max()} ({len(train_years)} tahun)")
        logging.info(f"Test years:  {test_years.min()}–{test_years.max()} ({len(test_years)} tahun)")

        if len(test_years) < 2:
            logging.info(f"[SKIP] {country} - test set terlalu pendek.")
            continue

        # ==================================================================
        # TRAIN XGBOOST
        # ==================================================================
        params = dict(
            objective="reg:squarederror",
            max_depth=4,
            learning_rate=0.05,
            n_estimators=300,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=3,
            gamma=1.0,
            random_state=42,
        )

        model = XGBRegressor(**params)
        model.fit(X_train, y_train)
        logging.info("✓ Model trained for %s", country)

        # ==================================================================
        # EVALUATE (TRAIN & TEST)
        # ==================================================================
        y_train_pred_log = model.predict(X_train)
        y_test_pred_log = model.predict(X_test)

        y_train_pred = np.expm1(y_train_pred_log)
        y_test_pred = np.expm1(y_test_pred_log)

        y_train_pred = np.maximum(y_train_pred, 0)
        y_test_pred = np.maximum(y_test_pred, 0)

        def eval_metrics(y_true, y_pred, name):
            mae = mean_absolute_error(y_true, y_pred)
            rmse = np.sqrt(mean_squared_error(y_true, y_pred))
            mape = np.mean(np.abs((y_true - y_pred) / (y_true + 1))) * 100.0
            r2 = r2_score(y_true, y_pred)
            logging.info(
                f"[{name}] MAE: {mae:.2f} | RMSE: {rmse:.2f} | MAPE: {mape:.2f}% | R²: {r2:.4f}"
            )
            return mae, rmse, mape, r2

        _, _, mape_train, _ = eval_metrics(y_train_orig, y_train_pred, "TRAIN")
        _, _, mape_test, r2_test = eval_metrics(y_test_orig, y_test_pred, "TEST")

        accuracy = max(0.0, 100.0 - mape_test)
        global_accuracies.append(accuracy)

        logging.info(
            f"[EVAL][{country}] Test Accuracy ~ {accuracy:.1f}% (MAPE: {mape_test:.1f}%, R²: {r2_test:.3f})"
        )

        # ==================================================================
        # PREDICT NEXT YEAR (FINAL FORECAST)
        # ==================================================================
        last_year = int(dfc["year"].max())
        next_year = last_year + 1

        # train final model on all dfc
        model_final = XGBRegressor(**params)
        model_final.fit(X, y)

        next_features = dfc.iloc[-1:].copy()
        next_features["year"] = next_year
        next_features["year_scaled"] = (next_year - dfc["year"].min()) / (dfc["year"].max() - dfc["year"].min())
        next_features["year_sin"] = np.sin(2 * np.pi * next_features["year_scaled"])
        next_features["year_cos"] = np.cos(2 * np.pi * next_features["year_scaled"])
        next_features["post_911"] = 1 if next_year >= 2001 else 0
        next_features["post_invasions"] = 1 if next_year >= 2003 else 0
        next_features["isis_era"] = 1 if (2013 <= next_year <= 2019) else 0

        X_next = next_features[feature_cols]
        next_pred_log = model_final.predict(X_next)[0]
        next_pred = float(np.expm1(next_pred_log))
        if next_pred < 0:
            next_pred = 0.0

        # risk_score simpel: clamp ke 0–100
        risk_score = next_pred
        if risk_score < 0:
            risk_score = 0.0
        if risk_score > 100:
            risk_score = 100.0

        final_predictions.append({
            "country_name": country,
            "prediction_year": int(next_year),
            "predicted_attacks": round(next_pred, 2),
            "risk_score": round(risk_score, 2),
            "model_accuracy": round(accuracy, 2),
        })

    # ======================================================================
    # GLOBAL SUMMARY & SAVE
    # ======================================================================
    if global_accuracies:
        avg_acc = np.mean(global_accuracies)
        logging.info("================================================")
        logging.info(f"RATA-RATA AKURASI TEST (XGBoost, per-country): {avg_acc:.2f}%")
        logging.info("================================================")
    else:
        logging.warning("Tidak ada model dengan test set yang valid.")

    if final_predictions:
        pred_df = pd.DataFrame(final_predictions)
        engine = create_engine(DB_CONN)
        pred_df.to_sql(TARGET_TABLE, engine, if_exists="replace", index=False)
        logging.info(f"Predictions saved to table '{TARGET_TABLE}'.")
    else:
        logging.warning("No predictions generated; nothing saved to DB.")


if __name__ == "__main__":
    run_risk_prediction_xgboost_yearly()
