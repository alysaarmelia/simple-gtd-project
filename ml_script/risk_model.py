import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import logging
import warnings

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO)

DB_CONN = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"
TARGET_TABLE = "investment_risk_predictions"


def run_risk_prediction_comparison():
    logging.info("=" * 80)
    logging.info("🤖 AI BATTLE OPTIMIZED: XGBoost vs Random Forest")
    logging.info("=" * 80)

    try:
        engine = create_engine(DB_CONN)

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

        countries = df["country_name"].unique()
        final_predictions = []
        
        wins_xgb = 0
        wins_rf = 0

        logging.info(f"Starting optimized comparison for {len(countries)} countries...")

        for country in countries:
            c_data = df[df["country_name"] == country].copy()
            
            if len(c_data) < 10: continue

            c_data = c_data.sort_values("year")
            
            dfc = c_data.rename(columns={"attack_count": "annual_attacks"}).copy()

            dfc["annual_attacks"] = dfc["annual_attacks"].astype(float)
            dfc["year"] = dfc["year"].astype(int)
            
            dfc["attacks_lag1"] = dfc["annual_attacks"].shift(1)
            dfc["attacks_lag2"] = dfc["annual_attacks"].shift(2)
            dfc["attacks_lag3"] = dfc["annual_attacks"].shift(3)
            
            dfc["mean_3y"] = dfc["annual_attacks"].rolling(3).mean()
            dfc["std_3y"] = dfc["annual_attacks"].rolling(3).std()
            
            dfc["trend_1y"] = dfc["annual_attacks"].shift(1) - dfc["annual_attacks"].shift(2)
            
            dfc["year_scaled"] = (dfc["year"] - dfc["year"].min()) / (dfc["year"].max() - dfc["year"].min())

            dfc = dfc.dropna()
            if len(dfc) < 10: continue

            features = ["attacks_lag1", "attacks_lag2", "attacks_lag3", "mean_3y", "std_3y", "trend_1y", "year_scaled"]
            
            n = len(dfc)
            train_size = int(n * 0.8)
            
            X = dfc[features]
            y = np.log1p(dfc["annual_attacks"].values) 
            
            X_train, X_test = X.iloc[:train_size], X.iloc[train_size:]
            y_train, y_test = y[:train_size], y[train_size:]
            
            if len(X_test) < 1: continue

            y_test_real = np.expm1(y_test)

            model_xgb = XGBRegressor(
                n_estimators=1000,      
                learning_rate=0.01,     
                max_depth=3,            
                subsample=0.7,          
                colsample_bytree=0.7,   
                random_state=42,
                n_jobs=-1
            )
            model_xgb.fit(X_train, y_train)
            pred_xgb = np.expm1(model_xgb.predict(X_test))
            
            mape_xgb = np.mean(np.abs((y_test_real - pred_xgb) / (y_test_real + 1))) * 100
            acc_xgb = max(0, 100 - mape_xgb)

            model_rf = RandomForestRegressor(
                n_estimators=500,       
                max_depth=5,            
                min_samples_leaf=2,     
                random_state=42,
                n_jobs=-1
            )
            model_rf.fit(X_train, y_train)
            pred_rf = np.expm1(model_rf.predict(X_test))

            mape_rf = np.mean(np.abs((y_test_real - pred_rf) / (y_test_real + 1))) * 100
            acc_rf = max(0, 100 - mape_rf)

            if acc_xgb >= acc_rf:
                winner_model = "XGBoost"
                winner_acc = acc_xgb
                final_model = model_xgb
                wins_xgb += 1
            else:
                winner_model = "Random Forest"
                winner_acc = acc_rf
                final_model = model_rf
                wins_rf += 1

            final_model.fit(X, y)
            
            next_year = int(dfc["year"].max()) + 1
            last_row = dfc.iloc[-1:].copy()
            
            next_feats_dict = {
                "attacks_lag1": last_row["annual_attacks"],         
                "attacks_lag2": last_row["attacks_lag1"],
                "attacks_lag3": last_row["attacks_lag2"],
                "mean_3y": (last_row["annual_attacks"] + last_row["attacks_lag1"] + last_row["attacks_lag2"]) / 3,
                "std_3y": np.std([last_row["annual_attacks"], last_row["attacks_lag1"], last_row["attacks_lag2"]]),
                "trend_1y": last_row["annual_attacks"] - last_row["attacks_lag1"],
                "year_scaled": (next_year - c_data["year"].min()) / (c_data["year"].max() - c_data["year"].min())
            }
            next_feats = pd.DataFrame([next_feats_dict])
            
            next_feats = next_feats[features].astype(float)
            
            pred_log = final_model.predict(next_feats)[0]
            pred_val = float(np.expm1(pred_log))
            
            risk_score = min(max(pred_val, 0), 100)
            if pred_val > 50: risk_score = 100

            final_predictions.append({
                "country_name": country,
                "prediction_year": next_year,
                "predicted_attacks": round(pred_val, 2),
                "risk_score": round(risk_score, 2),
                "xgb_accuracy": round(acc_xgb, 2), 
                "rf_accuracy": round(acc_rf, 2),
                "winner_accuracy": round(winner_acc, 2),
                "model_used": winner_model
            })

        if final_predictions:
            df_save = pd.DataFrame(final_predictions)
            
            avg_xgb = df_save['xgb_accuracy'].mean()
            avg_rf = df_save['rf_accuracy'].mean()

            logging.info("=" * 60)
            logging.info(f"🏆 FINAL BATTLE SCOREBOARD (OPTIMIZED)")
            logging.info(f"   XGBoost Wins      : {wins_xgb}")
            logging.info(f"   Random Forest Wins: {wins_rf}")
            logging.info("-" * 60)
            logging.info(f"📊 GLOBAL AVERAGE ACCURACY")
            logging.info(f"   XGBoost       : {avg_xgb:.2f}%")
            logging.info(f"   Random Forest : {avg_rf:.2f}%")
            logging.info("=" * 60)
            
            df_save.to_sql(TARGET_TABLE, engine, if_exists="replace", index=False)
            logging.info(f"✅ Success! Predictions saved to '{TARGET_TABLE}'.")
        else:
            logging.warning("⚠️ No predictions generated.")

    except Exception as e:
        logging.error(f"❌ Error in pipeline: {e}")

if __name__ == "__main__":
    run_risk_prediction_comparison()