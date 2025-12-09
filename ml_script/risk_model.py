import pandas as pd
from sqlalchemy import create_engine
from sklearn.ensemble import RandomForestRegressor

# Connect using the Airflow user
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

def run_risk_prediction():
    print("Starting ML Model...")
    engine = create_engine(DB_CONN)
    
    # 2. Fetch Data
    query = "SELECT country_name, year, count(event_id) as attack_count FROM public_staging.stg_attacks GROUP BY country_name, year"
    
    print("Fetching data...")
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("No data found!")
        return

    # 3. Train & Predict
    print(f"Training on {len(df)} rows...")
    final_predictions = []
    countries = df['country_name'].unique()
    
    for country in countries:
        c_data = df[df['country_name'] == country]
        if len(c_data) < 2: continue
        
        # Train Model
        model = RandomForestRegressor(n_estimators=10, random_state=42)
        model.fit(c_data[['year']], c_data['attack_count'])
        
        # Predict 2020 Risk
        pred = model.predict([[2020]])[0]
        
        final_predictions.append({
            'country_name': country,
            'risk_score': round(pred, 2)
        })
        
    # 4. Save
    pd.DataFrame(final_predictions).to_sql('investment_risk_predictions', engine, if_exists='replace', index=False)
    print("Success! Predictions saved.")

if __name__ == "__main__":
    run_risk_prediction()