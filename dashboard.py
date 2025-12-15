import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px

# ------------------------------------------------------------------
# 1. KONFIGURASI HALAMAN & KONEKSI
# ------------------------------------------------------------------
st.set_page_config(
    page_title="GTD Analytics",
    page_icon="🌍",
    layout="wide"
)

st.title("🌍 Global Terrorism Analytics Dashboard")
st.markdown("Dashboard ini menggabungkan **Data Warehouse** (Historis) dan **Machine Learning** (Prediksi).")

# Koneksi Database
DB_CONN = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"

@st.cache_data(ttl=600)
def load_data():
    engine = create_engine(DB_CONN)
    
    # A. TREND TAHUNAN
    sql_trend = """
    SELECT 
        d.year,
        COUNT(f.incident_count) as total_attacks,
        SUM(f.killed) as total_killed,
        SUM(f.wounded) as total_wounded
    FROM public_warehouse.fact_attacks f
    JOIN public_warehouse.dim_date d ON f.date_id = d.date_id
    GROUP BY d.year
    ORDER BY d.year;
    """
    
    # B. PETA SEBARAN
    sql_map = """
    SELECT 
        l.latitude,
        l.longitude,
        l.country_name,
        l.city_name,
        f.killed
    FROM public_warehouse.fact_attacks f
    JOIN public_warehouse.dim_location l ON f.location_id = l.location_id
    JOIN public_warehouse.dim_date d ON f.date_id = d.date_id
    WHERE l.latitude IS NOT NULL 
    ORDER BY d.year DESC, d.month DESC
    LIMIT 2000;
    """
    
    # C. DATA MART
    sql_mart = """
    SELECT 
        country_name, 
        year, 
        investment_signal, 
        property_index,    
        total_attacks      
    FROM public.mart_risk_analysis 
    WHERE year = (SELECT MAX(year) FROM public.mart_risk_analysis)
    ORDER BY property_index DESC;
    """

    # D. PREDIKSI ML (Hasil Battle)
    sql_pred = """
    SELECT * FROM public.investment_risk_predictions 
    ORDER BY risk_score DESC;
    """
    
    try:
        df_trend = pd.read_sql(sql_trend, engine)
        df_map = pd.read_sql(sql_map, engine)
        df_mart = pd.read_sql(sql_mart, engine)
        
        try:
            df_pred = pd.read_sql(sql_pred, engine)
        except:
            df_pred = pd.DataFrame()
            
        return df_trend, df_map, df_mart, df_pred
    except Exception as e:
        st.error(f"Error Database: {e}")
        return None, None, None, None

# Load Data
df_trend, df_map, df_mart, df_pred = load_data()

if df_trend is None:
    st.stop()

# ------------------------------------------------------------------
# 2. KPI GLOBAL
# ------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

total_attacks = df_trend['total_attacks'].sum()
total_killed = df_trend['total_killed'].sum()
most_dangerous_year = df_trend.loc[df_trend['total_attacks'].idxmax()]['year']

if not df_pred.empty:
    safest = df_pred.iloc[-1]['country_name']
else:
    safest = "N/A"

with col1: st.metric("Total Serangan (Historis)", f"{total_attacks:,.0f}")
with col2: st.metric("Total Korban Jiwa", f"{total_killed:,.0f}")
with col3: st.metric("Tahun Paling Berbahaya", str(int(most_dangerous_year)))
with col4: st.metric("Negara Teraman (Forecast)", safest)

st.divider()

# ------------------------------------------------------------------
# 3. TABS NAVIGATION
# ------------------------------------------------------------------
tab_overview, tab_forecast = st.tabs(["📊 Overview & Historical", "🤖 AI Forecast & Comparison"])

# ==================================================================
# TAB 1: OVERVIEW & HISTORICAL
# ==================================================================
with tab_overview:
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("📈 Tren Serangan Global")
        subtab1, subtab2 = st.tabs(["Jumlah Serangan", "Jumlah Korban"])
        with subtab1:
            fig_attacks = px.line(df_trend, x='year', y='total_attacks', markers=True, line_shape='spline')
            fig_attacks.update_traces(line_color='#FF4B4B')
            st.plotly_chart(fig_attacks, use_container_width=True)
        with subtab2:
            fig_killed = px.bar(df_trend, x='year', y='total_killed')
            fig_killed.update_traces(marker_color='#333333')
            st.plotly_chart(fig_killed, use_container_width=True)

    with col_right:
        st.subheader("💡 Sinyal Investasi (Mart)")
        if not df_mart.empty:
            latest_year = int(df_mart['year'].iloc[0])
            st.caption(f"Gabungan Data Ekonomi & GTD ({latest_year})")
            st.dataframe(
                df_mart[['country_name', 'investment_signal', 'property_index']],
                column_config={
                    "investment_signal": "Sinyal",
                    "property_index": st.column_config.NumberColumn("Idx Properti", format="%.1f"),
                },
                hide_index=True, use_container_width=True, height=400
            )
        else:
            st.warning("Data Mart kosong.")

    st.subheader("🗺️ Peta Lokasi Serangan (Live Data)")
    st.map(df_map, latitude='latitude', longitude='longitude', size='killed', color='#ff0000')

# ==================================================================
# TAB 2: AI FORECAST & COMPARISON
# ==================================================================
with tab_forecast:
    if df_pred.empty:
        st.warning("⚠️ Belum ada data prediksi. Pastikan pipeline ML (risk_model.py) sudah dijalankan.")
    else:
        # Cek apakah kolom akurasi tersedia (untuk kompatibilitas)
        cols = df_pred.columns
        has_metrics = 'xgb_accuracy' in cols and 'rf_accuracy' in cols
        
        pred_year = int(df_pred['prediction_year'].iloc[0])
        st.header(f"🤖 AI Battle: XGBoost vs Random Forest ({pred_year})")
        
        # --- HITUNG METRICS ---
        if has_metrics:
            avg_acc_xgb = df_pred['xgb_accuracy'].mean()
            avg_acc_rf = df_pred['rf_accuracy'].mean()
            st.caption(f"Perbandingan akurasi rata-rata (MAPE based) dari {len(df_pred)} negara.")
        else:
            avg_acc_xgb = 0
            avg_acc_rf = 0
            st.caption("Menampilkan data prediksi.")

        xgb_wins = df_pred[df_pred['model_used'] == 'XGBoost']
        rf_wins = df_pred[df_pred['model_used'] == 'Random Forest']
        
        # --- TAMPILKAN METRICS ---
        m1, m2, m3, m4 = st.columns(4)
        
        with m1: 
            st.metric("Avg Akurasi XGBoost", f"{avg_acc_xgb:.1f}%", 
                     delta=f"{avg_acc_xgb - avg_acc_rf:.1f}%" if has_metrics else None)
        with m2: 
            st.metric("Avg Akurasi Random Forest", f"{avg_acc_rf:.1f}%")
        with m3: 
            st.metric("Dominasi XGBoost", f"{len(xgb_wins)} Negara")
        with m4: 
            st.metric("Dominasi Random Forest", f"{len(rf_wins)} Negara")
            
        st.divider()

        # --- VISUALISASI ---
        c1, c2 = st.columns([2, 1])
        
        with c1:
            st.subheader("🏆 Proporsi Kemenangan Model")
            fig_pie = px.pie(
                df_pred, 
                names='model_used', 
                color='model_used',
                color_discrete_map={'XGBoost':'#00CC96', 'Random Forest':'#EF553B'},
                hole=0.4
            )
            st.plotly_chart(fig_pie, use_container_width=True)

            st.subheader("⚠️ Top 10 Negara Paling Berisiko")
            top_risk = df_pred.sort_values(by='risk_score', ascending=False).head(10)
            
            fig_bar = px.bar(
                top_risk,
                x='risk_score',
                y='country_name',
                color='model_used',
                orientation='h',
                text='predicted_attacks',
                color_discrete_map={'XGBoost':'#00CC96', 'Random Forest':'#EF553B'},
                title="Risk Score Tertinggi & Model Prediksinya"
            )
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar, use_container_width=True)

        with c2:
            st.subheader("📋 Detail Prediksi & Akurasi")
            
            # Pilih kolom yang mau ditampilkan
            show_cols = ['country_name', 'risk_score', 'predicted_attacks', 'model_used']
            if 'winner_accuracy' in cols:
                show_cols.append('winner_accuracy')
            elif 'model_accuracy' in cols:
                show_cols.append('model_accuracy') # Fallback ke nama lama jika perlu

            st.dataframe(
                df_pred[show_cols],
                column_config={
                    "risk_score": st.column_config.ProgressColumn("Risk Score", min_value=0, max_value=100, format="%d"),
                    "predicted_attacks": st.column_config.NumberColumn("Prediksi Atk", format="%.1f"),
                    "winner_accuracy": st.column_config.NumberColumn("Akurasi (Winner)", format="%.1f%%"),
                    "model_accuracy": st.column_config.NumberColumn("Akurasi", format="%.1f%%")
                },
                hide_index=True,
                height=600,
                use_container_width=True
            )