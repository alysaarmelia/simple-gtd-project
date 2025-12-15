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

# Judul Dashboard
st.title("🌍 Global Terrorism Analytics Dashboard")
st.markdown("Dashboard ini terhubung langsung ke **Data Warehouse (PostgreSQL)** Anda.")

# Koneksi Database
# Kita gunakan 'localhost' karena script ini jalan di Windows, 
# tapi database ada di Docker yang port 5432-nya sudah dibuka.
DB_CONN = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"

@st.cache_data(ttl=600) # Cache data selama 10 menit agar tidak berat
def load_data():
    engine = create_engine(DB_CONN)
    
    # A. QUERY TREN TAHUNAN (Fact + Dim Date)
    # Kita join Fact Table dengan Dimensi Waktu untuk agregasi per tahun
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
    
    # B. QUERY PETA SEBARAN (Fact + Dim Location)
    # Mengambil data serangan yang memiliki koordinat GPS valid
    # Limit 2000 data terbaru agar peta tidak lag
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
   # C. QUERY ANALISIS RISIKO (Dari Data Mart: Fact Economy + Fact Attacks)
    sql_ml = """
    SELECT 
        country_name, 
        year, 
        investment_signal, -- Kolom baru hasil logika dbt
        property_index,    -- Data ekonomi dari OECD
        total_attacks      -- Data keamanan dari GTD
    FROM public.mart_risk_analysis 
    WHERE year = (SELECT MAX(year) FROM public.mart_risk_analysis) -- Ambil tahun terakhir saja
    ORDER BY property_index DESC;
    """
    
    try:
        df_trend = pd.read_sql(sql_trend, engine)
        df_map = pd.read_sql(sql_map, engine)
        df_ml = pd.read_sql(sql_ml, engine)
        return df_trend, df_map, df_ml
    except Exception as e:
        return None, None, None

# Load Data
df_trend, df_map, df_ml = load_data()

# Error Handling jika Docker mati
if df_trend is None:
    st.error("❌ Gagal terhubung ke Database!")
    st.info("Pastikan Docker menyala (`docker-compose up`) dan port 5432 terbuka.")
    st.stop()

# ------------------------------------------------------------------
# 2. BAGIAN KPI (KEY PERFORMANCE INDICATORS)
# ------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

total_attacks = df_trend['total_attacks'].sum()
total_killed = df_trend['total_killed'].sum()
most_dangerous_year = df_trend.loc[df_trend['total_attacks'].idxmax()]['year']
safest_country = df_ml.iloc[-1]['country_name'] if not df_ml.empty else "N/A"

with col1:
    st.metric("Total Serangan", f"{total_attacks:,.0f}")
with col2:
    st.metric("Total Korban Jiwa", f"{total_killed:,.0f}")
with col3:
    st.metric("Tahun Paling Berbahaya", str(int(most_dangerous_year)))
with col4:
    st.metric("Negara Teraman (Prediksi)", safest_country)

st.divider()

# ------------------------------------------------------------------
# 3. VISUALISASI GRAFIK & TABEL
# ------------------------------------------------------------------
col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("📈 Tren Serangan Global")
    tab1, tab2 = st.tabs(["Jumlah Serangan", "Jumlah Korban"])
    
    with tab1:
        fig_attacks = px.line(df_trend, x='year', y='total_attacks', 
                              markers=True, line_shape='spline',
                              title='Evolusi Jumlah Serangan per Tahun')
        fig_attacks.update_traces(line_color='#FF4B4B')
        st.plotly_chart(fig_attacks, use_container_width=True)
        
    with tab2:
        fig_killed = px.bar(df_trend, x='year', y='total_killed',
                            title='Jumlah Korban Jiwa per Tahun')
        fig_killed.update_traces(marker_color='#333333')
        st.plotly_chart(fig_killed, use_container_width=True)

with col_right:
    st.subheader("💡 Sinyal Investasi (Data Mart)")
    
    if not df_ml.empty:
        latest_year = int(df_ml['year'].iloc[0])
        st.caption(f"Analisis Gabungan Keamanan & Ekonomi (Tahun {latest_year})")
        
        # Menampilkan tabel Mart
        st.dataframe(
            df_ml[['country_name', 'investment_signal', 'property_index', 'total_attacks']],
            column_config={
                "investment_signal": "Rekomendasi",
                "property_index": st.column_config.NumberColumn("Indeks Properti", format="%.2f"),
                "total_attacks": "Jml Serangan"
            },
            hide_index=True,
            use_container_width=True,
            height=400
        )
    else:
        st.warning("Belum ada data di Mart Risk Analysis.")

# ------------------------------------------------------------------
# 4. PETA INTERAKTIF
# ------------------------------------------------------------------
st.subheader("🗺️ Peta Lokasi Serangan (Live Data)")
st.map(df_map, latitude='latitude', longitude='longitude', size='killed', color='#ff0000')  