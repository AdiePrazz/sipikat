import streamlit as st
import pandas as pd
import re
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(
    page_title="Infografis PDPB KPU Malang",
    page_icon="📊",
    layout="wide"
)

db_url = st.secrets["db_pdpb"]["url"]
engine = create_engine(db_url)

# host = "localhost"
# user = "postgres"
# password = "admin"
# database = "db_pdpb"
# port = 5432
# db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
# engine = create_engine(db_url)

# inisiasi session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'df_pdpb' not in st.session_state:
    st.session_state.df_pdpb = None
if 'df_pdpb_before' not in st.session_state:
    st.session_state.df_pdpb_before = None
if 'df_model_a' not in st.session_state:
    st.session_state.df_model_a = None
if 'df_db_rekap' not in st.session_state:
    st.session_state.df_db_rekap = None
if 'selected_id' not in st.session_state:
    st.session_state.selected_id = None
if 'selected_label' not in st.session_state:
    st.session_state.selected_label = None

if 'view_mode' not in st.session_state:
    st.session_state.view_mode = 'home'
    
def simpan_ke_database(engine, df_pdpb_clean, df_triwulan_sebelumnya, df_rekap_model_a, df_db_rekap_model_a, df_disabilitas_clean, id_triwulan):
    with engine.begin() as conn:
        # --- 1️⃣ Simpan ke tabel rekapitulasi_pdpb ---
        existing = pd.read_sql(
            text("SELECT id_triwulan FROM rekapitulasi_pdpb WHERE id_triwulan = :id"),
            conn, params={"id": id_triwulan}
        )
        if existing.empty:
            df_pdpb_clean.to_sql('rekapitulasi_pdpb', conn, if_exists='append', index=False)
            st.success("✅ Data rekapitulasi PDPB berhasil disimpan.")
        else:
            st.warning("⚠️ Data untuk triwulan ini sudah ada di tabel rekapitulasi_pdpb — tidak disimpan ulang.")

        # --- 2️⃣ Simpan ke tabel triwulan_sebelumnya ---
        existing = pd.read_sql(
            text("SELECT id_triwulan FROM triwulan_sebelumnya WHERE id_triwulan = :id"),
            conn, params={"id": id_triwulan}
        )
        
        if existing.empty:
            df_pdpb_t_clean.to_sql('triwulan_sebelumnya', conn, if_exists='append', index=False)
            st.toast(f"✅ {len(df_pdpb_t_clean)} baris data disimpan")
        else:
            st.warning("⚠️ Data sudah ada — tidak disimpan ulang")
            
        if existing.empty:
            df_triwulan_sebelumnya.to_sql('triwulan_sebelumnya', conn, if_exists='append', index=False)
            st.success("✅ Data triwulan sebelumnya berhasil disimpan.")
        else:
            st.warning("⚠️ Data triwulan sebelumnya untuk periode ini sudah ada — tidak disimpan ulang.")

        # --- 3️⃣ Simpan ke tabel rekap_model_a ---
        existing = pd.read_sql(
            text("SELECT id_triwulan FROM rekap_model_a WHERE id_triwulan = :id"),
            conn, params={"id": id_triwulan}
        )
        if existing.empty:
            df_rekap_model_a.to_sql('rekap_model_a', conn, if_exists='append', index=False)
            st.success("✅ Data rekap Model A berhasil disimpan.")
        else:
            st.warning("⚠️ Data rekap Model A untuk triwulan ini sudah ada — tidak disimpan ulang.")

        # --- 4️⃣ Simpan ke tabel db_rekap_model_a ---
        existing = pd.read_sql(
            text("SELECT id_triwulan FROM db_rekap_model_a WHERE id_triwulan = :id"),
            conn, params={"id": id_triwulan}
        )
        if existing.empty:
            df_db_rekap_model_a.to_sql('db_rekap_model_a', conn, if_exists='append', index=False)
            st.success("✅ Data DB Rekap Model A berhasil disimpan.")
        else:
            st.warning("⚠️ Data DB Rekap Model A untuk triwulan ini sudah ada — tidak disimpan ulang.")
        
        # --- 5️⃣ Simpan ke tabel detail_disabilitas ---
        if df_disabilitas_clean is not None and not df_disabilitas_clean.empty:
            existing = pd.read_sql(
                text("SELECT id_triwulan FROM detail_disabilitas WHERE id_triwulan = :id"),
                conn, params={"id": id_triwulan}
            )
            if existing.empty:
                df_disabilitas_clean['id_triwulan'] = id_triwulan
                df_disabilitas_clean.to_sql('detail_disabilitas', conn, if_exists='append', index=False)
                st.success("✅ Data disabilitas berhasil disimpan.")
            else:
                st.warning("⚠️ Data disabilitas sudah ada — tidak disimpan ulang.")
            
def extract_triwulan_info(uploaded_file):
    try:
        # Baca baris atas dari sheet REKAPITULASI PDPB
        header_df = pd.read_excel(
            uploaded_file, 
            sheet_name='REKAPITULASI PDPB',
            header=None, 
            nrows=8,  # cukup untuk menangkap header dan judul
            engine='openpyxl'
        )
    except Exception as e:
        st.error(f"Gagal membaca header file Excel. Error: {e}")
        return None

    triwulan_text = None
    for row in header_df.values:
        for cell in row:
            if isinstance(cell, str) and "TRIWULAN" in cell.upper():
                triwulan_text = cell.strip()
                break
        if triwulan_text:
            break

    if not triwulan_text:
        st.warning("Tidak ditemukan teks yang mengandung kata 'TRIWULAN' di baris header file ini.")
        return None

    # regex
    match = re.search(r'TRIWULAN\s+KE?(\w+)\s+TAHUN\s+(\d{4})', triwulan_text, re.IGNORECASE)
    if not match:
        st.warning(f"Format data Triwulan/Tahun tidak dikenali: '{triwulan_text}'")
        return None

    triwulan_kata = match.group(1).lower()
    tahun = int(match.group(2))
    
    triwulan_map = {
        'satu': 1, 'kesatu': 1,
        'dua': 2, 'kedua': 2,
        'tiga': 3, 'ketiga': 3,
        'empat': 4, 'keempat': 4
    }
    triwulan_ke = next((val for key, val in triwulan_map.items() if key in triwulan_kata), 0)
    
    if triwulan_ke == 0:
        st.warning(f"Gagal mengkonversi Triwulan dari '{triwulan_kata}' menjadi angka.")
        return None

    return {
        'judul': triwulan_text,
        'tahun': tahun,
        'triwulan_ke': triwulan_ke
    }

def insert_or_get_triwulan_id(engine, triwulan_data):
    try:
        # Menggunakan engine.begin() untuk mengelola transaksi secara otomatis (commit/rollback)
        with engine.begin() as connection: 
            check_query = text("""
                SELECT id_triwulan FROM triwulan 
                WHERE tahun = :tahun AND triwulan_ke = :triwulan_ke AND judul = :judul
            """)
            result = connection.execute(check_query, triwulan_data).fetchone()

            if result:
                # st.info(f"🗓️ Data Triwulan/Tahun **T{triwulan_data['triwulan_ke']} {triwulan_data['tahun']}** sudah ada (ID: {result[0]}).")
                return result[0]
            else:
                # Jika belum ada, lakukan INSERT dan ambil ID yang baru dibuat
                insert_query = text("""
                    INSERT INTO triwulan (judul, tahun, triwulan_ke)
                    VALUES (:judul, :tahun, :triwulan_ke)
                    RETURNING id_triwulan
                """)
                result = connection.execute(insert_query, triwulan_data).fetchone()
                st.success(f"➕ Data Triwulan/Tahun **T{triwulan_data['triwulan_ke']} {triwulan_data['tahun']}** berhasil dimasukkan (ID: {result[0]}).")
                return result[0]
                
    except SQLAlchemyError as e:
        st.error(f"❌ Kesalahan Database saat memproses Triwulan: {e}")
        return None
    
def clean_and_map_rekapitulasi_pdpb(df):
    df = df[~df['No.'].astype(str).str.contains('JUMLAH', na=False, case=False)]
    column_mapping = {
        'Nama Kecamatan': 'nama_kecamatan',
        'Jumlah Desa/Kel': 'jumlah_desa_kel',
        'L': 'jumlah_pemilih_laki',
        'P': 'jumlah_pemilih_perempuan',
        'L + P': 'total_pemilih',
        'Keterangan': 'keterangan'
    }
    df.rename(columns=column_mapping, inplace=True)
    final_columns = list(column_mapping.values())
    df_final = df[[col for col in final_columns if col in df.columns]]
    df['No.'] = df.index + 1
    return df_final.dropna(subset=['nama_kecamatan'])

def clean_and_map_rekap_model_a(df):
    df = df[~df['No.'].astype(str).str.contains('JUMLAH', na=False, case=False)]
    column_mapping = {
        'Nama Kecamatan': 'nama_kecamatan',
        'Jumlah Desa/Kel': 'jumlah_desa_kel',
        'Jumlah Pemilih Baru': 'jumlah_pemilih_baru',
        'Jumlah Pemilih Tidak Memenuhi Syarat': 'jumlah_pemilih_tms',
        'Jumlah Perbaikan Data Pemilih': 'jumlah_perbaikan_data',
        'Keterangan': 'keterangan'
    }
    df.rename(columns=column_mapping, inplace=True)
    final_columns = list(column_mapping.values())
    df_final = df[[col for col in final_columns if col in df.columns]]
    return df_final.dropna(subset=['nama_kecamatan'])

def clean_and_map_db_rekap_model_a(df):
    df.columns = [
        '_'.join([str(c).strip() for c in col if 'Unnamed' not in str(c)]).lower()
        for col in df.columns.values
    ]
    df.columns = [re.sub(r'[^a-z0-9_]', '', c).replace('__', '_').strip('_') for c in df.columns]

    # Hapus baris total JUMLAH
    df = df[~df['no'].astype(str).str.contains('jumlah', na=False, case=False)]

    mapping = {
    'nama_kecamatan': ['namakecamatan', 'nama_kecamatan'],
    
    'pemilih_baru_l': ['jumlahpemilihbaru_l'],
    'pemilih_baru_p': ['jumlahpemilihbaru_p'],

    'tms_meninggal_l': ['jumlahpemilihtidakmemenuhisyarat_meninggal_l'],
    'tms_meninggal_p': ['jumlahpemilihtidakmemenuhisyarat_meninggal_p'],

    'tms_dibawah_umur_l': ['jumlahpemilihtidakmemenuhisyarat_dibawahumur_l'],
    'tms_dibawah_umur_p': ['jumlahpemilihtidakmemenuhisyarat_dibawahumur_p'],

    'tms_ganda_l': ['jumlahpemilihtidakmemenuhisyarat_ganda_l'],
    'tms_ganda_p': ['jumlahpemilihtidakmemenuhisyarat_ganda_p'],

    'tms_pindah_keluar_l': ['jumlahpemilihtidakmemenuhisyarat_pindahkeluar_l'],
    'tms_pindah_keluar_p': ['jumlahpemilihtidakmemenuhisyarat_pindahkeluar_p'],

    'tms_tni_l': ['jumlahpemilihtidakmemenuhisyarat_tni_l'],
    'tms_tni_p': ['jumlahpemilihtidakmemenuhisyarat_tni_p']
    }

    final_df = pd.DataFrame()
    for target, possible_names in mapping.items():
        for name in possible_names:
            match = [c for c in df.columns if re.search(name, c)]
            if match:
                final_df[target] = df[match[0]]
                break
            
    final_df = final_df.dropna(subset=['nama_kecamatan'])
    return final_df

def clean_and_map_pdpb_t2(df):
    # Hapus baris total 'JUMLAH' (berdasarkan kolom pertama)
    df = df[~df.iloc[:, 0].astype(str).str.contains('JUMLAH', na=False, case=False)]
    
    column_mapping = {
        'No.': 'no',
        'MALANG': 'nama_kecamatan',
        'TPS': 'jumlah_tps',
        'LK': 'laki',
        'PR': 'perempuan',
        'L + P': 'total_pemilih'
    }
    df.rename(columns=column_mapping, inplace=True)
    
    final_columns = ['nama_kecamatan', 'jumlah_tps', 'laki', 'perempuan']
    df_final = df[[col for col in final_columns if col in df.columns]]

    return df_final.dropna(subset=['nama_kecamatan'])

def clean_and_map_disabilitas(df):
    """
    Membersihkan dan mapping data disabilitas dari sheet SIDALIH WEB
    """
    # 1. Filter awal pada df
    df = df[~df.iloc[:, 0].astype(str).str.contains('%', na=False)]
    df = df[~df.iloc[:, 0].astype(str).str.match(r'^\d+\.', na=False)]
    
    # 2. Filter keyword
    keywords_to_remove = ['FISIK', 'INTELEKTUAL', 'MENTAL', 'SENSORIK', 'TOTAL', 'RATA']
    for keyword in keywords_to_remove:
        df = df[~df.iloc[:, 0].astype(str).str.contains(keyword, na=False, case=False)]
    
    # 3. Hapus JUMLAH
    df = df[~df.iloc[:, 0].astype(str).str.contains('JUMLAH', na=False, case=False)]
    df = df.dropna(subset=[df.columns[0]])
    
    # 4. Rename kolom
    column_mapping = {
        'Wilayah': 'nama_kecamatan',
        'Fisik': 'disabilitas_fisik',
        'Intelektual': 'disabilitas_intelektual',
        'Mental': 'disabilitas_mental',
        'Sensorik Wicara': 'disabilitas_sensorik_wicara',
        'Sensorik Rungu': 'disabilitas_sensorik_rungu',
        'Sensorik Netra': 'disabilitas_sensorik_netra'
        }
    df_renamed = df.rename(columns=column_mapping)
    
    # 5. Buat df_final
    final_columns = [
        'nama_kecamatan',
        'disabilitas_fisik',
        'disabilitas_intelektual',
        'disabilitas_mental',
        'disabilitas_sensorik_wicara',
        'disabilitas_sensorik_rungu',
        'disabilitas_sensorik_netra'
        ]
    df_final = df_renamed[[col for col in final_columns if col in df_renamed.columns]]
    
    # 6. Convert ke numeric
    # Convert ke numeric
    numeric_cols = [col for col in final_columns if col != 'nama_kecamatan']
    for col in numeric_cols:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(0).astype(int)
    
    # 7. Clean nama kecamatan
    df_final['nama_kecamatan'] = df_final['nama_kecamatan'].str.strip().str.upper()
    
    # ✅ 8. Validasi nama kecamatan - SETELAH df_final DIBUAT!
    df_final = df_final[~df_final['nama_kecamatan'].str.match(r'^\d', na=False)]
    
    # 9. Reset index
    df_final = df_final.reset_index(drop=True)
    
    return df_final

def simpan_disabilitas_ke_database(engine, df_disabilitas_clean, id_triwulan):
    """
    Simpan data disabilitas dengan anti-duplikasi
    """
    with engine.begin() as conn:
        # Cek duplikasi
        existing = pd.read_sql(
            text("SELECT id_triwulan FROM detail_disabilitas WHERE id_triwulan = :id"),
            conn, params={"id": id_triwulan}
        )
        
        if existing.empty:
            # Tambahkan id_triwulan
            df_disabilitas_clean['id_triwulan'] = id_triwulan
            
            # Simpan ke database
            df_disabilitas_clean.to_sql('detail_disabilitas', conn, if_exists='append', index=False)
            st.success("✅ Data disabilitas berhasil disimpan.")
            
            # Summary
            total_disabilitas = (
                df_disabilitas_clean['disabilitas_fisik'].sum() +
                df_disabilitas_clean['disabilitas_intelektual'].sum() +
                df_disabilitas_clean['disabilitas_mental'].sum() +
                df_disabilitas_clean['disabilitas_sensorik_wicara'].sum() +
                df_disabilitas_clean['disabilitas_sensorik_rungu'].sum() +
                df_disabilitas_clean['disabilitas_sensorik_netra'].sum()
            )
            
            st.info(f"📊 Total Pemilih Disabilitas: {int(total_disabilitas):,}")
        else:
            st.warning("⚠️ Data disabilitas sudah ada — tidak disimpan ulang.")
            
def display_home_overview(engine):
    # """
    # Menampilkan overview data tahunan dengan detail per triwulan
    # """
    # st.markdown("### Ringkasan Data Tahunan & Per Triwulan")
    # st.markdown("---")
    
    try:
        with engine.connect() as conn:
            # Data per triwulan
            df_yearly = pd.read_sql(text("""
                SELECT 
                    t.tahun,
                    t.triwulan_ke,
                    t.judul,
                    t.id_triwulan,
                    COUNT(DISTINCT r.nama_kecamatan) as jumlah_kecamatan,
                    SUM(r.jumlah_desa_kel) as total_desa_kel,
                    SUM(r.jumlah_pemilih_laki) as total_laki,
                    SUM(r.jumlah_pemilih_perempuan) as total_perempuan,
                    SUM(r.total_pemilih) as total_pemilih
                FROM triwulan t
                LEFT JOIN rekapitulasi_pdpb r ON t.id_triwulan = r.id_triwulan
                GROUP BY t.tahun, t.triwulan_ke, t.judul, t.id_triwulan
                ORDER BY t.tahun DESC, t.triwulan_ke DESC
            """), conn)
            
            # Data TMS per triwulan
            df_tms = pd.read_sql(text("""
                SELECT 
                    t.id_triwulan,
                    SUM(d.tms_meninggal_l + d.tms_meninggal_p) as total_meninggal,
                    SUM(d.tms_dibawah_umur_l + d.tms_dibawah_umur_p) as total_dibawah_umur,
                    SUM(d.tms_ganda_l + d.tms_ganda_p) as total_ganda,
                    SUM(d.tms_pindah_keluar_l + d.tms_pindah_keluar_p) as total_pindah,
                    SUM(d.tms_tni_l + d.tms_tni_p) as total_tni
                FROM triwulan t
                LEFT JOIN db_rekap_model_a d ON t.id_triwulan = d.id_triwulan
                GROUP BY t.id_triwulan
            """), conn)
            
            # Merge data
            df_yearly = df_yearly.merge(df_tms, on='id_triwulan', how='left')
            df_yearly['total_tms'] = (
                df_yearly['total_meninggal'].fillna(0) +
                df_yearly['total_dibawah_umur'].fillna(0) +
                df_yearly['total_ganda'].fillna(0) +
                df_yearly['total_pindah'].fillna(0) +
                df_yearly['total_tni'].fillna(0)
            )
        
        if df_yearly.empty:
            st.warning("⚠️ Belum ada data yang tersimpan di database.")
            st.info("💡 Silakan upload data triwulan terlebih dahulu menggunakan menu di sidebar.")
            return
        
        # ========================================
        # SECTION 1: 4 OVERVIEW CARDS
        # ========================================
        # st.markdown("## 📊 Overview Keseluruhan")
        
        # Data triwulan terbaru
        latest_data = df_yearly.iloc[0]
        latest_total = latest_data['total_pemilih']
        latest_quarter = latest_data['triwulan_ke']
        latest_year = latest_data['tahun']
        latest_tms = latest_data['total_tms']
        
        # Hitung data per tahun (agregasi semua triwulan dalam 1 tahun)
        df_yearly_summary = df_yearly.groupby('tahun').agg({
            'total_pemilih': 'sum',
            'total_laki': 'sum',
            'total_perempuan': 'sum',
            'total_tms': 'sum',
            'id_triwulan': 'first'  # Ambil id_triwulan terbaru untuk query disabilitas
        }).reset_index()
        df_yearly_summary = df_yearly_summary.sort_values('tahun', ascending=False)
        
        # Data tahun terbaru
        latest_year_data = df_yearly_summary.iloc[0]
        current_year = int(latest_year_data['tahun'])
        current_total = latest_year_data['total_pemilih']
        current_tms = latest_year_data['total_tms']
        
        # Data tahun sebelumnya (untuk growth)
        if len(df_yearly_summary) > 1:
            previous_year_data = df_yearly_summary.iloc[1]
            previous_total = previous_year_data['total_pemilih']
            yearly_growth = ((current_total - previous_total) / previous_total * 100) if previous_total > 0 else 0
            yearly_growth_abs = current_total - previous_total
        else:
            yearly_growth = 0
            yearly_growth_abs = 0
        
        # TMS Rate (tahunan)
        tms_rate = (current_tms / current_total * 100) if current_total > 0 else 0
        
        # Query total disabilitas (agregat semua triwulan di tahun ini)
        try:
            with engine.connect() as conn:
                df_dis_yearly = pd.read_sql(text("""
                    SELECT 
                        SUM(d.total_disabilitas) as total_disabilitas
                    FROM detail_disabilitas d
                    JOIN triwulan t ON d.id_triwulan = t.id_triwulan
                    WHERE t.tahun = :year
                """), conn, params={"year": current_year})
                
                total_disabilitas_yearly = int(df_dis_yearly['total_disabilitas'].iloc[0]) if not df_dis_yearly.empty and df_dis_yearly['total_disabilitas'].iloc[0] else 0
                disabilitas_rate = (total_disabilitas_yearly / current_total * 100) if current_total > 0 and total_disabilitas_yearly > 0 else 0
        except:
            total_disabilitas_yearly = 0
            disabilitas_rate = 0
        
        # Display 4 cards
        col1, col2, col3, col4 = st.columns(4)
        
        # CARD 1: Data Terbaru
        with col1:
            with st.container(border=True):
                st.markdown("**📈 Data Triwulan Terbaru**")
                st.markdown(f"### {int(latest_total):,}".replace(",", "."))
                st.markdown(f"**Triwulan {int(latest_quarter)} - {int(latest_year)}**")
                st.caption("Total pemilih terdaftar triwulan terakhir")
        
        # CARD 2: Growth Rate
        with col2:
            with st.container(border=True):
                st.markdown("**📊 Growth Rate**")
                
                # Growth value dengan warna
                if yearly_growth > 0:
                    st.markdown(f"### :green[+{yearly_growth:.2f}%]")
                    st.markdown(f":green[↑ {int(abs(yearly_growth_abs)):,} orang]".replace(",", "."))
                elif yearly_growth < 0:
                    st.markdown(f"### :red[{yearly_growth:.2f}%]")
                    st.markdown(f":red[↓ {int(abs(yearly_growth_abs)):,} orang]".replace(",", "."))
                else:
                    st.markdown(f"### {yearly_growth:.2f}%")
                    st.markdown("Tidak ada perubahan")
                
                st.caption("Dibanding tahun sebelumnya")
        
        # CARD 3: Total TMS & Rate
        with col3:
            with st.container(border=True):
                st.markdown("**⚠️ Total TMS**")
                st.markdown(f"### <span style='color:red'>{int(current_tms):,}".replace(",", ".") + "</span>", unsafe_allow_html=True)
                st.markdown(f"**Rate: {tms_rate:.2f}%**")
                
                
                # Status indicator
                if tms_rate > 1.5:
                    st.caption("⚠️ Tinggi - Perlu perhatian")
                elif tms_rate > 1.0:
                    st.caption("Perbandingan Total pemilih dengan Total TMS") #⚠️ Sedang
                else:
                    st.caption("Perbandingan Total pemilih dengan Total TMS") #✅ Rendah
        
        # CARD 4: Total Disabilitas (Placeholder)
        with col4:
            with st.container(border=True):
                st.markdown("**🦽 Total Disabilitas**")
                
                if total_disabilitas_yearly > 0:
                    st.markdown(f"### <span style='color:red'>{int(total_disabilitas_yearly):,}".replace(",", ".") + "</span>", unsafe_allow_html=True)
                    st.markdown(f"**Rate: {disabilitas_rate:.2f}%**")
                    st.caption("Perbandingan Total Pemilih dengan Total Disabilitas")
                else:
                    st.markdown(f"### -")
                    st.caption("Data belum tersedia")
                    st.info("💡 Upload Excel dengan sheet SIDALIH WEB")
        
        # st.markdown("---")
        
        # ========================================
        # SECTION 2: TREND PER TRIWULAN
        # ========================================
        
        with st.container(border=True):
            st.markdown("## 📈 Trend Data Per Triwulan")
            
            # Buat label untuk sumbu X
            df_yearly['label'] = df_yearly.apply(
                lambda x: f"T{x['triwulan_ke']}-{x['tahun']}", axis=1
            )
            
            # Grafik trend pemilih
            col_trend1, col_trend2 = st.columns([2, 1])
            
            with col_trend1:
                fig_trend = go.Figure()
                
                # Line untuk Total
                fig_trend.add_trace(go.Scatter(
                    x=df_yearly['label'][::-1],  # Reverse untuk urutan kronologis
                    y=df_yearly['total_pemilih'][::-1],
                    mode='lines+markers+text',
                    name='Total Pemilih',
                    line=dict(color='#A72703', width=3),
                    marker=dict(size=10),
                    text=df_yearly['total_pemilih'][::-1],
                    texttemplate='%{text:,.0f}',
                    textposition='top center'
                ))
                
                fig_trend.update_layout(
                    title='Trend Total Pemilih Per Triwulan',
                    xaxis_title='Triwulan',
                    yaxis_title='Jumlah Pemilih',
                    hovermode='x unified',
                    height=400,
                    showlegend=False
                )
                
                st.plotly_chart(fig_trend, use_container_width=True)
        
            with col_trend2:
                # Pie chart gender untuk data terbaru
                latest_data = df_yearly.iloc[0]
                
                fig_gender_latest = go.Figure(data=[go.Pie(
                    labels=['Laki-laki', 'Perempuan'],
                    values=[latest_data['total_laki'], latest_data['total_perempuan']],
                    hole=.4,
                    marker_colors=['#A72703', '#FFE797']
                )])
                
                fig_gender_latest.update_layout(
                    title=f'Distribusi Gender Pemilih<br>Triwulan {latest_data["triwulan_ke"]} tahun {latest_data["tahun"]}',
                    height=400,
                    showlegend=True
                )
                st.plotly_chart(fig_gender_latest, use_container_width=True)
                st.caption("Grafik perbandingan total pemilih laki-laki & perempuan pada Kabupaten Malang.")
        
        # ========================================
        # SECTION 3: PERBANDINGAN GENDER PER TRIWULAN
        # ========================================
        with st.container(border=True):
            st.markdown("## 👥 Perbandingan Gender Per Triwulan")
            
            fig_gender_trend = go.Figure()
            
            fig_gender_trend.add_trace(go.Bar(
                name='Laki-laki',
                x=df_yearly['label'][::-1],
                y=df_yearly['total_laki'][::-1],
                marker_color='#A72703',
                text=df_yearly['total_laki'][::-1],
                texttemplate='%{text:,.0f}',
                textposition='outside'
            ))
            
            fig_gender_trend.add_trace(go.Bar(
                name='Perempuan',
                x=df_yearly['label'][::-1],
                y=df_yearly['total_perempuan'][::-1],
                marker_color='#FFE797',
                text=df_yearly['total_perempuan'][::-1],
                texttemplate='%{text:,.0f}',
                textposition='outside'
            ))
            
            fig_gender_trend.update_layout(
                barmode='group',
                xaxis_title='Triwulan',
                yaxis_title='Jumlah Pemilih',
                height=400,
                hovermode='x unified'
            )
            
            st.plotly_chart(fig_gender_trend, use_container_width=True)
        
        # ========================================
        # SECTION 4: DATA TMS PER TRIWULAN
        # ========================================
        with st.container(border=True):
            st.markdown("## ⚠️ Data TMS (Tidak Memenuhi Syarat) Per Triwulan")
            
            # Stacked bar untuk TMS
            fig_tms = go.Figure()
            
            fig_tms.add_trace(go.Bar(
                name='Meninggal',
                x=df_yearly['label'][::-1],
                y=df_yearly['total_meninggal'][::-1],
                marker_color='#ef476f'
            ))
            
            fig_tms.add_trace(go.Bar(
                name='Di Bawah Umur',
                x=df_yearly['label'][::-1],
                y=df_yearly['total_dibawah_umur'][::-1],
                marker_color='#ffd166'
            ))
            
            fig_tms.add_trace(go.Bar(
                name='Ganda',
                x=df_yearly['label'][::-1],
                y=df_yearly['total_ganda'][::-1],
                marker_color='#06d6a0'
            ))
            
            fig_tms.add_trace(go.Bar(
                name='Pindah',
                x=df_yearly['label'][::-1],
                y=df_yearly['total_pindah'][::-1],
                marker_color='#118ab2'
            ))
            
            fig_tms.add_trace(go.Bar(
                name='TNI/Polri',
                x=df_yearly['label'][::-1],
                y=df_yearly['total_tni'][::-1],
                marker_color='#073b4c'
            ))
            
            fig_tms.update_layout(
                barmode='stack',
                title='Breakdown TMS Per Kategori',
                xaxis_title='Triwulan',
                yaxis_title='Jumlah',
                height=400,
                hovermode='x unified'
            )
            
            st.plotly_chart(fig_tms, use_container_width=True)
        
        # ========================================
        # SECTION 5: TABEL DETAIL PER TRIWULAN
        # ========================================
        # with st.expander("📋 Lihat Detail Data Per Triwulan"):
        #     # st.markdown("## 📋 Detail Data Per Triwulan")
        
        #     # Siapkan dataframe untuk display
        #     df_display = df_yearly[[
        #         'tahun', 'triwulan_ke', 'jumlah_kecamatan', 'total_desa_kel',
        #         'total_laki', 'total_perempuan', 'total_pemilih', 'total_tms'
        #     ]].copy()
            
        #     df_display.columns = [
        #         'Tahun', 'Triwulan', 'Kecamatan', 'Desa/Kel',
        #         'Laki-laki', 'Perempuan', 'Total Pemilih', 'TMS'
        #     ]
            
        #     # Format angka
        #     for col in ['Kecamatan', 'Desa/Kel', 'Laki-laki', 'Perempuan', 'Total Pemilih', 'TMS']:
        #         df_display[col] = df_display[col].apply(lambda x: f"{int(x):,}".replace(",", ".") if pd.notna(x) else "0")
            
        #     st.dataframe(df_display, use_container_width=True, hide_index=True)
        
    except Exception as e:
        st.error(f"❌ Error saat memuat data overview: {e}")
        st.info("💡 Pastikan tabel triwulan dan rekapitulasi_pdpb sudah terisi dengan benar.")
        
def reset_triwulan_data():
    """Reset session state untuk data triwulan"""
    st.session_state.data_loaded = False
    st.session_state.df_pdpb = None
    st.session_state.df_pdpb_before = None
    st.session_state.df_model_a = None
    st.session_state.df_db_rekap = None
    st.session_state.selected_id = None
    
# --- Homepage ---
st.set_page_config(page_title="Dashboard Infografis PDPB KPU Kabupaten Malang", layout="wide")
st.header("Infografis PDPB KPU Kabupaten Malang")
st.markdown("---")

# --- Sidebar ---
st.sidebar.write(f"<span style='font-weight:bold;'>TAMPILAN</span>", unsafe_allow_html=True)
# engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
# st.sidebar.markdown("### 📅 Pilih Data Triwulan")

# Radio button untuk pilih mode
nav_option = st.sidebar.radio(
    "Pilih Tampilan:",
    ["🏠 Beranda (Overview Tahunan)", "📊 Data Per Triwulan"],
    key="nav_radio"
)

# Update session state berdasarkan pilihan
if nav_option == "🏠 Beranda (Overview Tahunan)":
    st.session_state.view_mode = 'home'
    reset_triwulan_data()
else:
    st.session_state.view_mode = 'detail'

st.sidebar.markdown("---")

# ========================================
# ROUTING BERDASARKAN MODE
# ========================================

if st.session_state.view_mode == 'home':
    # TAMPILKAN HOME/OVERVIEW
    display_home_overview(engine)
    
else:
    # TAMPILKAN DETAIL PER TRIWULAN (kode yang sudah ada)
    st.sidebar.write(f"<span style='font-weight:bold;'>Data Triwulan</span>", unsafe_allow_html=True)
    st.sidebar.markdown("### 📅 Filter Data Triwulan")
    
try:
    with engine.connect() as conn:
        triwulan_list = conn.execute(text("""
            SELECT id_triwulan, triwulan_ke, tahun, judul
            FROM triwulan
            ORDER BY tahun DESC, triwulan_ke DESC
        """)).fetchall()

    if triwulan_list:
        triwulan_options = {
            f"Triwulan {row.triwulan_ke} - Tahun {row.tahun}": row.id_triwulan
            for row in triwulan_list
        }

        selected_label = st.sidebar.selectbox("Pilih data Triwulan:", list(triwulan_options.keys()))
        selected_id = triwulan_options[selected_label]
        
        info_box = st.empty()
        if st.sidebar.button("📊 Tampilkan Data") or st.session_state.data_loaded:
            if not st.session_state.data_loaded or st.session_state.selected_id != selected_id:
                with engine.connect() as conn:
                    # Load data triwulan sebelumnya
                    st.session_state.df_pdpb_before = pd.read_sql(
                        text("SELECT nama_kecamatan,jumlah_tps,laki,perempuan,total FROM triwulan_sebelumnya WHERE id_triwulan = :id"),
                        conn, 
                        params={"id": selected_id}
                    )
                    
                    # Fallback: Jika tabel triwulan_sebelumnya kosong, ambil dari rekapitulasi_pdpb triwulan sebelumnya
                    if st.session_state.df_pdpb_before.empty:
                        # Cari id_triwulan sebelumnya
                        df_prev_id = pd.read_sql(text("""
                            SELECT t1.id_triwulan, t1.tahun, t1.triwulan_ke
                            FROM triwulan t1
                            JOIN triwulan t2 ON t2.id_triwulan = :current_id
                            WHERE (t1.tahun < t2.tahun) 
                               OR (t1.tahun = t2.tahun AND t1.triwulan_ke < t2.triwulan_ke)
                            ORDER BY t1.tahun DESC, t1.triwulan_ke DESC
                            LIMIT 1
                        """), conn, params={"current_id": selected_id})
                        
                        if not df_prev_id.empty:
                            prev_id = df_prev_id.iloc[0]['id_triwulan']
                            st.session_state.df_pdpb_before = pd.read_sql(
                                text("""
                                    SELECT 
                                        nama_kecamatan,
                                        0 as jumlah_tps,
                                        jumlah_pemilih_laki as laki,
                                        jumlah_pemilih_perempuan as perempuan,
                                        total_pemilih as total
                                    FROM rekapitulasi_pdpb 
                                    WHERE id_triwulan = :id
                                """),
                                conn, 
                                params={"id": prev_id}
                            )
                        else:
                            # Tidak ada triwulan sebelumnya - buat dataframe kosong
                            st.session_state.df_pdpb_before = pd.DataFrame(columns=[
                                'nama_kecamatan', 'jumlah_tps', 'laki', 'perempuan', 'total'
                            ])
                    
                    st.session_state.df_pdpb = pd.read_sql(
                        text("SELECT nama_kecamatan,jumlah_desa_kel,jumlah_pemilih_laki,jumlah_pemilih_perempuan,total_pemilih FROM rekapitulasi_pdpb WHERE id_triwulan = :id"),
                        conn, 
                        params={"id": selected_id}
                    )

                    st.session_state.df_model_a = pd.read_sql(
                        text("SELECT nama_kecamatan,jumlah_desa_kel,jumlah_pemilih_baru,jumlah_pemilih_tms,jumlah_perbaikan_data FROM rekap_model_a WHERE id_triwulan = :id"), 
                        conn, 
                        params={"id": selected_id}
                    )

                    st.session_state.df_db_rekap = pd.read_sql(
                        text("SELECT nama_kecamatan,pemilih_baru_l,pemilih_baru_p,tms_meninggal_l,tms_meninggal_p,tms_dibawah_umur_l,tms_dibawah_umur_p,tms_ganda_l,tms_ganda_p,tms_pindah_keluar_l,tms_pindah_keluar_p,tms_tni_l,tms_tni_p FROM db_rekap_model_a WHERE id_triwulan = :id"), 
                        conn, 
                        params={"id": selected_id}
                    )
                st.session_state.data_loaded = True
                st.session_state.selected_id = selected_id
                st.session_state.selected_label = selected_label
                st.toast(f"Menampilkan data **{selected_label}**")
            
            df_pdpb = st.session_state.df_pdpb
            df_pdpb_before = st.session_state.df_pdpb_before
            df_model_a = st.session_state.df_model_a
            df_db_rekap = st.session_state.df_db_rekap
            
            #--- CHART ATAS  ---
            st.markdown(f"<h2 style='text-align: center;'>Data {selected_label}</h2>", unsafe_allow_html=True)
            
            # ===== FITUR FILTER KECAMATAN =====
            st.sidebar.markdown("---")
            st.sidebar.markdown("### 🔍 Filter Kecamatan")
            
            #Ambil daftar kecamatan
            if 'nama_kecamatan' in df_pdpb.columns and not df_pdpb.empty:
                list_kecamatan = sorted(df_pdpb['nama_kecamatan'].unique().tolist())
                
                # Opsi filter
                filter_option = st.sidebar.radio(
                    "Pilih tampilan:",
                    ["📊 Semua Kecamatan", "📍 Kecamatan Tertentu", "🔄 Perbandingan"],
                    key="filter_option"
                )
                #Inisialisasi variabel
                selected_kecamatan = None
                selected_kecamatans = []
                
                if filter_option == "📍 Kecamatan Tertentu":
                    selected_kecamatan = st.sidebar.selectbox(
                        "Pilih Kecamatan:",
                        list_kecamatan,
                        key="select_kecamatan"
                    )
                    
                    # Normalisasi nama untuk matching yang lebih baik
                    selected_kecamatan_norm = selected_kecamatan.upper().strip()
                    
                    # Buat kolom normalized untuk matching
                    df_pdpb['nama_kecamatan_norm'] = df_pdpb['nama_kecamatan'].str.upper().str.strip()
                    df_model_a['nama_kecamatan_norm'] = df_model_a['nama_kecamatan'].str.upper().str.strip()
                    df_db_rekap['nama_kecamatan_norm'] = df_db_rekap['nama_kecamatan'].str.upper().str.strip()
                    df_pdpb_before['nama_kecamatan_norm'] = df_pdpb_before['nama_kecamatan'].str.upper().str.strip()
                    
                    # Filter data dengan normalisasi
                    df_pdpb_filtered = df_pdpb[df_pdpb['nama_kecamatan_norm'] == selected_kecamatan_norm]
                    df_model_a_filtered = df_model_a[df_model_a['nama_kecamatan_norm'] == selected_kecamatan_norm]
                    df_db_rekap_filtered = df_db_rekap[df_db_rekap['nama_kecamatan_norm'] == selected_kecamatan_norm]
                    df_pdpb_before_filtered = df_pdpb_before[df_pdpb_before['nama_kecamatan_norm'] == selected_kecamatan_norm]
                    
                    # Debug info
                    # st.info(f"📍 Menampilkan data untuk **{selected_kecamatan}**")
                    # with st.expander("🔍 Debug Info"):
                    #     st.write(f"Kecamatan dipilih (normalized): {selected_kecamatan_norm}")
                    #     st.write(f"df_pdpb_filtered: {len(df_pdpb_filtered)} rows")
                    #     st.write(f"df_model_a_filtered: {len(df_model_a_filtered)} rows")
                    #     st.write(f"df_db_rekap_filtered: {len(df_db_rekap_filtered)} rows")
                    #     if len(df_model_a_filtered) == 0:
                    #         st.warning("⚠️ Model A filtered kosong!")
                    #         st.write("Kecamatan di df_model_a:", df_model_a['nama_kecamatan'].unique()[:5].tolist())
                    #     if len(df_db_rekap_filtered) == 0:
                    #         st.warning("⚠️ DB Rekap filtered kosong!")
                    #         st.write("Kecamatan di df_db_rekap:", df_db_rekap['nama_kecamatan'].unique()[:5].tolist())
                    
                elif filter_option == "🔄 Perbandingan":
                    selected_kecamatans = st.sidebar.multiselect(
                        "Pilih 2-5 Kecamatan untuk dibandingkan:",
                        list_kecamatan,
                        max_selections=5,
                        key="multiselect_kecamatan"
                    )
                    
                    if len(selected_kecamatans) >= 2:
                        # Normalisasi nama untuk matching
                        selected_kecamatans_norm = [k.upper().strip() for k in selected_kecamatans]
                        
                        # Buat kolom normalized
                        df_pdpb['nama_kecamatan_norm'] = df_pdpb['nama_kecamatan'].str.upper().str.strip()
                        df_model_a['nama_kecamatan_norm'] = df_model_a['nama_kecamatan'].str.upper().str.strip()
                        df_db_rekap['nama_kecamatan_norm'] = df_db_rekap['nama_kecamatan'].str.upper().str.strip()
                        df_pdpb_before['nama_kecamatan_norm'] = df_pdpb_before['nama_kecamatan'].str.upper().str.strip()
                        
                        # Filter dengan normalisasi
                        df_pdpb_filtered = df_pdpb[df_pdpb['nama_kecamatan_norm'].isin(selected_kecamatans_norm)]
                        df_model_a_filtered = df_model_a[df_model_a['nama_kecamatan_norm'].isin(selected_kecamatans_norm)]
                        df_db_rekap_filtered = df_db_rekap[df_db_rekap['nama_kecamatan_norm'].isin(selected_kecamatans_norm)]
                        df_pdpb_before_filtered = df_pdpb_before[df_pdpb_before['nama_kecamatan_norm'].isin(selected_kecamatans_norm)]
                        
                        st.info(f"🔄 Membandingkan {len(selected_kecamatans)} kecamatan: {', '.join(selected_kecamatans)}")
                    else:
                        st.warning("⚠️ Pilih minimal 2 kecamatan untuk perbandingan")
                        df_pdpb_filtered = df_pdpb
                        df_model_a_filtered = df_model_a
                        df_db_rekap_filtered = df_db_rekap
                        df_pdpb_before_filtered = df_pdpb_before
                else:
                    # Semua kecamatan
                    df_pdpb_filtered = df_pdpb
                    df_model_a_filtered = df_model_a
                    df_db_rekap_filtered = df_db_rekap
                    df_pdpb_before_filtered = df_pdpb_before
            else:
                df_pdpb_filtered = df_pdpb
                df_model_a_filtered = df_model_a
                df_db_rekap_filtered = df_db_rekap
                df_pdpb_before_filtered = df_pdpb_before
       
            # Hitung totals dari data yang sudah difilter
            total_pemilih = df_pdpb_filtered['total_pemilih'].sum() if 'total_pemilih' in df_pdpb_filtered.columns else 0
            total_kecamatan = df_pdpb_filtered['nama_kecamatan'].nunique() if 'nama_kecamatan' in df_pdpb_filtered.columns else 0
            total_desa_kel = df_pdpb_filtered['jumlah_desa_kel'].sum() if 'jumlah_desa_kel' in df_pdpb_filtered.columns else 0
            
            # --- Hitung perbandingan dengan Triwulan Sebelumnya ---
            # PENTING: Gunakan data NON-FILTERED untuk perbandingan (sama seperti chart bawah)
            prev_laki = df_pdpb_before['laki'].sum() if not df_pdpb_before.empty and 'laki' in df_pdpb_before.columns else 0
            prev_perempuan = df_pdpb_before['perempuan'].sum() if not df_pdpb_before.empty and 'perempuan' in df_pdpb_before.columns else 0
            prev_total = prev_laki + prev_perempuan

            # Hitung data triwulan saat ini (NON-FILTERED)
            curr_laki = df_pdpb['jumlah_pemilih_laki'].sum() if not df_pdpb.empty and 'jumlah_pemilih_laki' in df_pdpb.columns else 0
            curr_perempuan = df_pdpb['jumlah_pemilih_perempuan'].sum() if not df_pdpb.empty and 'jumlah_pemilih_perempuan' in df_pdpb.columns else 0
            curr_total = curr_laki + curr_perempuan

            # Hitung selisih (absolute difference)
            selisih_total = curr_total - prev_total
            selisih_laki = curr_laki - prev_laki
            selisih_perempuan = curr_perempuan - prev_perempuan

            # Hitung persentase perubahan
            persen_total = ((curr_total - prev_total) / prev_total * 100) if prev_total > 0 else 0
            persen_laki = ((curr_laki - prev_laki) / prev_laki * 100) if prev_laki > 0 else 0
            persen_perempuan = ((curr_perempuan - prev_perempuan) / prev_perempuan * 100) if prev_perempuan > 0 else 0
            
            # Metrics dengan delta indicator
            col1, col2, col3, col4 = st.columns(4, border=True)
            
            with col1:
                # Cek apakah ada data sebelumnya
                has_prev_data = prev_total > 0
                
                st.metric(
                    label="👥 Total Pemilih",
                    value=f"{int(total_pemilih):,}".replace(",", "."),
                    delta=f"{int(selisih_total):,} orang ({persen_total:+.2f}%)".replace(",", ".") if has_prev_data else "Tidak ada data sebelumnya",
                    delta_color="normal" if has_prev_data else "off"
                )
            
            with col2:
                has_prev_laki = prev_laki > 0
                
                st.metric(
                    label="♂️ Jumlah Pemilih Laki",
                    value=f"{int(df_pdpb_filtered['jumlah_pemilih_laki'].sum() if 'jumlah_pemilih_laki' in df_pdpb_filtered.columns else 0):,}".replace(",", "."),
                    delta=f"{int(selisih_laki):,} orang ({persen_laki:+.2f}%)".replace(",", ".") if has_prev_laki else "Tidak ada data sebelumnya",
                    delta_color="normal" if has_prev_laki else "off"
                )
            
            with col3:
                has_prev_perempuan = prev_perempuan > 0
                
                st.metric(
                    label="♀️ Jumlah Pemilih Perempuan",
                    value=f"{int(df_pdpb_filtered['jumlah_pemilih_perempuan'].sum() if 'jumlah_pemilih_perempuan' in df_pdpb_filtered.columns else 0):,}".replace(",", "."),
                    delta=f"{int(selisih_perempuan):,} orang ({persen_perempuan:+.2f}%)".replace(",", ".") if has_prev_perempuan else "Tidak ada data sebelumnya",
                    delta_color="normal" if has_prev_perempuan else "off"
                )
            
            with col4:
                st.metric(
                    label="🏠 Jumlah Desa/Kel",
                    value=f"{int(total_desa_kel)}"
                )
            
            # Grafik - Disesuaikan dengan filter
            if not df_pdpb_filtered.empty and 'nama_kecamatan' in df_pdpb_filtered.columns:
                st.subheader("📈 Distribusi Pemilih per Kecamatan")
                
                # Tambahkan gender breakdown
                col_chart1, col_chart2 = st.columns([2, 1], border=True)
                
                with col_chart1:
                    fig = px.bar(
                        df_pdpb_filtered.sort_values('total_pemilih', ascending=False),
                        x='nama_kecamatan',
                        y='total_pemilih',
                        title='Total Pemilih per Kecamatan',
                        labels={'total_pemilih': 'Jumlah Pemilih', 'nama_kecamatan': 'Kecamatan'},
                        color='total_pemilih',
                        color_continuous_scale='Reds',
                        text='total_pemilih'
                    )
                    fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
                    fig.update_layout(
                        xaxis_tickangle=-45,
                        showlegend=False,
                        height=400
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col_chart2:
                    # Pie chart untuk gender distribution
                    if 'jumlah_pemilih_laki' in df_pdpb_filtered.columns and 'jumlah_pemilih_perempuan' in df_pdpb_filtered.columns:
                        total_laki = df_pdpb_filtered['jumlah_pemilih_laki'].sum()
                        total_perempuan = df_pdpb_filtered['jumlah_pemilih_perempuan'].sum()
                        
                        fig_gender = go.Figure(data=[go.Pie(
                            labels=['Laki-laki', 'Perempuan'],
                            values=[total_laki, total_perempuan],
                            hole=.3,
                            marker_colors=['#A72703', "#FFE797"]
                        )])
                        fig_gender.update_layout(
                            title='Distribusi Gender',
                            height=400
                        )
                        st.plotly_chart(fig_gender, use_container_width=True)
                
                # Grafik perbandingan gender per kecamatan
                with st.container(border=True):
                    if filter_option == "🔄 Perbandingan" and len(selected_kecamatans) >= 2:
                        st.subheader("👥 Perbandingan Gender per Kecamatan")
                        df_gender = df_pdpb_filtered[['nama_kecamatan', 'jumlah_pemilih_laki', 'jumlah_pemilih_perempuan']].copy()
                        
                        fig_gender_compare = go.Figure()
                        fig_gender_compare.add_trace(go.Bar(
                            name='Laki-laki',
                            x=df_gender['nama_kecamatan'],
                            y=df_gender['jumlah_pemilih_laki'],
                            marker_color='#A72703'
                        ))
                        fig_gender_compare.add_trace(go.Bar(
                            name='Perempuan',
                            x=df_gender['nama_kecamatan'],
                            y=df_gender['jumlah_pemilih_perempuan'],
                            marker_color='#FFE797'
                        ))
                        
                        fig_gender_compare.update_layout(
                            barmode='group',
                            xaxis_tickangle=-45,
                            yaxis_title='Jumlah Pemilih',
                            height=400
                        )
                        st.plotly_chart(fig_gender_compare, use_container_width=True)
                    
            #--- CHART DISABILITAS ---
            # st.markdown("---")
            st.subheader("🦽 Detail Pemilih dengan Disabilitas")
        
            try:
                # Query data disabilitas untuk triwulan ini
                with engine.connect() as conn:
                    df_disabilitas = pd.read_sql(text("""
                        SELECT 
                            UPPER(TRIM(nama_kecamatan)) as nama_kecamatan,
                            disabilitas_fisik,
                            disabilitas_intelektual,
                            disabilitas_mental,
                            disabilitas_sensorik_wicara,
                            disabilitas_sensorik_rungu,
                            disabilitas_sensorik_netra,
                            total_disabilitas
                        FROM detail_disabilitas
                        WHERE id_triwulan = :id
                        ORDER BY total_disabilitas DESC
                    """), conn, params={"id": selected_id})
                    
                    # Query data disabilitas triwulan sebelumnya (untuk delta)
                    df_disabilitas_before = pd.read_sql(text("""
                        SELECT 
                            UPPER(TRIM(nama_kecamatan)) as nama_kecamatan,
                            disabilitas_fisik,
                            disabilitas_intelektual,
                            disabilitas_mental,
                            disabilitas_sensorik_wicara,
                            disabilitas_sensorik_rungu,
                            disabilitas_sensorik_netra,
                            total_disabilitas
                        FROM detail_disabilitas d
                        JOIN triwulan t ON d.id_triwulan = t.id_triwulan
                        WHERE t.tahun = (SELECT tahun FROM triwulan WHERE id_triwulan = :id)
                        AND t.triwulan_ke = (SELECT triwulan_ke - 1 FROM triwulan WHERE id_triwulan = :id)
                        ORDER BY total_disabilitas DESC
                    """), conn, params={"id": selected_id})
                
                if not df_disabilitas.empty:
                    # Normalisasi nama kecamatan di data filtered juga
                    if 'nama_kecamatan' in df_pdpb_filtered.columns:
                        df_pdpb_filtered['nama_kecamatan'] = df_pdpb_filtered['nama_kecamatan'].str.upper().str.strip()
                    
                    # Filter sesuai dengan opsi filter yang dipilih
                    if filter_option == "📍 Kecamatan Tertentu" and selected_kecamatan:
                        # Normalisasi nama kecamatan yang dipilih
                        selected_kecamatan_norm = selected_kecamatan.upper().strip()
                        # Filter 1 kecamatan
                        df_disabilitas_filtered = df_disabilitas[df_disabilitas['nama_kecamatan'] == selected_kecamatan_norm]
                        
                        # Debug info
                        if df_disabilitas_filtered.empty:
                            st.warning(f"🔍 Debug: Mencari '{selected_kecamatan_norm}' di data disabilitas...")
                            st.write("Kecamatan yang tersedia:", df_disabilitas['nama_kecamatan'].tolist()[:5])
                    elif filter_option == "🔄 Perbandingan" and selected_kecamatans and len(selected_kecamatans) >= 2:
                        # Normalisasi nama kecamatan yang dipilih
                        selected_kecamatans_norm = [k.upper().strip() for k in selected_kecamatans]
                        # Filter multiple kecamatan
                        df_disabilitas_filtered = df_disabilitas[df_disabilitas['nama_kecamatan'].isin(selected_kecamatans_norm)]
                        
                        # Debug info
                        if df_disabilitas_filtered.empty:
                            st.warning(f"🔍 Debug: Mencari {len(selected_kecamatans_norm)} kecamatan...")
                            st.write("Kecamatan dipilih:", selected_kecamatans_norm)
                            st.write("Kecamatan tersedia:", df_disabilitas['nama_kecamatan'].tolist()[:5])
                    else:
                        # Semua kecamatan
                        df_disabilitas_filtered = df_disabilitas.copy()
                    
                    if not df_disabilitas_filtered.empty:
                        # Hitung data triwulan sebelumnya (untuk delta)
                        if not df_disabilitas_before.empty:
                            # Filter data sebelumnya sesuai dengan filter yang sama
                            if filter_option == "📍 Kecamatan Tertentu" and selected_kecamatan:
                                selected_kecamatan_norm = selected_kecamatan.upper().strip()
                                df_disabilitas_before_filtered = df_disabilitas_before[df_disabilitas_before['nama_kecamatan'] == selected_kecamatan_norm]
                            elif filter_option == "🔄 Perbandingan" and selected_kecamatans and len(selected_kecamatans) >= 2:
                                selected_kecamatans_norm = [k.upper().strip() for k in selected_kecamatans]
                                df_disabilitas_before_filtered = df_disabilitas_before[df_disabilitas_before['nama_kecamatan'].isin(selected_kecamatans_norm)]
                            else:
                                df_disabilitas_before_filtered = df_disabilitas_before.copy()
                            
                            prev_total_disabilitas = df_disabilitas_before_filtered['total_disabilitas'].sum()
                            prev_fisik = df_disabilitas_before_filtered['disabilitas_fisik'].sum()
                            prev_intelektual = df_disabilitas_before_filtered['disabilitas_intelektual'].sum()
                            prev_mental = df_disabilitas_before_filtered['disabilitas_mental'].sum()
                        else:
                            prev_total_disabilitas = 0
                            prev_fisik = 0
                            prev_intelektual = 0
                            prev_mental = 0
                        
                        # Metrics Total
                        col1, col2, col3, col4 = st.columns(4, border=True)
                        
                        total_disabilitas = df_disabilitas_filtered['total_disabilitas'].sum()
                        total_fisik = df_disabilitas_filtered['disabilitas_fisik'].sum()
                        total_intelektual = df_disabilitas_filtered['disabilitas_intelektual'].sum()
                        total_mental = df_disabilitas_filtered['disabilitas_mental'].sum()
                        
                        # Hitung delta
                        delta_total_disabilitas = total_disabilitas - prev_total_disabilitas
                        delta_fisik = total_fisik - prev_fisik
                        delta_intelektual = total_intelektual - prev_intelektual
                        delta_mental = total_mental - prev_mental
                        
                        persen_disabilitas = ((total_disabilitas - prev_total_disabilitas) / prev_total_disabilitas * 100) if prev_total_disabilitas > 0 else 0
                        persen_fisik = ((total_fisik - prev_fisik) / prev_fisik * 100) if prev_fisik > 0 else 0
                        persen_intelektual = ((total_intelektual - prev_intelektual) / prev_intelektual * 100) if prev_intelektual > 0 else 0
                        persen_mental = ((total_mental - prev_mental) / prev_mental * 100) if prev_mental > 0 else 0
                        
                        with col1:
                            st.metric(
                                label="📊 Total Disabilitas",
                                value=f"{int(total_disabilitas):,}".replace(",", "."),
                                delta=f"{delta_total_disabilitas:+,} ({persen_disabilitas:+.2f}%)".replace(",", ".") if prev_total_disabilitas > 0 else "Tidak ada data sebelumnya",
                                delta_color="normal"
                            )
                        with col2:
                            st.metric(
                                label="🦿 Fisik",
                                value=f"{int(total_fisik):,}".replace(",", "."),
                                delta=f"{delta_fisik:+,} ({persen_fisik:+.2f}%)".replace(",", ".") if prev_fisik > 0 else "Tidak ada data sebelumnya",
                                delta_color="normal"
                            )
                        with col3:
                            st.metric(
                                label="🧠 Intelektual",
                                value=f"{int(total_intelektual):,}".replace(",", "."),
                                delta=f"{delta_intelektual:+,} ({persen_intelektual:+.2f}%)".replace(",", ".") if prev_intelektual > 0 else "Tidak ada data sebelumnya",
                                delta_color="normal"
                            )
                        with col4:
                            st.metric(
                                label="💭 Mental",
                                value=f"{int(total_mental):,}".replace(",", "."),
                                delta=f"{delta_mental:+,} ({persen_mental:+.2f}%)".replace(",", ".") if prev_mental > 0 else "Tidak ada data sebelumnya",
                                delta_color="normal"
                            )
                        
                        # Charts dalam 2 kolom
                        col_chart1, col_chart2 = st.columns([3,2], border=True)
                        
                        with col_chart1:
                            # Stacked Bar Chart - Detail per Kecamatan
                            st.markdown("#### 📊 Distribusi Jenis Disabilitas per Kecamatan")
                            
                            fig_stacked = go.Figure()
                            
                            fig_stacked.add_trace(go.Bar(
                                name='Fisik',
                                x=df_disabilitas_filtered['nama_kecamatan'],
                                y=df_disabilitas_filtered['disabilitas_fisik'],
                                marker_color='#ff595e'
                            ))
                            fig_stacked.add_trace(go.Bar(
                                name='Intelektual',
                                x=df_disabilitas_filtered['nama_kecamatan'],
                                y=df_disabilitas_filtered['disabilitas_intelektual'],
                                marker_color='#ff924c'
                            ))
                            fig_stacked.add_trace(go.Bar(
                                name='Mental',
                                x=df_disabilitas_filtered['nama_kecamatan'],
                                y=df_disabilitas_filtered['disabilitas_mental'],
                                marker_color='#ffca3a'
                            ))
                            fig_stacked.add_trace(go.Bar(
                                name='Sensorik Wicara',
                                x=df_disabilitas_filtered['nama_kecamatan'],
                                y=df_disabilitas_filtered['disabilitas_sensorik_wicara'],
                                marker_color='#8ac926'
                            ))
                            fig_stacked.add_trace(go.Bar(
                                name='Sensorik Rungu',
                                x=df_disabilitas_filtered['nama_kecamatan'],
                                y=df_disabilitas_filtered['disabilitas_sensorik_rungu'],
                                marker_color='#1982c4'
                            ))
                            fig_stacked.add_trace(go.Bar(
                                name='Sensorik Netra',
                                x=df_disabilitas_filtered['nama_kecamatan'],
                                y=df_disabilitas_filtered['disabilitas_sensorik_netra'],
                                marker_color='#6a4c93'
                            ))
                            
                            fig_stacked.update_layout(
                                barmode='stack',
                                xaxis_tickangle=-45,
                                yaxis_title='Jumlah Pemilih',
                                height=450,
                                legend=dict(
                                    orientation="h",
                                    yanchor="bottom",
                                    y=1.02,
                                    xanchor="right",
                                    x=1
                                )
                            )
                            
                            st.plotly_chart(fig_stacked, use_container_width=True)
                        
                        with col_chart2:
                            # Pie Chart - Proporsi Jenis Disabilitas
                            st.markdown("#### 🦽 Proporsi Jenis Disabilitas")
                            
                            total_sensorik_wicara = df_disabilitas_filtered['disabilitas_sensorik_wicara'].sum()
                            total_sensorik_rungu = df_disabilitas_filtered['disabilitas_sensorik_rungu'].sum()
                            total_sensorik_netra = df_disabilitas_filtered['disabilitas_sensorik_netra'].sum()
                            
                            jenis_disabilitas = [
                                'Fisik', 
                                'Intelektual', 
                                'Mental', 
                                'Sensorik Wicara', 
                                'Sensorik Rungu', 
                                'Sensorik Netra'
                            ]
                            
                            nilai_disabilitas = [
                                total_fisik,
                                total_intelektual,
                                total_mental,
                                total_sensorik_wicara,
                                total_sensorik_rungu,
                                total_sensorik_netra
                            ]
                            
                            colors = ['#ff595e', '#ff924c', '#ffca3a', '#8ac926', '#1982c4', '#6a4c93']
                            
                            fig_pie = go.Figure(data=[go.Pie(
                                labels=jenis_disabilitas,
                                values=nilai_disabilitas,
                                hole=.4,
                                marker_colors=colors,
                                textposition='inside',
                                textinfo='percent+label'
                            )])
                            
                            fig_pie.update_layout(
                                height=450,
                                showlegend=True,
                                legend=dict(
                                    orientation="v",
                                    yanchor="middle",
                                    y=0.5,
                                    xanchor="left",
                                    x=1.05
                                )
                            )
                            
                            st.plotly_chart(fig_pie, use_container_width=True)
                        
                        # Chart Perbandingan Antar Kecamatan (jika mode Perbandingan)
                        with st.container(border=True):
                            if filter_option == "🔄 Perbandingan" and len(selected_kecamatans) >= 2:
                                # st.markdown("---")
                                st.markdown("#### 📊 Perbandingan Detail Disabilitas Antar Kecamatan")
                                
                                # Grouped Bar Chart - Perbandingan per jenis
                                fig_compare = go.Figure()
                                
                                colors_compare = {
                                    'disabilitas_fisik': '#ff595e',
                                    'disabilitas_intelektual': '#ff924c',
                                    'disabilitas_mental': '#ffca3a',
                                    'disabilitas_sensorik_wicara': '#8ac926',
                                    'disabilitas_sensorik_rungu': '#1982c4',
                                    'disabilitas_sensorik_netra': '#6a4c93'
                                }
                                
                                labels_compare = {
                                    'disabilitas_fisik': 'Fisik',
                                    'disabilitas_intelektual': 'Intelektual',
                                    'disabilitas_mental': 'Mental',
                                    'disabilitas_sensorik_wicara': 'Sensorik Wicara',
                                    'disabilitas_sensorik_rungu': 'Sensorik Rungu',
                                    'disabilitas_sensorik_netra': 'Sensorik Netra'
                                }
                                
                                for col, label in labels_compare.items():
                                    fig_compare.add_trace(go.Bar(
                                        name=label,
                                        x=df_disabilitas_filtered['nama_kecamatan'],
                                        y=df_disabilitas_filtered[col],
                                        marker_color=colors_compare[col]
                                    ))
                                
                                fig_compare.update_layout(
                                    barmode='group',
                                    xaxis_tickangle=-45,
                                    yaxis_title='Jumlah Pemilih',
                                    height=450,
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        xanchor="right",
                                        x=1
                                    ),
                                    title="Perbandingan Setiap Jenis Disabilitas per Kecamatan"
                                )
                                
                                st.plotly_chart(fig_compare, use_container_width=True)
                                
                                # Tabel Perbandingan
                                # st.markdown("#### 📋 Tabel Perbandingan")
                                
                                # Buat pivot table untuk perbandingan yang lebih jelas
                                # df_pivot = df_disabilitas_filtered.set_index('nama_kecamatan')
                                # df_pivot = df_pivot[[
                                #     'disabilitas_fisik',
                                #     'disabilitas_intelektual', 
                                #     'disabilitas_mental',
                                #     'disabilitas_sensorik_wicara',
                                #     'disabilitas_sensorik_rungu',
                                #     'disabilitas_sensorik_netra',
                                #     'total_disabilitas'
                                # ]]
                                
                                # Format angka
                                # df_pivot_display = df_pivot.copy()
                                # for col in df_pivot_display.columns:
                                #     df_pivot_display[col] = df_pivot_display[col].apply(lambda x: f"{int(x):,}".replace(",", "."))
                                
                                # st.dataframe(
                                #     df_pivot_display,
                                #     use_container_width=True,
                                #     column_config={
                                #         "disabilitas_fisik": "Fisik",
                                #         "disabilitas_intelektual": "Intelektual",
                                #         "disabilitas_mental": "Mental",
                                #         "disabilitas_sensorik_wicara": "Sensorik Wicara",
                                #         "disabilitas_sensorik_rungu": "Sensorik Rungu",
                                #         "disabilitas_sensorik_netra": "Sensorik Netra",
                                #         "total_disabilitas": st.column_config.NumberColumn(
                                #             "Total",
                                #             format="%d"
                                #         )
                                #     }
                                # )
                        
                        # Tabel Detail (Optional - dalam expander)
                        # with st.expander("📋 Lihat Tabel Detail Disabilitas"):
                        #     # Format angka dengan pemisah ribuan
                        #     df_display = df_disabilitas_filtered.copy()
                        #     for col in df_display.columns:
                        #         if col != 'nama_kecamatan':
                        #             df_display[col] = df_display[col].apply(lambda x: f"{int(x):,}".replace(",", "."))
                            
                        #     st.dataframe(
                        #         df_display,
                        #         use_container_width=True,
                        #         hide_index=True,
                        #         column_config={
                        #             "nama_kecamatan": "Kecamatan",
                        #             "disabilitas_fisik": "Fisik",
                        #             "disabilitas_intelektual": "Intelektual",
                        #             "disabilitas_mental": "Mental",
                        #             "disabilitas_sensorik_wicara": "Sensorik Wicara",
                        #             "disabilitas_sensorik_rungu": "Sensorik Rungu",
                        #             "disabilitas_sensorik_netra": "Sensorik Netra",
                        #             "total_disabilitas": "Total Disabilitas"
                        #         }
                        #     )
                    else:
                        st.info("💡 Tidak ada data disabilitas untuk kecamatan yang dipilih.")
                else:
                    st.info("💡 Data disabilitas belum tersedia untuk triwulan ini. Upload Excel dengan sheet SIDALIH WEB.")
            
            except Exception as e:
                st.warning(f"⚠️ Data disabilitas tidak dapat ditampilkan: {e}")
            
            #--- CHART BAWAH ---
            total_pemilih_l = df_pdpb['jumlah_pemilih_laki'].sum()
            total_pemilih_p = df_pdpb['jumlah_pemilih_perempuan'].sum()
            col_bar_tot, col_lp_keckel = st.columns([2, 1])
            
            # --- Hitung perbandingan dengan Triwulan Sebelumnya ---
            prev_laki = df_pdpb_before['laki'].sum()
            prev_perempuan = df_pdpb_before['perempuan'].sum()
            prev_total = prev_laki + prev_perempuan

            curr_laki = df_pdpb['jumlah_pemilih_laki'].sum() if not df_pdpb.empty else 0
            curr_perempuan = df_pdpb['jumlah_pemilih_perempuan'].sum() if not df_pdpb.empty else 0
            curr_total = curr_laki + curr_perempuan

            delta_laki = ((curr_laki - prev_laki) / prev_laki * 100) if prev_laki else 0
            delta_perempuan = ((curr_perempuan - prev_perempuan) / prev_perempuan * 100) if prev_perempuan else 0
            delta_total = ((curr_total - prev_total) / prev_total * 100) if prev_total else 0
            
            # with col_bar_tot:
            #     with st.container(border=True):
            #         st.markdown("### Grafik Total Pemilih")
            #         st.caption("Grafik perbandingan total pemilih laki-laki & perempuan pada triwulan terpilih pada Kabupaten Malang.")
                    
            #         data_khusus = pd.DataFrame({
            #             "Kategori": [" Laki-laki", " Perempuan"],
            #             "Jumlah": [total_pemilih_l, total_pemilih_p]
            #         })
            #         fig_total_pemilih = px.pie(
            #             data_khusus,
            #             values="Jumlah",
            #             names="Kategori",
            #             color_discrete_sequence=["#FFE797", "#A72703"],
            #             hole=0.3
            #             )
            #         fig_total_pemilih.update_layout(paper_bgcolor="#ffffff")
            #         st.plotly_chart(fig_total_pemilih, use_container_width=True)      
                    
            # with col_lp_keckel:
                
            #     selisih_l = curr_laki - prev_laki
            #     selisih_p = curr_perempuan - prev_perempuan
            #     selisih_tot = curr_total - prev_total
            #     if selisih_tot > 0:
            #         perubahan_kata = f"bertambah sejumlah {abs(int(selisih_tot)):,}"
            #     elif selisih_tot < 0:
            #         perubahan_kata = f"berkurang sejumlah {abs(int(selisih_tot)):,}"
            #     else:
            #         perubahan_kata = "tidak mengalami perubahan"

            #     col_atas_1, col_atas_2 = st.columns(2)
            #     with col_atas_1:
            #         with st.container(border=True):
            #             st.metric(
            #                 "Total Pemilih keseluruhan", 
            #                 f"{int(curr_total):,}".replace(",", "."),
            #                 delta=f"{int(selisih_tot):,} orang".replace(",", ".") if 'selisih_tot' in locals() else None,
            #                 # delta=f"{delta_total:+.2f}% ({int(selisih_tot):,} orang)".replace(",", "."),
            #                 delta_color="normal"
            #             )
            #         with st.container(border=True):
            #             inner_col1, inner_col2 = st.columns([4, 1], vertical_alignment="center")
            #             with inner_col1:
            #                 st.metric(
            #                     "Total Pemilih Laki-laki", 
            #                     f"{int(curr_laki):,}".replace(",", "."),
            #                     # delta=f"{delta_laki:+.2f}%",
            #                     delta=f"{int(selisih_l):,} orang".replace(",", "."),
            #                     delta_color="normal"
            #                 )
            #             with inner_col2:
            #                 st.image("https://img.icons8.com/ios-filled/50/000000/male.png", width=40)
            #         with st.container(border=True):
            #             inner_col1, inner_col2 = st.columns([4, 1], vertical_alignment="center")
            #             with inner_col1:
            #                 st.metric(
            #                     "Total Pemilih Perempuan", 
            #                     f"{int(curr_perempuan):,}".replace(",", "."),
            #                     # delta=f"{delta_perempuan:+.2f}%",
            #                     delta=f"{int(selisih_p):,} orang".replace(",", "."),
            #                     delta_color="normal"
            #                 )
            #             with inner_col2:
            #                 st.image("https://img.icons8.com/ios-filled/50/000000/female.png", width=40)
            #         with st.container(border=True): 
            #             st.metric("Total TPS", f"{int(df_pdpb_before['jumlah_tps'].sum()):,}".replace(",", "."))
            #     with col_atas_2:
            #         with st.container(border=True):
            #             st.metric("Total Kecamatan", f"{int(df_db_rekap['nama_kecamatan'].count()):,}".replace(",", "."))
            #         with st.container(border=True):
            #             st.metric("Desa / Kelurahan", f"{int(df_pdpb['jumlah_desa_kel'].sum()):,}".replace(",", "."))
            #         with st.container(border=True):
            #             st.markdown("#### Kesimpulan")
            #             st.caption(
            #                 f"""Berdasarkan triwulan yang dipilih :Total Pemilih pada triwulan ini adalah **{int(curr_total):,}** 
            #                     dengan rincian **{int(curr_laki):,}** Laki-laki dan **{int(curr_perempuan):,}** Perempuan. 
            #                     Terdapat perubahan sebesar **{perubahan_kata}** pada total pemilih dengan triwulan sebelumnya."""
            #                 )
            
            # CHART - PEMILIH BARU & PERBAIKAN DATA
            col_A_bar, col_B_metric = st.columns([2,4])
            with col_A_bar:
                with st.container(border=True):
                    st.markdown("### Pemilih Baru & Perbaikan Data")
                    
                    # Gunakan filtered jika tidak kosong, jika kosong pakai data lengkap
                    df_model_a_display = df_model_a_filtered if not df_model_a_filtered.empty else df_model_a
                    
                    total_baru = int(df_model_a_display['jumlah_pemilih_baru'].sum())
                    total_perbaikan = int(df_model_a_display['jumlah_perbaikan_data'].sum())
                        
                    data_baru = pd.DataFrame({
                        "Kategori": ["Pemilih Baru", "Perbaikan Data"],
                        "Jumlah": [total_baru, total_perbaikan]
                    })
                    fig1 = px.bar(
                        data_baru,
                        x="Kategori",
                        y="Jumlah",
                        color="Kategori",
                        text="Jumlah",
                        color_discrete_sequence=["#A72703", "#FFE797"]
                    )
                    fig1.update_traces(textposition="outside")
                    fig1.update_layout(
                        xaxis_title=None, yaxis_title=None,
                        plot_bgcolor="#ffffff",
                        paper_bgcolor="#ffffff",
                        barmode='group'
                    )
                    st.plotly_chart(fig1, use_container_width=True)
            with col_B_metric:
                with st.container(border=True):
                    st.markdown("### Detail Pemilih Baru & Perbaikan Data")
    
                    # Gunakan filtered jika tidak kosong
                    df_model_a_display = df_model_a_filtered if not df_model_a_filtered.empty else df_model_a
                    
                    detail_baru_wide = pd.DataFrame({
                        "Kecamatan": df_model_a_display['nama_kecamatan'].tolist(),
                        "Pemilih Baru": df_model_a_display['jumlah_pemilih_baru'].tolist(),
                        "Perbaikan Data": df_model_a_display['jumlah_perbaikan_data'].tolist()
                    })
                    
                    detail_baru_long = pd.melt(
                        detail_baru_wide,
                        id_vars=["Kecamatan"],                       
                        value_vars=["Pemilih Baru", "Perbaikan Data"],
                        var_name="Kategori",                       
                        value_name="Jumlah"                     
                    )

                    fig_detail = px.bar(
                        detail_baru_long, 
                        x="Kecamatan",          
                        y="Jumlah",            
                        color="Kategori",      
                        barmode='group',      
                        text="Jumlah",     
                        color_discrete_map={ 
                            "Pemilih Baru": "#A72703",
                            "Perbaikan Data": "#FFE797"
                        },
                        labels={"Jumlah": "Total", "Kategori": "Kategori"}
                    )
                    
                    fig_detail.update_traces(textposition="outside")
                    fig_detail.update_layout(
                        xaxis_title=None, 
                        yaxis_title=None,
                        plot_bgcolor="#ffffff",
                        paper_bgcolor="#ffffff",
                        xaxis_tickangle=-45 
                    )
                    
                    st.plotly_chart(fig_detail, use_container_width=True)
               
            # Chart - TMS
            
            col_chart_pie_tms, funnel_tms = st.columns(2)
            
            with col_chart_pie_tms:
                with st.container(border=True):
                    st.markdown("### Data Tidak Memenuhi Syarat (TMS)")
                    
                    # Gunakan filtered jika tidak kosong, jika kosong pakai data lengkap
                    df_db_rekap_display = df_db_rekap_filtered if not df_db_rekap_filtered.empty else df_db_rekap
                    
                    total_meninggal = int(df_db_rekap_display['tms_meninggal_l'].sum() + df_db_rekap_display['tms_meninggal_p'].sum())
                    total_dibawah = int(df_db_rekap_display['tms_dibawah_umur_l'].sum() + df_db_rekap_display['tms_dibawah_umur_p'].sum())
                    total_ganda = int(df_db_rekap_display['tms_ganda_l'].sum() + df_db_rekap_display['tms_ganda_p'].sum())
                    total_pindah = int(df_db_rekap_display['tms_pindah_keluar_l'].sum() + df_db_rekap_display['tms_pindah_keluar_p'].sum())
                    total_tni = int(df_db_rekap_display['tms_tni_l'].sum() + df_db_rekap_display['tms_tni_p'].sum())
                    
                    total_tms = total_meninggal + total_dibawah + total_ganda + total_pindah + total_tni
                    
                    data_khusus = pd.DataFrame({
                        "Kategori": [" Meninggal", " Di Bawah Umur", " Ganda", " Pindah Keluar", " TNI"],
                        "Jumlah": [total_meninggal, total_dibawah, total_ganda, total_pindah, total_tni]
                    })
                    fig2 = px.pie(
                        data_khusus,
                        values="Jumlah",
                        names="Kategori",
                        color_discrete_sequence=["#ff595e", "#ff924c", "#ffca3a", "#8ac926", "#1982c4"],
                        hole=0.4
                        )
                    fig2.update_layout(paper_bgcolor="#ffffff")
                    st.plotly_chart(fig2, use_container_width=True)
            with funnel_tms:
                with st.container(border=True):    
                    st.markdown("### Rank Tidak Memenuhi Syarat (TMS)")
                    data_funnel_sorted = data_khusus.sort_values(by="Jumlah", ascending=False)
                    
                    fig_funnel = px.funnel_area(
                        data_funnel_sorted,
                        names="Kategori",
                        values="Jumlah",
                        color_discrete_sequence=["#ff595e", "#ff924c", "#ffca3a", "#8ac926", "#1982c4"]
                    )
                    fig_funnel.update_traces(
                        textinfo="value",           # Tampilkan angka, bukan persen
                        textfont=dict(size=17)
                    )
                    fig_funnel.update_layout(paper_bgcolor="#ffffff")
                    st.plotly_chart(fig_funnel, use_container_width=True)
            
            col_detail_tms_1, col_detail_tms_2 ,col_detail_tms_3, col_detail_tms_4, col_detail_tms_5, col_detail_tms_6 = st.columns([1,1,1,1,1,2])
            
            # Gunakan filtered jika tidak kosong
            df_db_rekap_display = df_db_rekap_filtered if not df_db_rekap_filtered.empty else df_db_rekap
            
            with col_detail_tms_1:
                with st.container(border=True):
                    pindah_keluar_l = df_db_rekap_display['tms_pindah_keluar_l'].sum()
                    pindah_keluar_p = df_db_rekap_display['tms_pindah_keluar_p'].sum()
                    st.metric("🚚 Pindah Keluar", f"{int(pindah_keluar_l+pindah_keluar_p):,}".replace(",", "."))
            with col_detail_tms_2:
                with st.container(border=True):
                    ganda_l = df_db_rekap_display['tms_ganda_l'].sum()
                    ganda_p = df_db_rekap_display['tms_ganda_p'].sum()
                    st.metric("🧍‍♂️ Ganda", f"{int(ganda_l+ganda_p):,}".replace(",", "."))
            with col_detail_tms_3:
                with st.container(border=True):
                    dibawah_umur_l = df_db_rekap_display['tms_dibawah_umur_l'].sum()
                    dibawah_umur_p = df_db_rekap_display['tms_dibawah_umur_p'].sum()
                    st.metric("👶 Di Bawah Umur", f"{int(dibawah_umur_l+dibawah_umur_p):,}".replace(",", "."))
            with col_detail_tms_4:
                with st.container(border=True):
                    meninggal_l = df_db_rekap_display['tms_meninggal_l'].sum()
                    meninggal_p = df_db_rekap_display['tms_meninggal_p'].sum()
                    st.metric("☠️ Meninggal", f"{int(meninggal_l+meninggal_p):,}".replace(",", "."))
            with col_detail_tms_5:
                with st.container(border=True):
                    tni_l = df_db_rekap_display['tms_tni_l'].sum()
                    tni_p = df_db_rekap_display['tms_tni_p'].sum()
                    st.metric("🎖️ TNI", f"{int(tni_l+tni_p):,}".replace(",", "."))
            with col_detail_tms_6:
                with st.container(border=True):
                    st.metric("Total TMS", f"{int(total_meninggal+total_dibawah+total_ganda+total_pindah+total_tni):,}".replace(",", "."))
            
            # with st.expander("📋 Data PDPB Triwulan Sebelumnya", expanded=False):
            #     st.subheader("Data PDPB Triwulan Sebelumnya")
            #     st.dataframe(df_pdpb_before, use_container_width=True)
            # with st.expander("📋 Data REKAPITULASI PDPB", expanded=False):
            #     st.subheader("Data REKAPITULASI PDPB")
            #     st.dataframe(df_pdpb, use_container_width=True)
            # with st.expander("📋 Data REKAP MODEL A"):
            #     st.subheader("Data REKAP MODEL A")
            #     st.dataframe(df_model_a, use_container_width=True)
            # with st.expander("📋 Data DB REKAP MODEL A", expanded=False):
            #     st.subheader("Data DB REKAP MODEL A")
            #     st.dataframe(df_db_rekap, use_container_width=True)
        else:
            info_box.info("Silakan **pilih data Triwulan** terlebih dahulu pada panel sebelah kiri & **Upload** jika diperlukan.")
    else:
        st.sidebar.info("Belum ada data Triwulan yang tersimpan di database.")
except Exception as e:
    st.sidebar.error(f"Gagal mengambil daftar Triwulan: {e}")

st.sidebar.markdown("---")
st.sidebar.markdown("### 📂 Upload data Triwulan ")
uploaded_file = st.sidebar.file_uploader("Upload file Excel MODEL-A REKAP PDPB Kabupaten Malang", type=["xlsx"])

# --- Proses Utama ---
if uploaded_file:
    st.toast(f"File **{uploaded_file.name}** telah diunggah. Klik tombol di bawah untuk memulai.")
    triwulan_data = extract_triwulan_info(uploaded_file)
    
    if triwulan_data:
        st.subheader(f"Data PDPB Triwulan ke {triwulan_data['triwulan_ke']} Tahun {triwulan_data['tahun']}")

        # if st.button("Hanya simpan Data Triwulan ke Database"):
        #     # engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
        #     id_triwulan = insert_or_get_triwulan_id(engine, triwulan_data)
        #     st.json(triwulan_data)
            
        #     if id_triwulan is not None:
        #         st.write(f"ID Triwulan yang akan digunakan untuk data utama adalah: **{id_triwulan}**")
        #     else:
        #         st.error("Proses penyimpanan Triwulan gagal. Cek log database Anda.")
                
    if st.button("🚀 Proses File dan Simpan ke Database", type="primary"):
        try:
            with st.spinner("Menghubungkan ke database dan memproses file..."):

                # --- Ambil data triwulan ---
                triwulan_data = extract_triwulan_info(uploaded_file)
                if not triwulan_data:
                    st.error("Tidak bisa melanjutkan tanpa data Triwulan yang valid.")
                    st.stop()

                id_triwulan = insert_or_get_triwulan_id(engine, triwulan_data)
                if id_triwulan is None:
                    st.error("Gagal mendapatkan ID Triwulan dari database.")
                    st.stop()

                # --- PDPB (TRIWULAN SEBELUMNYA) ---
                st.toast("Mencari sheet `PDPB sebelumnya`...")
                xls = pd.ExcelFile(uploaded_file, engine="openpyxl")
                
                first_sheet = xls.sheet_names[0]
                
                if first_sheet in xls.sheet_names:
                    df_pdpb_t_raw = pd.read_excel(xls, sheet_name=first_sheet, header=0, engine='openpyxl')
                    df_pdpb_t_clean = clean_and_map_pdpb_t2(df_pdpb_t_raw)
                    df_pdpb_t_clean['id_triwulan'] = id_triwulan
                    
                    df_pdpb_t2_clean = df_pdpb_t_clean.drop(columns=['TOTAL'], errors='ignore')
                    
                    with engine.begin() as conn:
                        try:
                            # conn.execute(text("TRUNCATE TABLE triwulan_sebelumnya RESTART IDENTITY CASCADE;"))
                            df_pdpb_t_clean.to_sql('triwulan_sebelumnya', conn, if_exists='append', index=False)
                            st.toast(f"✅ {len(df_pdpb_t_clean)} baris data disimpan ke `triwulan_sebelumnya`.")
                        except SQLAlchemyError as e:
                            st.error(f"Gagal menyimpan ke `triwulan_sebelumnya`. Pastikan tabel ada. Error: {e}")
                else:
                    st.warning("Sheet PDPB Triwulan sebelumnya tidak ditemukan dalam file Excel.")
                        
                # --- REKAPITULASI PDPB ---
                df_pdpb_raw = pd.read_excel(uploaded_file, sheet_name='REKAPITULASI PDPB', skiprows=9, engine='openpyxl')
                df_pdpb_raw.rename(columns={'Jumlah Pemilih': 'L', 'Unnamed: 4': 'P', 'Unnamed: 5': 'L + P'}, inplace=True)
                df_pdpb_raw = df_pdpb_raw.drop(df_pdpb_raw.index[0]).reset_index(drop=True)
                df_pdpb_clean = clean_and_map_rekapitulasi_pdpb(df_pdpb_raw)
                df_pdpb_clean['id_triwulan'] = id_triwulan

                with engine.begin() as conn:
                    # conn.execute(text("TRUNCATE TABLE rekapitulasi_pdpb RESTART IDENTITY CASCADE;"))
                    # 1. Check apakah data sudah ada
                    existing = pd.read_sql(
                        text("SELECT id_triwulan FROM rekapitulasi_pdpb WHERE id_triwulan = :id"),
                        conn, params={"id": id_triwulan}
                    )
                    
                    # 2. Insert hanya jika BELUM ada
                    if existing.empty:
                        df_pdpb_clean.to_sql('rekapitulasi_pdpb', conn, if_exists='append', index=False)
                        # st.toast("✅ Data disimpan")
                        st.toast(f"✅ {len(df_pdpb_clean)} baris data disimpan ke `rekapitulasi_pdpb`.")
                    else:
                        st.warning("⚠️ Data rekapitulasi pdpb sudah ada — tidak disimpan ulang.")

                # --- REKAP MODEL A ---
                df_rekap_a_raw = pd.read_excel(uploaded_file, sheet_name='REKAP MODEL A', skiprows=8, engine='openpyxl')
                df_rekap_a_clean = clean_and_map_rekap_model_a(df_rekap_a_raw)
                df_rekap_a_clean['id_triwulan'] = id_triwulan

                with engine.begin() as conn:
                    # conn.execute(text("TRUNCATE TABLE rekap_model_a RESTART IDENTITY CASCADE;"))
                    
                    # 1. Check apakah data sudah ada
                    existing = pd.read_sql(
                        text("SELECT id_triwulan FROM rekap_model_a WHERE id_triwulan = :id"),
                        conn, params={"id": id_triwulan}
                    )
                    
                    # 2. Insert hanya jika BELUM ada
                    if existing.empty:
                        df_rekap_a_clean.to_sql('rekap_model_a', conn, if_exists='append', index=False)
                        st.toast(f"✅ {len(df_rekap_a_clean)} baris data disimpan ke `rekap_model_a`.")
                        # st.toast("✅ Data disimpan")
                    else:
                        st.warning("⚠️ Data rekap model A sudah ada — tidak disimpan ulang.")

                # --- DB REKAP MODEL A ---
                xls = pd.ExcelFile(uploaded_file, engine="openpyxl")
                df_db_rekap_raw = pd.read_excel(xls, sheet_name='DB REKAP MODEL A', header=[8, 9, 10])
                df_db_rekap_clean = clean_and_map_db_rekap_model_a(df_db_rekap_raw)
                df_db_rekap_clean['id_triwulan'] = id_triwulan
                
                # --- DETAIL DISABILITAS ---
                try:
                    df_disabilitas_raw = pd.read_excel(
                        uploaded_file,
                        sheet_name='SIDALIH WEB',
                        skiprows=141,  # Skip sampai row 142 (header ada di row 142)
                        header=0,
                        engine='openpyxl'
                    )
                    
                    # Clean dan map data disabilitas
                    df_disabilitas_clean = clean_and_map_disabilitas(df_disabilitas_raw)
                    
                    st.success(f"✅ Data disabilitas berhasil dibaca: {len(df_disabilitas_clean)} kecamatan")
                    
                    # Preview data (optional)
                    with st.expander("📊 Preview Data Disabilitas"):
                        st.dataframe(df_disabilitas_clean, use_container_width=True)
                        
                        # Tampilkan total per kategori
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Fisik", int(df_disabilitas_clean['disabilitas_fisik'].sum()))
                        with col2:
                            st.metric("Intelektual", int(df_disabilitas_clean['disabilitas_intelektual'].sum()))
                        with col3:
                            st.metric("Mental", int(df_disabilitas_clean['disabilitas_mental'].sum()))
                    
                except Exception as e:
                    st.warning(f"⚠️ Sheet SIDALIH WEB tidak ditemukan atau error: {e}")
                    st.info("💡 Data disabilitas akan dilewati. Pastikan sheet SIDALIH WEB ada di file Excel.")
                    df_disabilitas_clean = None

                if df_disabilitas_clean is not None and not df_disabilitas_clean.empty:
                    df_disabilitas_clean['id_triwulan'] = id_triwulan
                    
                    with engine.begin() as conn:
                        # Check duplikasi
                        existing = pd.read_sql(
                            text("SELECT id_triwulan FROM detail_disabilitas WHERE id_triwulan = :id"),
                            conn, params={"id": id_triwulan}
                        )
                        
                        # Insert hanya jika BELUM ada
                        if existing.empty:
                            df_disabilitas_clean.to_sql('detail_disabilitas', conn, if_exists='append', index=False)
                            
                            total_disabilitas = (
                                df_disabilitas_clean['disabilitas_fisik'].sum() +
                                df_disabilitas_clean['disabilitas_intelektual'].sum() +
                                df_disabilitas_clean['disabilitas_mental'].sum() +
                                df_disabilitas_clean['disabilitas_sensorik_wicara'].sum() +
                                df_disabilitas_clean['disabilitas_sensorik_rungu'].sum() +
                                df_disabilitas_clean['disabilitas_sensorik_netra'].sum()
                            )
                            
                            st.toast(f"✅ {len(df_disabilitas_clean)} baris data disabilitas disimpan.")
                            st.success(f"📊 Total Pemilih Disabilitas: **{int(total_disabilitas):,}**".replace(",", "."))
                        else:
                            st.warning("⚠️ Data disabilitas sudah ada — tidak disimpan ulang.")
            
                with engine.begin() as conn:
                    # conn.execute(text("TRUNCATE TABLE db_rekap_model_a RESTART IDENTITY CASCADE;"))
                    
                    # 1. Check apakah data sudah ada
                    existing = pd.read_sql(
                        text("SELECT id_triwulan FROM db_rekap_model_a WHERE id_triwulan = :id"),
                        conn, params={"id": id_triwulan}
                    )
                    
                    # 2. Insert hanya jika BELUM ada
                    if existing.empty:
                        df_db_rekap_clean.to_sql('db_rekap_model_a', conn, if_exists='append', index=False)
                        st.toast(f"✅ {len(df_db_rekap_clean)} baris data disimpan ke `db_rekap_model_a`.")
                        # st.toast("✅ Data disimpan")
                    else:
                        st.warning("⚠️ Data db rekap model a sudah ada — tidak disimpan ulang.")

                # --- DATAFRAME ---
                st.subheader("PDPB TRIWULAN SEBELUMNYA")
                st.dataframe(df_pdpb_t_clean)
            
                st.subheader("REKAPITULASI PDPB")
                st.dataframe(df_pdpb_clean)

                st.subheader("REKAP MODEL A")
                st.dataframe(df_rekap_a_clean)

                st.subheader("DB REKAP MODEL A")
                st.dataframe(df_db_rekap_clean)
                
                st.subheader("Data Disabilitas (Jika Tersedia)")
                if df_disabilitas_clean is not None:
                    st.dataframe(df_disabilitas_clean)

            st.success("🎉 Semua data berhasil disimpan dan terhubung dengan Triwulan.")
            st.balloons()

            # --- SIMPAN KE REKAPITULASI PDPB (CEK DUPLIKAT) ---
            existing = pd.read_sql(
            text("SELECT id_triwulan FROM rekapitulasi_pdpb WHERE id_triwulan = :id"),
            conn, params={"id": id_triwulan}
            )
            if existing.empty:
                df_pdpb_clean.to_sql('rekapitulasi_pdpb', conn, if_exists='append', index=False)
            else:
                st.warning("Data untuk triwulan ini sudah ada di tabel rekapitulasi_pdpb.")
                
        except Exception as e:
            st.error(f"Terjadi kesalahan: {e}")
            st.warning("Pastikan nama sheet dan format Excel sesuai.")