import streamlit as st
import pandas as pd
import re
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import plotly.express as px

db_url = st.secrets["db_pdpb"]["url"]
engine = create_engine(db_url)

# host = "localhost"
# user = "postgres"
# password = "admin"
# database = "db_pdpb"
# port = 5432

def simpan_ke_database(engine, df_pdpb_clean, df_triwulan_sebelumnya, df_rekap_model_a, df_db_rekap_model_a, id_triwulan):
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
        'MALANG': 'nama_kecamatan',
        'TPS': 'jumlah_tps',
        'LK': 'laki',
        'PR': 'perempuan',
        'L + P': 'total'
    }
    df.rename(columns=column_mapping, inplace=True)
    
    final_columns = ['nama_kecamatan', 'jumlah_tps', 'laki', 'perempuan']
    df_final = df[[col for col in final_columns if col in df.columns]]

    return df_final.dropna(subset=['nama_kecamatan'])

# --- Homepage ---
st.set_page_config(page_title="Infografis PDPB KPU Kabupaten Malang", layout="wide")
st.header("Infografis PDPB KPU Kabupaten Malang")
st.markdown("---")

# --- Sidebar ---
st.sidebar.write(f"<span style='font-weight:bold;'>Pilih data triwulan atau upload file MODEL-A REKAP PDPB Kabupaten Malang.</span>", unsafe_allow_html=True)
# engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
st.sidebar.markdown("### 📅 Pilih Triwulan yang Sudah Terupload")

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
        if st.sidebar.button("📊 Tampilkan Data"):
            with engine.connect() as conn:
                df_pdpb_before = pd.read_sql(
                    text("SELECT nama_kecamatan,jumlah_tps,laki,perempuan,total FROM triwulan_sebelumnya WHERE id_triwulan = :id"),
                    conn, 
                    params={"id": selected_id}
                )
                df_pdpb = pd.read_sql(
                    text("SELECT nama_kecamatan,jumlah_desa_kel,jumlah_pemilih_laki,jumlah_pemilih_perempuan FROM rekapitulasi_pdpb WHERE id_triwulan = :id"),
                    conn, 
                    params={"id": selected_id}
                )

                df_model_a = pd.read_sql(
                    text("SELECT nama_kecamatan,jumlah_desa_kel,jumlah_pemilih_baru,jumlah_pemilih_tms,jumlah_perbaikan_data FROM rekap_model_a WHERE id_triwulan = :id"), 
                    conn, 
                    params={"id": selected_id}
                )

                df_db_rekap = pd.read_sql(
                    text("SELECT nama_kecamatan,pemilih_baru_l,pemilih_baru_p,tms_meninggal_l,tms_meninggal_p,tms_dibawah_umur_l,tms_dibawah_umur_p,tms_ganda_l,tms_ganda_p,tms_pindah_keluar_l,tms_pindah_keluar_p,tms_tni_l,tms_tni_p FROM db_rekap_model_a WHERE id_triwulan = :id"), 
                    conn, 
                    params={"id": selected_id}
                )
            st.toast(f"Menampilkan data **{selected_label}**")
            
            #--- CHART ATAS ---
            st.markdown(f"<h2 style='text-align: center;'>Data {selected_label}</h2>", unsafe_allow_html=True)

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

            with col_bar_tot:
                with st.container(border=True):
                    st.markdown("### Grafik Total Pemilih")
                    st.caption("Grafik perbandingan total pemilih laki-laki & perempuan pada triwulan terpilih pada Kabupaten Malang.")
                    
                    data_khusus = pd.DataFrame({
                        "Kategori": [" Laki-laki", " Perempuan"],
                        "Jumlah": [total_pemilih_l, total_pemilih_p]
                    })
                    fig_total_pemilih = px.pie(
                        data_khusus,
                        values="Jumlah",
                        names="Kategori",
                        color_discrete_sequence=["#FFE797", "#A72703"],
                        hole=0.3
                        )
                    fig_total_pemilih.update_layout(paper_bgcolor="#ffffff")
                    st.plotly_chart(fig_total_pemilih, use_container_width=True)
                    
            with col_lp_keckel:
                
                selisih_l = curr_laki - prev_laki
                selisih_p = curr_perempuan - prev_perempuan
                selisih_tot = curr_total - prev_total
                if selisih_tot > 0:
                    perubahan_kata = f"bertambah sejumlah {abs(int(selisih_tot)):,}"
                elif selisih_tot < 0:
                    perubahan_kata = f"berkurang sejumlah {abs(int(selisih_tot)):,}"
                else:
                    perubahan_kata = "tidak mengalami perubahan"

                col_atas_1, col_atas_2 = st.columns(2)
                with col_atas_1:
                    with st.container(border=True):
                        st.metric(
                            "Total Pemilih keseluruhan", 
                            f"{int(curr_total):,}".replace(",", "."),
                            delta=f"{int(selisih_tot):,} orang".replace(",", ".") if 'selisih_tot' in locals() else None,
                            # delta=f"{delta_total:+.2f}% ({int(selisih_tot):,} orang)".replace(",", "."),
                            delta_color="normal"
                        )
                    with st.container(border=True):
                        inner_col1, inner_col2 = st.columns([4, 1], vertical_alignment="center")
                        with inner_col1:
                            st.metric(
                                "Total Pemilih Laki-laki", 
                                f"{int(curr_laki):,}".replace(",", "."),
                                # delta=f"{delta_laki:+.2f}%",
                                delta=f"{int(selisih_l):,} orang".replace(",", "."),
                                delta_color="normal"
                            )
                        with inner_col2:
                            st.image("https://img.icons8.com/ios-filled/50/000000/male.png", width=40)
                    with st.container(border=True):
                        inner_col1, inner_col2 = st.columns([4, 1], vertical_alignment="center")
                        with inner_col1:
                            st.metric(
                                "Total Pemilih Perempuan", 
                                f"{int(curr_perempuan):,}".replace(",", "."),
                                # delta=f"{delta_perempuan:+.2f}%",
                                delta=f"{int(selisih_p):,} orang".replace(",", "."),
                                delta_color="normal"
                            )
                        with inner_col2:
                            st.image("https://img.icons8.com/ios-filled/50/000000/female.png", width=40)
                    with st.container(border=True): 
                        st.metric("Total TPS", f"{int(df_pdpb_before['jumlah_tps'].sum()):,}".replace(",", "."))
                with col_atas_2:
                    with st.container(border=True):
                        st.metric("Total Kecamatan", f"{int(df_db_rekap['nama_kecamatan'].count()):,}".replace(",", "."))
                    with st.container(border=True):
                        st.metric("Desa / Kelurahan", f"{int(df_pdpb['jumlah_desa_kel'].sum()):,}".replace(",", "."))
                    with st.container(border=True):
                        st.markdown("#### Kesimpulan")
                        st.caption(
                            f"""Berdasarkan triwulan yang dipilih :Total Pemilih pada triwulan ini adalah **{int(curr_total):,}** 
                                dengan rincian **{int(curr_laki):,}** Laki-laki dan **{int(curr_perempuan):,}** Perempuan. 
                                Terdapat perubahan sebesar **{perubahan_kata}** pada total pemilih dengan triwulan sebelumnya."""
                            )
            
            # CHART - PEMILIH BARU & PERBAIKAN DATA
            col_A_bar, col_B_metric = st.columns([2,4])
            with col_A_bar:
                with st.container(border=True):
                    st.markdown("### Pemilih Baru & Perbaikan Data")
                    total_baru = int(df_model_a['jumlah_pemilih_baru'].sum())
                    total_perbaikan = int(df_model_a['jumlah_perbaikan_data'].sum())
                        
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
    
                    detail_baru_wide = pd.DataFrame({
                        "Kecamatan": df_model_a['nama_kecamatan'].tolist(),
                        "Pemilih Baru": df_model_a['jumlah_pemilih_baru'].tolist(),
                        "Perbaikan Data": df_model_a['jumlah_perbaikan_data'].tolist()
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
                    total_meninggal = int(df_db_rekap['tms_meninggal_l'].sum() + df_db_rekap['tms_meninggal_p'].sum())
                    total_dibawah = int(df_db_rekap['tms_dibawah_umur_l'].sum() + df_db_rekap['tms_dibawah_umur_p'].sum())
                    total_ganda = int(df_db_rekap['tms_ganda_l'].sum() + df_db_rekap['tms_ganda_p'].sum())
                    total_pindah = int(df_db_rekap['tms_pindah_keluar_l'].sum() + df_db_rekap['tms_pindah_keluar_p'].sum())
                    total_tni = int(df_db_rekap['tms_tni_l'].sum() + df_db_rekap['tms_tni_p'].sum())
                    
                    total_tms = total_meninggal + total_dibawah + total_ganda + total_pindah + total_tni
                    
                    data_khusus = pd.DataFrame({
                        "Kategori": [" Meninggal", " Di Bawah Umur", " Ganda", " Pindah Keluar", " TNI"],
                        "Jumlah": [total_meninggal, total_dibawah, total_ganda, total_pindah, total_tni]
                    })
                    fig2 = px.pie(
                        data_khusus,
                        values="Jumlah",
                        names="Kategori",
                        color_discrete_sequence=["#FF5656", "#FFA239", "#FEEE91", "#FFF2C6", "#8CE4FF"],
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
                        color_discrete_sequence=["#FF5656", "#FFA239", "#FEEE91", "#FFF2C6", "#8CE4FF"]
                    )
                    fig_funnel.update_traces(
                        textinfo="value",           # Tampilkan angka, bukan persen
                        textfont=dict(size=17)
                    )
                    fig_funnel.update_layout(paper_bgcolor="#ffffff")
                    st.plotly_chart(fig_funnel, use_container_width=True)
            
            col_detail_tms_1, col_detail_tms_2 ,col_detail_tms_3, col_detail_tms_4, col_detail_tms_5, col_detail_tms_6 = st.columns([1,1,1,1,1,2])
            with col_detail_tms_1:
                with st.container(border=True):
                    pindah_keluar_l = df_db_rekap['tms_pindah_keluar_l'].sum()
                    pindah_keluar_p = df_db_rekap['tms_pindah_keluar_p'].sum()
                    st.metric("🚚 Pindah Keluar", f"{int(pindah_keluar_l+pindah_keluar_p):,}".replace(",", "."))
            with col_detail_tms_2:
                with st.container(border=True):
                    ganda_l = df_db_rekap['tms_ganda_l'].sum()
                    ganda_p = df_db_rekap['tms_ganda_p'].sum()
                    st.metric("🧍‍♂️ Ganda", f"{int(ganda_l+ganda_p):,}".replace(",", "."))
            with col_detail_tms_3:
                with st.container(border=True):
                    dibawah_umur_l = df_db_rekap['tms_dibawah_umur_l'].sum()
                    dibawah_umur_p = df_db_rekap['tms_dibawah_umur_p'].sum()
                    st.metric("👶 Di Bawah Umur", f"{int(dibawah_umur_l+dibawah_umur_p):,}".replace(",", "."))
            with col_detail_tms_4:
                with st.container(border=True):
                    meninggal_l = df_db_rekap['tms_meninggal_l'].sum()
                    meninggal_p = df_db_rekap['tms_meninggal_p'].sum()
                    st.metric("☠️ Meninggal", f"{int(meninggal_l+meninggal_p):,}".replace(",", "."))
            with col_detail_tms_5:
                with st.container(border=True):
                    tni_l = df_db_rekap['tms_tni_l'].sum()
                    tni_p = df_db_rekap['tms_tni_p'].sum()
                    st.metric("🎖️ TNI", f"{int(tni_l+tni_p):,}".replace(",", "."))
            with col_detail_tms_6:
                with st.container(border=True):
                    st.metric("Total TMS", f"{int(total_meninggal+total_dibawah+total_ganda+total_pindah+total_tni):,}".replace(",", "."))
            
            st.subheader("Data PDPB Triwulan Sebelumnya")
            st.dataframe(df_pdpb_before, use_container_width=True)
            st.subheader("Data REKAPITULASI PDPB")
            st.dataframe(df_pdpb, use_container_width=True)
            st.subheader("Data REKAP MODEL A")
            st.dataframe(df_model_a, use_container_width=True)
            st.subheader("Data DB REKAP MODEL A")
            st.dataframe(df_db_rekap, use_container_width=True)
        else:
            info_box.info("Silakan **pilih data Triwulan** terlebih dahulu pada panel sebelah kiri & **Upload** jika diperlukan.")
    else:
        st.sidebar.info("Belum ada data Triwulan yang tersimpan di database.")
except Exception as e:
    st.sidebar.error(f"Gagal mengambil daftar Triwulan: {e}")

st.sidebar.markdown("---")
st.sidebar.markdown("### 📂 Upload data Triwulan ")
uploaded_file = st.sidebar.file_uploader("Upload file Excel", type=["xlsx"])

# --- Proses Utama ---
if uploaded_file:
    st.toast(f"File **{uploaded_file.name}** telah diunggah. Klik tombol di bawah untuk memulai.")
    triwulan_data = extract_triwulan_info(uploaded_file)
    
    if triwulan_data:
        st.subheader(f"Data PDPB Triwulan ke {triwulan_data['triwulan_ke']} Tahun {triwulan_data['tahun']}")

        if st.button("Simpan Data Triwulan ke Database"):
            # engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')
            id_triwulan = insert_or_get_triwulan_id(engine, triwulan_data)
            st.json(triwulan_data)
            
            if id_triwulan is not None:
                st.write(f"ID Triwulan yang akan digunakan untuk data utama adalah: **{id_triwulan}**")
            else:
                st.error("Proses penyimpanan Triwulan gagal. Cek log database Anda.")
                
    if st.button("🚀 Proses File dan Simpan ke Database", type="primary"):
        try:
            # engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

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
                    df_pdpb_clean.to_sql('rekapitulasi_pdpb', conn, if_exists='append', index=False)
                st.toast(f"✅ {len(df_pdpb_clean)} baris data disimpan ke `rekapitulasi_pdpb`.")

                # --- REKAP MODEL A ---
                df_rekap_a_raw = pd.read_excel(uploaded_file, sheet_name='REKAP MODEL A', skiprows=8, engine='openpyxl')
                df_rekap_a_clean = clean_and_map_rekap_model_a(df_rekap_a_raw)
                df_rekap_a_clean['id_triwulan'] = id_triwulan

                with engine.begin() as conn:
                    # conn.execute(text("TRUNCATE TABLE rekap_model_a RESTART IDENTITY CASCADE;"))
                    df_rekap_a_clean.to_sql('rekap_model_a', conn, if_exists='append', index=False)
                st.toast(f"✅ {len(df_rekap_a_clean)} baris data disimpan ke `rekap_model_a`.")

                # --- DB REKAP MODEL A ---
                xls = pd.ExcelFile(uploaded_file, engine="openpyxl")
                df_db_rekap_raw = pd.read_excel(xls, sheet_name='DB REKAP MODEL A', header=[8, 9, 10])
                df_db_rekap_clean = clean_and_map_db_rekap_model_a(df_db_rekap_raw)
                df_db_rekap_clean['id_triwulan'] = id_triwulan
                
                with engine.begin() as conn:
                    # conn.execute(text("TRUNCATE TABLE db_rekap_model_a RESTART IDENTITY CASCADE;"))
                    df_db_rekap_clean.to_sql('db_rekap_model_a', conn, if_exists='append', index=False)
                st.toast(f"✅ {len(df_db_rekap_clean)} baris data disimpan ke `db_rekap_model_a`.")

                # --- DATAFRAME ---
                st.subheader("PDPB TRIWULAN SEBELUMNYA")
                st.dataframe(df_pdpb_t_clean)
            
                st.subheader("REKAPITULASI PDPB")
                st.dataframe(df_pdpb_clean)

                st.subheader("REKAP MODEL A")
                st.dataframe(df_rekap_a_clean)

                st.subheader("DB REKAP MODEL A")
                st.dataframe(df_db_rekap_clean)

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
    
