# Proyek ETL dengan Apache Airflow dan PostgreSQL

## Deskripsi
Ini adalah repositori untuk proyek ETL yang menggunakan **Apache Airflow** dan **PostgreSQL**. Proyek ini mencakup pengaturan DAG untuk fully ingest data dan daily serta backdate data , memodelkan data ke dalam skema database, dan membuat data mart.

## Fitur Utama
- **Automatisasi Proses ETL:** Memungkinkan pemrosesan data dari berbagai sumber dengan cara yang terstruktur.
- **Menyimpan dan Memodelkan Data Penjualan:** Data diolah dan disimpan dalam skema PostgreSQL untuk analisis lebih lanjut.
- **Docker:** Menyediakan lingkungan pengembangan yang konsisten dengan menggunakan Docker dan Docker Compose.

## Prasyarat
Sebelum memulai, pastikan Anda memiliki hal-hal berikut:
- **Docker**
- **Docker Compose**
- **PostgreSQL**
- **Apache Airflow**

## Instalasi
1. **Clone repositori ini:**
   ```bash
   git clone <URL_REPOSITORI>
   cd <NAMA_FOLDER_REPOSITORI>
2. **buat jaringan docker**
    docker network create ridho
3. **jalankan docker compose**
    docker-compose up -d --build