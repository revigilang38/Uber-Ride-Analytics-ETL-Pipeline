## Uber Ride Analytics ETL Pipeline

### Deskripsi Proyek

Proyek ini adalah sebuah **pipeline ETL (Extract, Transform, Load)** yang dirancang untuk mengotomatisasi pemrosesan data perjalanan Uber. Pipeline ini menggunakan **Apache Airflow** sebagai orkestrator, **PostgreSQL** sebagai database staging, dan **Elasticsearch** sebagai tempat penyimpanan data akhir untuk analisis lebih lanjut. Data yang sudah diproses di Elasticsearch kemudian dapat divisualisasikan menggunakan **Kibana**.

### Tujuan

Tujuan utama dari proyek ini adalah untuk:

- Mengekstrak data mentah dari file CSV.
- Mentransformasi dan membersihkan data (menghilangkan duplikat, menangani missing value, dan normalisasi kolom).
- Memuat data yang sudah bersih ke dalam Elasticsearch, membuatnya siap untuk diindeks dan dianalisis.
- **Menyediakan data untuk visualisasi dan pembuatan dashboard interaktif menggunakan Kibana.**
- Menjadwalkan seluruh proses secara otomatis menggunakan Airflow DAG (Directed Acyclic Graph).

### Teknologi yang Digunakan

- **Apache Airflow**: Untuk orkestrasi dan penjadwalan alur kerja ETL.
- **PostgreSQL**: Digunakan sebagai database sementara untuk menyimpan data mentah sebelum proses transformasi.
- **Pandas**: Library Python untuk manipulasi dan pembersihan data.
- **Psycopg2**: Adapter Python untuk berinteraksi dengan database PostgreSQL.
- **Elasticsearch**: Database NoSQL yang digunakan sebagai tujuan akhir untuk menyimpan data yang sudah bersih.
- **Kibana**: Tools visualisasi yang digunakan untuk menganalisis dan menampilkan data yang ada di Elasticsearch.

### Struktur DAG (Directed Acyclic Graph)

Alur kerja (DAG) dalam proyek ini terdiri dari beberapa task yang berjalan secara sekuensial:

1. **`create_table_in_postgres`**: Task ini bertanggung jawab untuk membuat tabel `table_m3` di PostgreSQL jika belum ada, atau menghapusnya dan membuatnya ulang untuk memastikan skema yang bersih.
2. **`insert_data_to_postgres`**: Task ini membaca data dari file CSV mentah dan memasukkannya ke dalam tabel `table_m3` di PostgreSQL.
3. **`fetch_from_postgres`**: Task ini mengambil data dari PostgreSQL dan mengirimkannya ke task berikutnya melalui XCom (Cross-Communication).
4. **`data_cleaning`**: Task ini melakukan berbagai operasi pembersihan data menggunakan Pandas, termasuk menghapus duplikat, menormalisasi nama kolom, dan menangani missing value pada kolom numerik dan kategorikal.
5. **`post_to_elasticsearch`**: Task terakhir ini memuat data yang sudah bersih dari file CSV yang telah dibuat di task sebelumnya ke dalam indeks Elasticsearch, yang disebut `uber_ride_analytics_clean`.
