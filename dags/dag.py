from __future__ import annotations

import pendulum
import psycopg2
import pandas as pd
import csv
import os
from elasticsearch import Elasticsearch

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def create_table_in_postgres():
    '''
    Fungsi ini membuat table m_3
    '''
    db_params = {
        'dbname': 'airflow',
        'user': 'airflow',
        'password': 'airflow',
        'host': 'postgres',
        'port': '5432'
    }
    
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print("Berhasil terhubung ke PostgreSQL.")
        
        # SQL DDL
        create_table_sql = """
            DROP TABLE IF EXISTS table_m3;
            CREATE TABLE table_m3 (
                Date VARCHAR(255),
                Time VARCHAR(255),
                "Booking ID" VARCHAR(100),
                "Booking Status" VARCHAR(100),
                "Customer ID" VARCHAR(100),
                "Vehicle Type" VARCHAR(50),
                "Pickup Location" VARCHAR(100),
                "Drop Location" VARCHAR(100),
                "Avg VTAT" VARCHAR(50),
                "Avg CTAT" VARCHAR(50),
                "Cancelled Rides by Customer" VARCHAR(50),
                "Reason for cancelling by Customer" VARCHAR(200),
                "Cancelled Rides by Driver" VARCHAR(50),
                "Driver Cancellation Reason" VARCHAR(200),
                "Incomplete Rides" VARCHAR(50),
                "Incomplete Rides Reason" VARCHAR(200),
                "Booking Value" VARCHAR(50),
                "Ride Distance" VARCHAR(50),
                "Driver Ratings" VARCHAR(50),
                "Customer Rating" VARCHAR(50),
                "Payment Method" VARCHAR(50)
            );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print("Tabel 'table_m3' berhasil dibuat.")

    except (Exception, psycopg2.Error) as error:
        print(f"Error saat membuat tabel: {error}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Koneksi PostgreSQL ditutup.")


def insert_data_to_postgres():
    """
    Fungsi ini membaca data dari file CSV (dengan header) dan memasukkannya ke
    dalam tabel 'table_m3' di PostgreSQL.
    """
    db_params = {
        'dbname': 'airflow',
        'user': 'airflow',
        'password': 'airflow',
        'host': 'postgres',
        'port': '5432'
    }

    csv_file_path = '/opt/airflow/dags/uber_data_raw.csv'
    
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        print("Berhasil terhubung ke PostgreSQL.")
        
        # check table
        cursor.execute("SELECT COUNT(*) FROM table_m3;")
        if cursor.fetchone()[0] > 0:
            print("Data sudah ada di tabel 'table_m3'. Proses INSERT dilewati.")
            return

        with open(csv_file_path, 'r') as f:
            reader = csv.reader(f)
            next(reader)  
            
            for row in reader:
                # change null dengan None 
                processed_row = [None if item == 'null' else item for item in row]
                #insert data 
                cursor.execute("""
                    INSERT INTO table_m3 (
                        Date, Time, "Booking ID", "Booking Status", "Customer ID", "Vehicle Type", "Pickup Location", "Drop Location", 
                        "Avg VTAT", "Avg CTAT", "Cancelled Rides by Customer", 
                        "Reason for cancelling by Customer", "Cancelled Rides by Driver", "Driver Cancellation Reason", "Incomplete Rides", 
                        "Incomplete Rides Reason", "Booking Value", "Ride Distance", "Driver Ratings", "Customer Rating", "Payment Method"
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, processed_row)
        
        conn.commit()
        print("Data berhasil dimasukkan ke tabel 'table_m3'.")

    except (Exception, psycopg2.Error) as error:
        print(f"Error saat memasukkan data: {error}")
        raise
    
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Koneksi PostgreSQL ditutup.")


def fetch_data_from_postgres(**kwargs):
    '''
    Function ini untuk mengambil data dari postgres
    '''
    db_params = {
        'dbname': 'airflow',
        'user': 'airflow',
        'password': 'airflow',
        'host': 'postgres',
        'port': '5432'
    }
    
    conn = None
    try:
        conn = psycopg2.connect(**db_params)
        query = "SELECT * FROM table_m3;"
        df = pd.read_sql(query, conn)
        return df.to_json()
        
    except (Exception, psycopg2.Error) as error:
        print(f"Error saat mengambil data dari PostgreSQL: {error}")
        raise
        
    finally:
        if conn:
            conn.close()
            print("Koneksi PostgreSQL ditutup.")

def clean_data(**kwargs):
    '''
    Fungsi ini untuk melakukan cleaning data
    '''
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='fetch_from_postgres')
    df = pd.read_json(df_json)
    
    # Menghapus duplikat
    df.drop_duplicates(inplace=True)
    print("Jumlah data setelah menghapus duplikat:", len(df))

    # Normalisasi nama kolom
    new_columns = []
    for column in df.columns:
        new_column = column.lower().replace(" ", "_").replace('"', '').replace("'", "")
        new_columns.append(new_column)
    
    df.columns = new_columns
    print("Nama kolom setelah normalisasi:", df.columns.tolist())

    #convert kolom date and time
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['time'] = pd.to_datetime(df['time'], errors='coerce')
    
    # handling missing value "null" dengan NaN
    df.replace('null', pd.NA, inplace=True)
    
    # handling missing value pada kolom numerik dengan rata-rata
    numeric_cols = ['avg_vtat', 'avg_ctat', 'cancelled_rides_by_customer', 
                    'cancelled_rides_by_driver', 'incomplete_rides', 
                    'booking_value', 'ride_distance', 'driver_ratings', 
                    'customer_rating']
    for col in numeric_cols:
        # convert kolom numerik
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col].fillna(df[col].mean(), inplace=True)
    
    # handling missing value pada kolom kategorikal dengan 'Unknown'
    categorical_cols = df.select_dtypes(include=['object']).columns
    for col in categorical_cols:
        df[col] = df[col].fillna('Unknown')
    
    print("Status missing value setelah handling:")
    print(df.isnull().sum())
    
    clean_csv_path = '/opt/airflow/dags/uber_data_clean.csv'
    df.to_csv(clean_csv_path, index=False)
    print(f"Data bersih berhasil disimpan ke: {clean_csv_path}")

def post_to_elasticsearch():
    '''
    Function ini untuk load data ke elasticsearch
    '''
    es = Elasticsearch(hosts=[{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}])
    
    if not es.ping():
        raise ValueError("Koneksi ke Elasticsearch gagal!")
    print("Berhasil terhubung ke Elasticsearch.")
    
    index_name = "uber_ride_analytics_clean"
    
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Index lama '{index_name}' berhasil dihapus.")

    clean_csv_path = '/opt/airflow/dags/uber_data_clean.csv'
    df_clean = pd.read_csv(clean_csv_path)
    
    for i, row in df_clean.iterrows():
        doc = row.to_dict()
        es.index(index=index_name, body=doc)
    
    print(f"Semua data berhasil diunggah ke Elasticsearch index: '{index_name}'")

#setting DAG
with DAG(
    dag_id="Uber_DAG",
    start_date=pendulum.datetime(2024, 11, 1, tz="Asia/Jakarta"),
    schedule_interval="10,20,30 9 * * 6",
    catchup=False,
    tags=["milestone_3"],
) as dag:
    
    # all task
    create_table_task = PythonOperator(
        task_id="create_table_in_postgres",
        python_callable=create_table_in_postgres,
    )

    insert_task = PythonOperator(
        task_id="insert_data_to_postgres",
        python_callable=insert_data_to_postgres,
    )

    fetch_task = PythonOperator(
        task_id="fetch_from_postgres",
        python_callable=fetch_data_from_postgres,
    )

    clean_task = PythonOperator(
        task_id="data_cleaning",
        python_callable=clean_data,
    )

    post_es_task = PythonOperator(
        task_id="post_to_elasticsearch",
        python_callable=post_to_elasticsearch,
    )

    #pipeline
    create_table_task >> insert_task >> fetch_task >> clean_task >> post_es_task