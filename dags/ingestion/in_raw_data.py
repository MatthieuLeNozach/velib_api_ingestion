from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
from include.custom_operators.minio import MinIOHook
import io

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'velib_raw_data_ingestion',
    default_args=default_args,
    description='A DAG to ingest raw Velib API data',
    schedule_interval="*/2 * * * *",
)

def create_folder_structure(**kwargs):
    minio_hook = MinIOHook()
    bucket_name = 'raw_data'
    minio_hook.create_bucket(bucket_name)
    
    now = datetime.now()
    year = now.year
    week = now.isocalendar()[1]
    day = now.strftime('%Y-%m-%d')
    
    folder_path = f"{year}/week_{week}/{day}"
    minio_hook.create_folder(bucket_name, folder_path)
    
    kwargs['ti'].xcom_push(key='folder_path', value=folder_path)
    kwargs['ti'].xcom_push(key='bucket_name', value=bucket_name)
    print(f"Folder structure created: {bucket_name}/{folder_path}")

def fetch_velib_data(**kwargs):
    api_url = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        raw_data = response.json()
        kwargs['ti'].xcom_push(key='raw_data', value=raw_data)
        print("Data fetched successfully from Velib API")
    else:
        print(f"Failed to fetch data from Velib API. Status code: {response.status_code}")
        raise Exception("API request failed")

def write_to_minio(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='fetch_velib_data')
    folder_path = ti.xcom_pull(key='folder_path', task_ids='create_folder_structure')
    bucket_name = ti.xcom_pull(key='bucket_name', task_ids='create_folder_structure')
    
    minio_hook = MinIOHook()
    
    timestamp = datetime.now().strftime('%H%M%S')
    object_name = f"{folder_path}/api_call_{timestamp}.json"
    
    json_bytes = json.dumps(raw_data).encode('utf-8')
    
    minio_hook.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=io.BytesIO(json_bytes),
        length=len(json_bytes),
        part_size=10*1024*1024  # 10MB part size
    )
    
    print(f"Raw data stored in MinIO: {bucket_name}/{object_name}")

create_folder_task = PythonOperator(
    task_id='create_folder_structure',
    python_callable=create_folder_structure,
    provide_context=True,
    dag=dag,
)

fetch_data_task = PythonOperator(
    task_id='fetch_velib_data',
    python_callable=fetch_velib_data,
    provide_context=True,
    dag=dag,
)

write_to_minio_task = PythonOperator(
    task_id='write_to_minio',
    python_callable=write_to_minio,
    provide_context=True,
    dag=dag,
)

create_folder_task >> fetch_data_task >> write_to_minio_task