import requests
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Configurações do MinIO
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_name = "raw"

# URL do arquivo OSM
osm_url = "https://download.geofabrik.de/south-america/brazil/sudeste-latest.osm.pbf"
local_file_path = "/tmp/sudeste-latest.osm.pbf"

# Função para baixar o arquivo
def download_Data_OpenStreetMap():
    response = requests.get(osm_url, stream=True)
    if response.status_code == 200:
        with open(local_file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Arquivo baixado com sucesso: {local_file_path}")
    else:
        print(f"Erro ao baixar o arquivo: {response.status_code}")

# Função para enviar o arquivo para o MinIO
def upload_file_to_minio():
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key
    )
    object_name = "geodata/sudeste-latest.osm.pbf"
    s3_client.upload_file(local_file_path, bucket_name, object_name)
    print(f"Arquivo {local_file_path} enviado para o MinIO como {object_name}")

# Defina a DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'StaticData_OpenStreetMap_toMinio',
    default_args=default_args,
    description='DAG para baixar e enviar arquivo OSM para o MinIO',
    schedule_interval="@daily",
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id='download_Data_OpenStreetMap_osm',
        python_callable=download_Data_OpenStreetMap,
    )

    upload_task = PythonOperator(
        task_id='upload_file_to_minio',
        python_callable=upload_file_to_minio,
    )

    download_task >> upload_task
