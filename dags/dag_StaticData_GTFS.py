import requests
import boto3
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Configurações do MinIO
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_name = "raw"

# URL base dos arquivos brutos no GitHub
raw_base_url = "https://github.com/pri-pereiras/Eng-Dados/tree/main/airflow/data/GTFS%20SPTrans"

# Lista de arquivos no repositório GitHub que deseja baixar
file_list = [
    "shapes.txt", 
    "agency.txt", 
    "calendar.txt", 
    "fare_attributes.txt", 
    "fare_rules.txt", 
    "frequencies.txt", 
    "routes.txt", 
    "stop_times.txt", 
    "stops.txt", 
    "trips.txt"

    # Inclua todos os nomes dos arquivos que estão no diretório.
]

# Função para baixar e subir cada arquivo
def StaticData_GTFS_toMinio(**kwargs):
    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)
    
    for filename in file_list:
        
        if filename == "shapes.txt" :
            raw_base_url = "https://github.com/pri-pereiras/Eng-Dados/tree/main/airflow/data"
        
        # Monta a URL para download do arquivo
        file_url = f"{raw_base_url}/{filename}"
        response = requests.get(file_url)
        
        if response.status_code == 200:
            # Salva o arquivo localmente
            local_file_path = f"/tmp/{filename}"
            with open(local_file_path, "wb") as file:
                file.write(response.content)
            
            # Define o caminho no bucket MinIO
            bucket_path = f"GTFS-SPTrans/{filename}"
            s3_client.upload_file(local_file_path, bucket_name, bucket_path)
            print(f"Arquivo {filename} enviado para o MinIO em {bucket_path}.")
            
            # Remove o arquivo local após upload
            os.remove(local_file_path)
        else:
            print(f"Erro ao baixar {filename}: {response.status_code}")

# Configurações do DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Definição do DAG
with DAG(
    'StaticData_GTFS_toMinio',
    default_args=default_args,
    description='DAG para baixar arquivos GTFS do GitHub e subir no MinIO',
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_upload_files = PythonOperator(
        task_id='StaticData_GTFS_toMinio',
        python_callable=StaticData_GTFS_toMinio,
        provide_context=True,
    )

    task_upload_files