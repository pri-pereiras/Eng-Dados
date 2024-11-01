import requests
import boto3
import logging
import os
import pytz
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

logger = logging.getLogger("airflow")

# Defina sua chave de API da SPTrans
api_key = "9aa2fcbfb81e92aaf26c640c539848fa69193acd16d9784ec862d1d42b29d28c"

# Função para autenticação e busca do arquivo KMZ de Velocidade nas Vias
def GetData_API_Velocidade_KMZ(**kwargs):
    auth_url = "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar"
    params = {"token": api_key}
    
    # Autenticação
    auth_response = requests.post(auth_url, params=params)
    if auth_response.status_code == 200 and auth_response.text == 'true':
        print("Autenticação bem-sucedida!")
    else:
        print("Falha na autenticação:", auth_response.text)
        return
    
    # Baixar o arquivo KMZ de Velocidade nas Vias
    kmz_url = "http://api.olhovivo.sptrans.com.br/v2.1/KMZ"
    response = requests.get(kmz_url, cookies=auth_response.cookies)

    if response.status_code == 200:
        local_kmz_file = "/tmp/DataAPI_Velocidade.kmz"
        with open(local_kmz_file, 'wb') as file:
            file.write(response.content)
        print("Arquivo KMZ baixado com sucesso!")
        kwargs['ti'].xcom_push(key='kmz_file_path', value=local_kmz_file)
    else:
        print("Erro ao buscar o arquivo KMZ de Velocidade nas Vias")

# Função para salvar o arquivo KMZ no MinIO
def save_KMZ_to_minio(**kwargs):
    ti = kwargs['ti']
    local_kmz_file = ti.xcom_pull(key='kmz_file_path', task_ids='GetData_API_Velocidade_KMZ')
    
    if local_kmz_file is None:
        logger.error("Nenhum arquivo KMZ encontrado no XCom.")
        return
    
    # Configurações do MinIO
    minio_endpoint = "http://host.docker.internal:9050"
    minio_access_key = "datalake"
    minio_secret_key = "datalake"
    bucket_raw = "raw"
       
    # Configuração do fuso horário para GMT-3
    timezone = pytz.timezone("America/Sao_Paulo")
    now = datetime.now(timezone)
    
    ano = now.strftime('%Y')
    mes = now.strftime('%m')
    dia = now.strftime('%d')
    hora = now.strftime('%H%M')
    file_name = f"velocidade_{now.strftime('%Y%m%d_%H%M')}.kmz"
    path = f"velocidade/ano={ano}/mes={mes}/dia={dia}/{file_name}"
    
    # Conectar e salvar o arquivo KMZ no MinIO
    try:
        s3_client = boto3.client('s3',
                                 endpoint_url=minio_endpoint,
                                 aws_access_key_id=minio_access_key,
                                 aws_secret_access_key=minio_secret_key)
        
        with open(local_kmz_file, 'rb') as f:
            s3_client.upload_fileobj(f, bucket_raw, path)
        print(f"Arquivo KMZ salvo no MinIO em {path}.")
    except Exception as e:
        logger.error(f"Erro ao enviar o arquivo para o MinIO: {str(e)}")

# Definir o DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 20),
    'retries': 0
}

with DAG('DataAPI_Velocidade_KMZ_SaveMinIO',
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         catchup=False) as dag:
    
    # Task 1: Buscar arquivo KMZ da API
    fetch_kmz_task = PythonOperator(
        task_id='GetData_API_Velocidade_KMZ',
        python_callable=GetData_API_Velocidade_KMZ,
        provide_context=True
    )
    
    # Task 2: Salvar o arquivo KMZ no MinIO
    save_kmz_task = PythonOperator(
        task_id='save_KMZ_to_minio',
        python_callable=save_KMZ_to_minio,
        provide_context=True
    )

    # Definir dependência: fetch_kmz_task -> save_kmz_task
    fetch_kmz_task >> save_kmz_task
