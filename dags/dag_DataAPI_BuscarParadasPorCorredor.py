import requests
import pandas as pd
import boto3
import logging
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.hooks.base import BaseHook
from datetime import datetime

logger = logging.getLogger("airflow")

# Defina sua chave de API da SPTrans
api_key = "9aa2fcbfb81e92aaf26c640c539848fa69193acd16d9784ec862d1d42b29d28c"

# Função para autenticação e busca das linhas de ônibus
def GetData_API_BuscarParadasPorCorredor(**kwargs):
    auth_url = "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar"
    params = {"token": api_key}
    
    # Autenticação
    auth_response = requests.post(auth_url, params=params)
    if auth_response.status_code == 200 and auth_response.text == 'true':
        print("Autenticação bem-sucedida!")
    else:
        print("Falha na autenticação:", auth_response.text)
        return
    
    search_url = "http://api.olhovivo.sptrans.com.br/v2.1/Parada/BuscarParadasPorCorredor"
    prefixos = [str(i) for i in range(10)]
    todas_as_paradas = []
    
    for prefixo in prefixos:
        response = requests.get(search_url, params={"codigoCorredor": prefixo}, cookies=auth_response.cookies)
        if response.status_code == 200:
            paradas = response.json()
            todas_as_paradas.extend(paradas)
            print(f"Paradas encontradas para o corredor {prefixo}: {len(paradas)}")
        else:
            print(f"Erro ao buscar paradas com prefixo {prefixo}: {response.text}")
    
    todas_as_paradas = [dict(t) for t in {tuple(d.items()) for d in todas_as_paradas}]
    df = pd.DataFrame(todas_as_paradas)
    
    # Armazenar o DataFrame no XCom
    kwargs['ti'].xcom_push(key='data_paradas_corredor', value=df.to_dict())
    print("Dados armazenados no XCom com sucesso!")

# Função para salvar os dados no MinIO
def save_paradasPorcorredor_to_minio(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data_paradas_corredor', task_ids='GetData_API_BuscarParadasPorCorredor')
    
    if data is None:
        logger.error("Nenhum dado encontrado no XCom.")
        return
    
    df = pd.DataFrame(data)
    local_file = "/tmp/DataAPI_BuscarParadasPorCorredor.csv"
    logger.info(f"Salvando o DataFrame no arquivo {local_file}.")
    df.to_csv(local_file, index=False)
    
    # Verificar se o arquivo foi criado com sucesso
    if os.path.exists(local_file):
        logger.info(f"O arquivo {local_file} foi salvo com sucesso.")
    
    # Configurações do MinIO (ajuste conforme necessário)
    minio_endpoint = "http://host.docker.internal:9050"
    minio_access_key = "datalake"
    minio_secret_key = "datalake"
    bucket_name = "raw"
    object_name = "DataAPI_BuscarParadasPorCorredor.csv"
    
    
    # Conectar e salvar no MinIO
    try:
        s3_client = boto3.client('s3',
                                 endpoint_url=minio_endpoint,
                                 aws_access_key_id=minio_access_key,
                                 aws_secret_access_key=minio_secret_key)
        
        # Verificar e criar bucket se necessário
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' encontrado.")
        except:
            print(f"Bucket '{bucket_name}' não encontrado. Criando bucket.")
            s3_client.create_bucket(Bucket=bucket_name)
        
        
        s3_client.upload_file(local_file, bucket_name, object_name)
        print(f"Arquivo {local_file} salvo no bucket {bucket_name} como {object_name}.")
    except Exception as e:
        logger.error(f"Erro ao enviar o arquivo para o MinIO: {str(e)}")

# Definir o DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 20),
    'retries': 1
}

# Definir a DAG única
with DAG('DataAPI_BuscarParadasPorCorredor_SaveMinIO',
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:
    
    # Task 1: Buscar dados da API
    fetch_task = PythonOperator(
        task_id='GetData_API_BuscarParadasPorCorredor',
        python_callable=GetData_API_BuscarParadasPorCorredor,
        provide_context=True
    )
    
    # Task 2: Salvar os dados no MinIO
    save_task = PythonOperator(
        task_id='save_paradasPorcorredor_to_minio',
        python_callable=save_paradasPorcorredor_to_minio,
        provide_context=True
    )

    # Definir dependência: fetch_task -> save_task
    fetch_task >> save_task
