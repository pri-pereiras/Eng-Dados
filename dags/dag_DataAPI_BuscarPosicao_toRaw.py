import requests
import json
import pandas as pd
import boto3
import pytz
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago

# Configurações do MinIO
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_raw = "raw"

# Função para buscar a posição dos veículos e armazenar na camada raw
def GetData_API_BuscarPosicao(**kwargs):
    # URL e parâmetros da API (ajuste conforme necessário)
    url = "http://api.olhovivo.sptrans.com.br/v2.1/Posicao"
    api_key = "9aa2fcbfb81e92aaf26c640c539848fa69193acd16d9784ec862d1d42b29d28c"
    
    # Autenticação e coleta de dados
    auth_response = requests.post("http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar", params={"token": api_key})
    if auth_response.status_code == 200 and auth_response.text == 'true':
        print("Autenticação bem-sucedida!")
        response = requests.get(url, cookies=auth_response.cookies)
        if response.status_code == 200:
            data = response.json()
            
            # Configuração do fuso horário para GMT-3
            timezone = pytz.timezone("America/Sao_Paulo")
            now = datetime.now(timezone)
            
            # Estrutura do nome e caminho do arquivo
            ano = now.strftime('%Y')
            mes = now.strftime('%m')
            dia = now.strftime('%d')
            hora = now.strftime('%H%M')
            file_name = f"veiculos_posicao_{now.strftime('%Y%m%d_%H%M')}.json"
            path = f"veiculos-posicao/ano={ano}/mes={mes}/dia={dia}/hora={now.strftime('%H')}/{file_name}"

            # Salvar JSON no MinIO
            s3_client = boto3.client('s3',
                                     endpoint_url=minio_endpoint,
                                     aws_access_key_id=minio_access_key,
                                     aws_secret_access_key=minio_secret_key)
            s3_client.put_object(Bucket=bucket_raw, Key=path, Body=json.dumps(data))
            print(f"Dados de posição salvos no MinIO em {path}.")
        else:
            print(f"Erro ao buscar posição dos veículos: {response.status_code}")
    else:
        print("Falha na autenticação com a API")


# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG('DataAPI_BuscarPosicao_toRaw', default_args=default_args, 
         schedule_interval='*/2 * * * *',  # Executa a cada 15 minutos
         catchup=False) as dag:
    
    fetch_task = PythonOperator(
        task_id='GetData_API_BuscarPosicao',
        python_callable=GetData_API_BuscarPosicao
    )

    # Trigger para a próxima DAG
    trigger_trusted = TriggerDagRunOperator(
        task_id='trigger_trusted',
        trigger_dag_id='DataAPI_BuscarPosicao_toTrusted',
        wait_for_completion=True,
    )

    fetch_task >> trigger_trusted