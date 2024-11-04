import requests
import pandas as pd
import json
import boto3
import pytz
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
import time

# Configurações do MinIO
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_raw = "raw"
api_key = "9aa2fcbfb81e92aaf26c640c539848fa69193acd16d9784ec862d1d42b29d28c"
linha_csv_path = "/tmp/DataAPI_BuscaLinhas.csv"

# Função para baixar o CSV de linhas do MinIO
def baixar_csv_linhas_minio():
    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)
    s3_client.download_file(bucket_raw, "DataAPI_BuscaLinhas.csv", linha_csv_path)
    print("Arquivo CSV de linhas baixado com sucesso.")

# Função de autenticação
def autenticar(api_key):
    auth_url = "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar"
    params = {"token": api_key}
    auth_response = requests.post(auth_url, params=params)
    
    if auth_response.status_code == 200 and auth_response.text == 'true':
        print("Autenticação bem-sucedida!")
        return auth_response.cookies
    else:
        raise Exception("Falha na autenticação")

# Função principal para obter previsões de chegada para cada linha com re-autenticação em caso de falha
def GetData_API_PrevisaoLinha(**kwargs):
    # Tenta autenticar inicialmente
    cookies = autenticar(api_key)
    
    # Carregar a relação de linhas a partir do CSV baixado
    print("Carregando a base de linhas baixada do Minio")
    df_linhas = pd.read_csv(linha_csv_path)
    
    # Lista para consolidar os dados
    consolidated_data = []

    # Iterar sobre cada linha e buscar previsões
    for _, linha in df_linhas.iterrows():
        codigo_linha = linha["cl"]
        success = False
        attempts = 0

        # Tenta fazer a requisição até 3 vezes (re-autenticando se necessário)
        while not success and attempts < 3:
            try:
                # Chamar a API para previsão de chegada de cada linha
                url = f"http://api.olhovivo.sptrans.com.br/v2.1/Previsao/Linha?codigoLinha={codigo_linha}"
                response = requests.get(url, cookies=cookies)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Verificar se há dados válidos e adicionar código da linha
                    if data.get("ps"):
                        for parada in data["ps"]:
                            parada["codigo_linha"] = codigo_linha
                        consolidated_data.append({
                            "codigo_linha": codigo_linha,
                            "previsoes": data["ps"]
                        })
                        print(f"Dados válidos adicionados para a linha {codigo_linha}")
                    else:
                        print(f"Nenhum dado válido para a linha {codigo_linha}")
                    success = True  # Sai do loop se a chamada foi bem-sucedida
                else:
                    print(f"Erro ao buscar previsões para a linha {codigo_linha}: {response.text}")
                    attempts += 1
                    if attempts < 3:
                        cookies = autenticar(api_key)  # Re-autentica antes da próxima tentativa
            except requests.ConnectionError as e:
                print(f"Erro de conexão ao buscar dados para a linha {codigo_linha}: {e}")
                attempts += 1
                if attempts < 3:
                    cookies = autenticar(api_key)  # Re-autentica antes da próxima tentativa
                time.sleep(5)  # Aguarda 5 segundos antes da nova tentativa

    # Salvar dados consolidados no MinIO
    if consolidated_data:
        timezone = pytz.timezone("America/Sao_Paulo")
        now = datetime.now(timezone)
        ano, mes, dia, hora = now.strftime('%Y'), now.strftime('%m'), now.strftime('%d'), now.strftime('%H%M')
        file_name = f"previsao_linha_consolidado_{now.strftime('%Y%m%d_%H%M')}.json"
        path = f"previsao-linha/ano={ano}/mes={mes}/dia={dia}/{file_name}"
        
        s3_client = boto3.client('s3',
                                 endpoint_url=minio_endpoint,
                                 aws_access_key_id=minio_access_key,
                                 aws_secret_access_key=minio_secret_key)
        
        s3_client.put_object(Bucket=bucket_raw, Key=path, Body=json.dumps(consolidated_data))
        print(f"Arquivo consolidado salvo no MinIO em {path}.")
    else:
        print("Nenhum dado consolidado para salvar.")

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG('DataAPI_BuscarPrevisaoLinha_toRaw', default_args=default_args, 
         schedule_interval='*/2 * * * *',  # Executa a cada 15 minutos
         catchup=False) as dag:
    
    task_baixar_csv_linhas = PythonOperator(
        task_id='baixar_csv_linhas_minio',
        python_callable=baixar_csv_linhas_minio,
        provide_context=True,
    )

    task_GetData_API_PrevisaoLinha = PythonOperator(
        task_id='GetData_API_PrevisaoLinha',
        python_callable=GetData_API_PrevisaoLinha,
        provide_context=True,
    )

    # Trigger para a próxima DAG
    trigger_trusted = TriggerDagRunOperator(
        task_id='trigger_trusted',
        trigger_dag_id='DataAPI_BuscarPrevisaoLinha_toTrusted',
        wait_for_completion=True,
    )

    task_baixar_csv_linhas >> task_GetData_API_PrevisaoLinha >> trigger_trusted
