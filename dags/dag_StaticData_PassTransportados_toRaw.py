import boto3
import logging
import os
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime
from airflow.utils.dates import days_ago

# Configurações do MinIO
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_name = "raw"

# URL base dos arquivos brutos no GitHub
raw_base_url = "https://raw.githubusercontent.com/pri-pereiras/Eng-Dados/main/airflow/data/Passageiros-Transportados"

# Mapeamento de meses com o número correspondente para os nomes de arquivos
meses_portugues = {
    "01": "Janeiro", "02": "Fevereiro", "03": "Março", "04": "Abril",
    "05": "Maio", "06": "Junho", "07": "Julho", "08": "Agosto",
    "09": "Setembro", "10": "Outubro", "11": "Novembro", "12": "Dezembro"
}

def download_all_xls_files(**kwargs):
    # Configurar cliente MinIO (S3)
    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)
    
    # Gerar a lista de arquivos com nome e caminho corretos
    filenames = [
        f"Consolidado {num_mes}-{mes}-{ano}.xls"
        for ano in range(2024, 2025)
        for num_mes, mes in meses_portugues.items()
    ]

    for filename in filenames:
        file_url = f"{raw_base_url}/{filename.replace(' ', '%20')}"
        response = requests.get(file_url)
        
        if response.status_code == 200:
            excel_path = f"/tmp/{filename}"
            csv_path = f"/tmp/{filename.replace('.xls', '.csv')}"

            # Salvar o arquivo Excel localmente
            with open(excel_path, "wb") as file:
                file.write(response.content)

            # Ler o Excel em um DataFrame do pandas
            df_pandas = pd.read_excel(excel_path, skiprows=2, engine="openpyxl")
            df_pandas.to_csv(csv_path, index=False) 

            # Extraindo ano e mês a partir do nome do arquivo
            ano = filename.split("-")[-1].replace(".xls", "")
            num_mes = filename.split("-")[0].split(" ")[1]

            # Definir o caminho no MinIO
            caminho_bucket = f"transporte-passageiros/ano={ano}/mes={num_mes}/{filename.replace('.xls', '.csv')}"
            s3_client.upload_file(csv_path, bucket_name, caminho_bucket)
            logging.info(f"Arquivo {filename} salvo no MinIO em {caminho_bucket}.")

            # Verificar se o arquivo existe antes de tentar remover
            if os.path.exists(excel_path):
                os.remove(excel_path)
            if os.path.exists(csv_path):
                os.remove(csv_path)
        else:
            logging.error(f"Erro ao baixar {filename}: {response.status_code}")

# Configurações da DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG('StaticData_PassTransportados_toRaw', default_args=default_args, 
         schedule_interval='@monthly',  # Executa mensalmente
         catchup=False) as dag:
    
    task_download_files = PythonOperator(
        task_id='download_xls_files',
        python_callable=download_all_xls_files,
        provide_context=True,
    )

    # Trigger para a próxima DAG
    trigger_trusted = TriggerDagRunOperator(
        task_id='trigger_trusted',
        trigger_dag_id='StaticData_PassTransportados_toTrusted',
        wait_for_completion=True,
    )

    task_download_files >> trigger_trusted