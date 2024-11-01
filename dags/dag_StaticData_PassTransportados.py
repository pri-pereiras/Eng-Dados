import requests
import boto3
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
raw_base_url = "https://github.com/pri-pereiras/Eng-Dados/tree/main/airflow/data/Passageiros-Transportados"

# Mapeamento de meses com o número correspondente para os nomes de arquivos
meses_portugues = {
    "01": "Janeiro", "02": "Fevereiro", "03": "Março", "04": "Abril",
    "05": "Maio", "06": "Junho", "07": "Julho", "08": "Agosto",
    "09": "Setembro", "10": "Outubro", "11": "Novembro", "12": "Dezembro"
}

# Função para baixar arquivos XLS
def download_all_xls_files(**kwargs):
    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)

    # Gerar a lista de arquivos com nome e caminho corretos
    filenames = [
        f"Consolidado {num_mes}-{mes}-{ano}.xls"
        for ano in range(2020, 2025)
        for num_mes, mes in meses_portugues.items()
    ]

    for filename in filenames:
        file_url = f"{raw_base_url}/{filename.replace(' ', '%20')}"  # Codificar espaços no URL
        response = requests.get(file_url)
        
        if response.status_code == 200:
            local_file_path = f"/tmp/{filename}"
            with open(local_file_path, "wb") as file:
                file.write(response.content)
            
            # Extraindo ano e mês a partir do nome do arquivo
            ano = filename.split("-")[-1].replace(".xls", "")
            num_mes = filename.split("-")[0].split(" ")[1]

            # Definir o caminho no MinIO
            caminho_bucket = f"transporte-passageiros/ano={ano}/mes={num_mes}/{filename}"
            s3_client.upload_file(local_file_path, bucket_name, caminho_bucket)
            print(f"Arquivo {filename} salvo no MinIO em {caminho_bucket}.")
        else:
            print(f"Erro ao baixar {filename}: {response.status_code}")

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

# Definição da DAG
with DAG(
    'StaticsData_passageirosTransportados_toMinio',
    default_args=default_args,
    description='DAG para ingerir dados de transporte de passageiros no MinIO',
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_download_files = PythonOperator(
        task_id='download_all_xls_files',
        python_callable=download_all_xls_files,
        provide_context=True,
    )

    task_download_files