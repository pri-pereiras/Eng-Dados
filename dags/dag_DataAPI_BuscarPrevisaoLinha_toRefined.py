import os
import boto3
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
bucket_trusted = "trusted"
bucket_refined = "refined"

# Função para processar arquivos da trusted para refined
def save_BuscarPrevisaoLinha_toRefined(**kwargs):
    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)

    # Listar arquivos no bucket trusted
    arquivos = s3_client.list_objects_v2(Bucket=bucket_trusted, Prefix="previsao-linha/")

    for obj in arquivos.get('Contents', []):
        arquivo_trusted = obj['Key']
        ano, mes, dia, nome_arquivo = arquivo_trusted.split('/')[-4:]

        # Definir o caminho para o arquivo na camada refined
        path_refined = f"previsao-linha/ano={ano}/mes={mes}/dia={dia}/{nome_arquivo}"

        # Verificar se o arquivo já existe na refined
        try:
            s3_client.head_object(Bucket=bucket_refined, Key=path_refined)
            print(f"Arquivo {path_refined} já existe na camada refined. Pulando.")
            continue
        except s3_client.exceptions.ClientError:
            print(f"Processando arquivo {arquivo_trusted} para a camada refined.")

        # Baixar o arquivo Parquet da camada trusted
        response = s3_client.get_object(Bucket=bucket_trusted, Key=arquivo_trusted)
        with open(f"/tmp/{nome_arquivo}", "wb") as file:
            file.write(response['Body'].read())

        # Ler o arquivo Parquet em um DataFrame
        df = pd.read_parquet(f"/tmp/{nome_arquivo}")

        # Realizar qualquer transformação necessária no DataFrame (opcional)
        # Aqui você pode modificar o DataFrame `df` se precisar de alguma transformação extra.

        # Salvar o DataFrame em Parquet no caminho local
        try:
            refined_dir = f"/tmp/refined/{ano}/{mes}/{dia}"
            if not os.path.exists(refined_dir):
                os.makedirs(refined_dir)
                
            arquivo_parquet = f"{refined_dir}/{nome_arquivo}"
            df.to_parquet(arquivo_parquet)
            print(f"Arquivo Parquet {arquivo_parquet} criado com sucesso para a camada refined.")

            # Upload para a camada refined
            with open(arquivo_parquet, "rb") as data:
                s3_client.upload_fileobj(data, bucket_refined, path_refined)
            print(f"Arquivo {path_refined} salvo na camada refined.")
        except Exception as e:
            print(f"Erro ao salvar {arquivo_trusted} na camada refined: {e}")

# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG('DataAPI_BuscarPrevisaoLinha_toRefined', default_args=default_args, 
         schedule_interval=None,
         catchup=False) as dag:
    
    move_data_task = PythonOperator(
        task_id='save_BuscarPrevisaoLinha_toRefined',
        python_callable=save_BuscarPrevisaoLinha_toRefined
    )

    # Trigger para a próxima DAG
    trigger_postgres = TriggerDagRunOperator(
        task_id='trigger_postgres',
        trigger_dag_id='DataAPI_BuscarPrevisaoLinha_toPostgres',
        wait_for_completion=True,
    )

    move_data_task >> trigger_postgres