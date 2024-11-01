import boto3
import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

logger = logging.getLogger("airflow")

def save_to_minio(**kwargs):
    ti = kwargs['ti']
    # Recuperar o run_id atual (se precisar garantir que está no mesmo contexto)
    run_id = kwargs['run_id']
    
    # Recuperar dados do XCom com o task_id correto e contexto correto
    data = ti.xcom_pull(key='data_linhas_onibus', task_ids='fetch_olho_vivo_data', dag_id='DataAPI_BuscaLinhas', run_id=run_id)
    
    if data is None:
        logger.error("Nenhum dado encontrado no XCom.")
        return
    
    # Processar o DataFrame
    df = pd.DataFrame(data)
    logger.info("DataFrame carregado com sucesso.")

    local_file = "/tmp/DataAPI_BuscaLinhas.csv"
    logger.info(f"Salvando o DataFrame no arquivo {local_file}.")
    df.to_csv(local_file, index=False)
    
    minio_endpoint = "http://localhost:9051/"
    minio_access_key = "datalake"
    minio_secret_key = "datalake"
    bucket_name = "raw"
    object_name = "DataAPI_BuscaLinhas.csv"
    
    try:
        logger.info("Conectando ao MinIO e tentando fazer o upload do arquivo.")
        s3_client = boto3.client('s3',
                                 endpoint_url=minio_endpoint,
                                 aws_access_key_id=minio_access_key,
                                 aws_secret_access_key=minio_secret_key)
        
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except:
            logger.warning(f"Bucket {bucket_name} não encontrado, criando bucket.")
            s3_client.create_bucket(Bucket=bucket_name)
        
        s3_client.upload_file(local_file, bucket_name, object_name)
        logger.info(f"Arquivo {local_file} salvo no bucket {bucket_name} como {object_name}.")
    except Exception as e:
        logger.error(f"Erro ao enviar o arquivo para o MinIO: {str(e)}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 20),
    'retries': 1
}

with DAG('save_DataAPI_BuscaLinhas_to_minio',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    
    save_task = PythonOperator(
        task_id='save_DataAPI_BuscaLinhas_to_minio',
        python_callable=save_to_minio,
        provide_context=True
    )
