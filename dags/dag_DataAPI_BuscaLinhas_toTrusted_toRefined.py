import boto3
import logging
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
from airflow.operators.dagrun_operator import TriggerDagRunOperator



logger = logging.getLogger("airflow")

# Configuração MinIO e buckets
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_origem = 'raw'
bucket_trusted = 'trusted'
bucket_refined = 'refined'

def salva_trusted():
    # Inicializa a sessão do Spark dentro da função
    spark = SparkSession.builder.getOrCreate()

    # Configura o cliente S3 (Minio)
    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)

    # Faz o download do arquivo CSV do bucket S3 (Minio)
    s3_client.download_file(bucket_origem, "DataAPI_BuscaLinhas.csv", '/tmp/DataAPI_BuscaLinhas.csv')

    # Carrega o CSV no Spark
    data = spark.read.option("header", True).csv('/tmp/DataAPI_BuscaLinhas.csv')

    # Registra o DataFrame como uma tabela temporária
    data.createOrReplaceTempView("data_table")

    # Executa a consulta SQL no DataFrame
    data = spark.sql('SELECT distinct cl as codigo_linha, lc as circular, lt as prim_letreiro, tl as seg_letreiro, sl as sentido FROM data_table')

    # Salva o DataFrame no formato Parquet no bucket "trusted"
    data.write.mode("overwrite").parquet('/tmp/LinhasTrusted')

    # Faz o upload dos arquivos Parquet para o bucket "trusted" no Minio
    i = 0
    for each in os.listdir('/tmp/LinhasTrusted'):
        if each.endswith('.parquet'):  # Verifica se o arquivo é Parquet
            s3_client.upload_file('/tmp/LinhasTrusted/' + each, bucket_trusted, 'Linhas/LinhasTrusted{}.parquet'.format(i))
            i += 1

def salva_refined():
    # Inicializa a sessão do Spark dentro da função
    spark = SparkSession.builder.getOrCreate()

    # Configura o cliente S3 (Minio)
    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)

    caminho_trusted = '/tmp/LinhasTrusted'

    # Carrega os arquivos Parquet salvos no trusted
    data = spark.read.parquet(caminho_trusted)

    # Fazendo transformações adicionais 
    data_refined = data.withColumn('letreiro', concat_ws(",", "prim_letreiro", "seg_letreiro"))

    data_refined = data_refined.drop('prim_letreiro').drop('seg_letreiro')

    data_refined.write.mode("overwrite").parquet('/tmp/LinhasRefined')

    # Faz o upload dos arquivos refinados para o bucket "refined" no Minio
    i = 0
    for each in os.listdir('/tmp/LinhasRefined'):
        if each.endswith('.parquet'):  # Verifica se o arquivo é Parquet
            s3_client.upload_file('/tmp/LinhasRefined/' + each, bucket_refined, 'Linhas/LinhasRefined{}.parquet'.format(i))
            i += 1


# Definições padrão para o DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

# Criação do DAG no Airflow
with DAG('DataAPI_BuscaLinhas_toTrusted_toRefined',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    
    save_trusted_task = PythonOperator(
        task_id='salva_linhas_trusted',
        python_callable=salva_trusted
    )

    save_refined_task = PythonOperator(
        task_id='salva_linhas_refined',
        python_callable=salva_refined
    )

    # Trigger para a próxima DAG
    trigger_postgres = TriggerDagRunOperator(
        task_id='trigger_postgres',
        trigger_dag_id='DataAPI_BuscaLinhas_toPostgres',
        wait_for_completion=True,
    )

    # Definindo a sequência de execução
    save_trusted_task >> save_refined_task >> trigger_postgres
