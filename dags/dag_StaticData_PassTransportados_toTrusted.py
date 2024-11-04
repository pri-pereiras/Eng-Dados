import os
import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format

# Configurações do MinIO
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_raw = "raw"
bucket_trusted = "trusted"

# Função para verificar e processar arquivos
def save_PassTransportados_toTrusted(**kwargs):
    # Inicializa a sessão do Spark dentro da função
    spark = SparkSession.builder.getOrCreate()

    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)

    # Listar arquivos no bucket raw
    arquivos = s3_client.list_objects_v2(Bucket=bucket_raw, Prefix="transporte-passageiros/")

    for obj in arquivos.get('Contents', []):
        arquivo_raw = obj['Key']
        ano, mes, nome_arquivo = arquivo_raw.split('/')[-3:]
        
        csv_path = f"/tmp/{nome_arquivo}"
        parquet_dir = f"/tmp/{nome_arquivo.replace('.csv', '')}"

        # Definir o caminho no bucket trusted
        path_trusted = f"transporte-passageiros/{ano}/{mes}/{nome_arquivo.replace('.csv', '')}"

        # Faz o download do arquivo CSV do bucket raw (MinIO)
        s3_client.download_file(bucket_raw, arquivo_raw, csv_path)
        
        # Carrega o CSV no Spark
        data = spark.read.option("header", True).csv(csv_path)

        # Registra o DataFrame como uma tabela temporária
        data.createOrReplaceTempView("data_table")

        # Executa a consulta SQL no DataFrame
        data = spark.sql('SELECT Data, Grupo as grupo, Lote as lote, Empresa as empresa, Linha as linha, `Passageiros Pagantes`, `Passageiros Com Gratuidade`, `Tot Passageiros Transportados` FROM data_table') \
                    .withColumnRenamed("Passageiros Pagantes", "passpag") \
                    .withColumnRenamed("Passageiros Com Gratuidade", "passgra") \
                    .withColumnRenamed("Tot Passageiros Transportados", "passtot")\
                    .withColumn("data", date_format(to_date("Data", "dd/MM/yyyy"), "yyyy-MM-dd"))

        # Cria o diretório para salvar o Parquet se ele não existir
        os.makedirs(parquet_dir, exist_ok=True)

        # Salva o DataFrame no formato Parquet no diretório temporário
        data.write.mode("overwrite").parquet(parquet_dir)

        # Faz o upload dos arquivos Parquet para o bucket "trusted" no Minio
        for i, each in enumerate(os.listdir(parquet_dir)):
            if each.endswith('.parquet'):  # Verifica se o arquivo é Parquet
                local_file = os.path.join(parquet_dir, each)
                s3_path = f"{path_trusted}_{i}.parquet"
                s3_client.upload_file(local_file, bucket_trusted, s3_path)
                print(f"Uploaded {local_file} to {bucket_trusted}/{s3_path}")

        # Remove arquivos temporários locais após o upload
        os.remove(csv_path)
        for each in os.listdir(parquet_dir):
            os.remove(os.path.join(parquet_dir, each))
        os.rmdir(parquet_dir)

# Definições padrão para o DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG('StaticData_PassTransportados_toTrusted', default_args=default_args, 
         schedule_interval=None,
         catchup=False) as dag:
    
    process_data_task = PythonOperator(
        task_id='save_passTransportados_toTrusted',
        python_callable=save_PassTransportados_toTrusted
    )

    # Trigger para a próxima DAG
    trigger_refined = TriggerDagRunOperator(
        task_id='trigger_refined',
        trigger_dag_id='StaticData_PassTransportados_toRefined',
        wait_for_completion=True,
    )

    process_data_task >> trigger_refined
