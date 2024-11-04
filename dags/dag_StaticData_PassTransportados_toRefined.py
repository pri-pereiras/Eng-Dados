import os
import boto3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configurações do MinIO
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_raw = "raw"
bucket_trusted = "trusted"
bucket_refined = 'refined'

# Função para verificar e processar arquivos
def save_PassTransportados_toRefined(**kwargs):
    # Inicializa a sessão do Spark dentro da função
    spark = SparkSession.builder.getOrCreate()

    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)

    # Listar arquivos no bucket trusted
    arquivos = s3_client.list_objects_v2(Bucket=bucket_trusted, Prefix="transporte-passageiros/")

    for obj in arquivos.get('Contents', []):
        arquivo_raw = obj['Key']
        ano, mes, nome_arquivo = arquivo_raw.split('/')[-3:]
        
        parquet_dir = f"/tmp/{nome_arquivo.replace('.parquet', '')}"
        local_parquet_path = f"/tmp/{nome_arquivo}"

        # Faz o download do arquivo Parquet do bucket trusted (MinIO)
        s3_client.download_file(bucket_trusted, arquivo_raw, local_parquet_path)
        
        # Carrega o arquivo Parquet no Spark
        data = spark.read.parquet(local_parquet_path)

        # Cria um campo chamado letreiro para pegar os 6 primeiros caracteres da coluna Linha
        data_refined = data.withColumn('letreiro', col("linha").substr(1, 6))

        # Cria o diretório temporário se ele não existir
        os.makedirs(parquet_dir, exist_ok=True)

        # Salva o DataFrame refinado no formato Parquet no diretório temporário
        data_refined.write.mode("overwrite").parquet(parquet_dir)

        # Faz o upload dos arquivos Parquet para o bucket "refined" no Minio
        for i, each in enumerate(os.listdir(parquet_dir)):
            if each.endswith('.parquet'):  # Verifica se o arquivo é Parquet
                local_file = os.path.join(parquet_dir, each)
                s3_path = f"transporte-passageiros/{ano}/{mes}/{nome_arquivo.replace('.parquet', '')}_{i}.parquet"
                s3_client.upload_file(local_file, bucket_refined, s3_path)
                print(f"Uploaded {local_file} to {bucket_refined}/{s3_path}")

        # Remover arquivos temporários
        os.remove(local_parquet_path)
        for each in os.listdir(parquet_dir):
            os.remove(os.path.join(parquet_dir, each))
        os.rmdir(parquet_dir)

# Definições padrão para o DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG('StaticData_PassTransportados_toRefined', default_args=default_args, 
         schedule_interval=None,
         catchup=False) as dag:
    
    process_data_task = PythonOperator(
        task_id='save_passTransportados_toRefined',
        python_callable=save_PassTransportados_toRefined
    )

    # Trigger para a próxima DAG
    trigger_postgres = TriggerDagRunOperator(
        task_id='trigger_postgres',
        trigger_dag_id='StaticData_PassTransportados_toPostgres',
        wait_for_completion=True,
    )

    process_data_task >> trigger_postgres