import os
import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Configurações do MinIO
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_refined = "refined"

# Configurações do PostgreSQL
postgres_host = "host.docker.internal"
postgres_port = "5432"
postgres_db = "postgres"
postgres_user = "airflow"
postgres_password = "airflow"
postgres_schema = "dados_no_ponto"
postgres_table = "tb_paradas"

# Função para salvar os arquivos Parquet no PostgreSQL
def save_refined_to_postgres(**kwargs):
    # Conectar ao MinIO e ao PostgreSQL
    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)

    engine = create_engine(f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}")

    # Criar schema e tabela se não existirem
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {postgres_schema};"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {postgres_schema}.{postgres_table} (
                codigo_parada INT,
                nome_parada TEXT,
                endereco_parada TEXT,
                latitude FLOAT,
                longitude FLOAT
            );
        """))
        conn.execute(text(f"TRUNCATE TABLE {postgres_schema}.{postgres_table};"))
        print("Schema e tabela verificados/criados com sucesso.")

    # Listar arquivos no bucket refined
    arquivos = s3_client.list_objects_v2(Bucket=bucket_refined, Prefix="Paradas/")

    # Processar cada arquivo individualmente
    for obj in arquivos.get('Contents', []):
        arquivo_refined = obj['Key']
        nome_arquivo = arquivo_refined.split('/')[-1]

        # Baixar o arquivo Parquet da camada refined
        response = s3_client.get_object(Bucket=bucket_refined, Key=arquivo_refined)
        local_path = f"/tmp/{nome_arquivo}"
        with open(local_path, "wb") as file:
            file.write(response['Body'].read())

        # Ler o arquivo Parquet em um DataFrame
        df = pd.read_parquet(local_path)

        # Inserir os dados no PostgreSQL
        try:
            with engine.connect() as conn:
                df.to_sql(postgres_table, conn, schema=postgres_schema, if_exists="append", index=False)
            print(f"Dados do arquivo {nome_arquivo} inseridos com sucesso na tabela.")
        except Exception as e:
            print(f"Erro ao inserir dados do arquivo {nome_arquivo}: {e}")
        finally:
            # Remover o arquivo temporário xxx
            os.remove(local_path)

# Definições padrão para o DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

# Criação do DAG no Airflow
with DAG('DataAPI_BuscarParadas_toPostgres',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    
    save_task = PythonOperator(
        task_id='save_Paradas_toPostgres',
        python_callable=save_refined_to_postgres
    )

    # Apenas a task de salvamento
    save_task
#