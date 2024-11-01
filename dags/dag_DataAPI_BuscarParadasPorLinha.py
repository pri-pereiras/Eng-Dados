import requests
import pandas as pd
import boto3
import os
import logging
from io import StringIO
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger("airflow")

# Configurações da API e MinIO
api_key = "9aa2fcbfb81e92aaf26c640c539848fa69193acd16d9784ec862d1d42b29d28c"
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_name = "raw"
linha_csv_path = "/tmp/linhas.csv"  # Caminho temporário para salvar o CSV

# Função para baixar o arquivo CSV de linhas do MinIO
def baixar_csv_linhas_minio():
    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)
    
    # Baixa o arquivo CSV de linhas do MinIO
    s3_client.download_file(bucket_name, "DataAPI_BuscaLinhas.csv", linha_csv_path)
    print("Arquivo CSV de linhas baixado com sucesso.")

# Função de autenticação
def autenticar(api_key):
    auth_url = "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar"
    params = {"token": api_key}
    
    # Autenticação
    auth_response = requests.post(auth_url, params=params)
    if auth_response.status_code == 200 and auth_response.text == 'true':
        print("Autenticação bem-sucedida!")
        return auth_response.cookies
    else:
        raise Exception("Falha na autenticação")

# Função para buscar todas as paradas de uma linha
def buscar_paradas_por_linha(codigo_linha, cookies):
    search_url = "http://api.olhovivo.sptrans.com.br/v2.1/Parada/BuscarParadasPorLinha"
    response = requests.get(search_url,  params={"codigoLinha": codigo_linha}, cookies=cookies)
    print("iniciando busca por paradas de uma linha")
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao buscar paradas para linha {codigo_linha}: {response.text}")
        return []

# Função principal para obter todas as paradas de todas as linhas e salvar em um arquivo CSV
def GetData_API_BuscarParadasPorLinha(**kwargs):
    # Autenticação
    cookies = autenticar(api_key)
    
    # Carregar a relação de linhas a partir do CSV baixado
    print("Carregando a base de linhas baixada do Minio")
    df_linhas = pd.read_csv(linha_csv_path)
    
    # Armazenar todas as paradas de todas as linhas
    dados_paradas = []

    for _, linha in df_linhas.iterrows():
        codigo_linha = linha["cl"]
        sentido = linha["sl"]
                
        # Obter as paradas para a linha
        paradas = buscar_paradas_por_linha(codigo_linha, cookies)
        print(f"Inciando busca de paradas da linha {codigo_linha}")
        
        for parada in paradas:
            dados_paradas.append({
                "codigo_linha": codigo_linha,
                "sentido": sentido,
                "codigo_parada": parada["cp"],
                "nome_parada": parada["np"],
                "endereco": parada["ed"],
                "latitude": parada["py"],
                "longitude": parada["px"]
            })

    # Converter para DataFrame e salvar como CSV
    df_paradas = pd.DataFrame(dados_paradas)
    
    # Armazenar o DataFrame no XCom
    kwargs['ti'].xcom_push(key='data_ParadasPorLinha', value=df_paradas.to_dict())
    print("Dados armazenados no XCom com sucesso!")
    
    # Função para salvar os dados no MinIO
def save_ParadasPorLinha_to_minio(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='data_ParadasPorLinha', task_ids='GetData_API_BuscarParadasPorLinha')
    
    if data is None:
        print("Nenhum dado encontrado no XCom.")
        return
    
    df = pd.DataFrame(data)
    local_file = "/tmp/DataAPI_BuscarParadasPorLinha.csv"
    print(f"Salvando o DataFrame no arquivo {local_file}.")
    df.to_csv(local_file, index=False)
    
    # Verificar se o arquivo foi criado com sucesso
    if os.path.exists(local_file):
        print(f"O arquivo {local_file} foi salvo com sucesso.")
    
    # Configurações do MinIO (ajuste conforme necessário)
    minio_endpoint = "http://host.docker.internal:9050"
    minio_access_key = "datalake"
    minio_secret_key = "datalake"
    bucket_name = "raw"
    object_name = "DataAPI_BuscarParadasPorLinha.csv"
    
    
    # Conectar e salvar no MinIO
    try:
        s3_client = boto3.client('s3',
                                 endpoint_url=minio_endpoint,
                                 aws_access_key_id=minio_access_key,
                                 aws_secret_access_key=minio_secret_key)
        
        # Verificar e criar bucket se necessário
        try:
            s3_client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' encontrado.")
        except:
            print(f"Bucket '{bucket_name}' não encontrado. Criando bucket.")
            s3_client.create_bucket(Bucket=bucket_name)
        
        
        s3_client.upload_file(local_file, bucket_name, object_name)
        print(f"Arquivo {local_file} salvo no bucket {bucket_name} como {object_name}.")
    except Exception as e:
        logger.error(f"Erro ao enviar o arquivo para o MinIO: {str(e)}")

# Configurações da DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'DataAPI_BuscarParadasPorLinha_SaveMinIO',
    default_args=default_args,
    description='DAG para buscar todas as paradas de todas as linhas a partir de um CSV no MinIO e salvar no MinIO',
    schedule_interval='@daily',  # Executar uma vez por mês
    catchup=False,
) as dag:

    # Tarefa para baixar o CSV de linhas do MinIO
    task_baixar_csv_linhas = PythonOperator(
        task_id='baixar_csv_linhas_minio',
        python_callable=baixar_csv_linhas_minio,
        provide_context=True,
    )

    # Tarefa para obter todas as paradas por linha
    task_GetData_API_BuscarParadasPorLinha = PythonOperator(
        task_id='GetData_API_BuscarParadasPorLinha',
        python_callable=GetData_API_BuscarParadasPorLinha,
        provide_context=True,
    )
    
        # Tarefa para obter todas as paradas por linha
    task_save_ParadasPorLinha_to_minio = PythonOperator(
        task_id='save_ParadasPorLinha_to_minio',
        python_callable=save_ParadasPorLinha_to_minio,
        provide_context=True,
    )    
    

    # Definir a ordem das tarefas
    task_baixar_csv_linhas >> task_GetData_API_BuscarParadasPorLinha >> task_save_ParadasPorLinha_to_minio
