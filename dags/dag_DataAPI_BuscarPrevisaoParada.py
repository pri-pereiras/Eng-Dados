import requests
import json
import boto3
import pytz
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Configurações do MinIO
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_raw = "raw"
api_key = "9aa2fcbfb81e92aaf26c640c539848fa69193acd16d9784ec862d1d42b29d28c"

# Função para buscar previsões de chegada e consolidar as respostas válidas em um único arquivo
def GetData_API_PrevisaoParada(**kwargs):
    auth_url = "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar"
    params = {"token": api_key}
    auth_response = requests.post(auth_url, params=params)
    
    if auth_response.status_code == 200 and auth_response.text == 'true':
        print("Autenticação bem-sucedida!")
        s3_client = boto3.client('s3',
                                 endpoint_url=minio_endpoint,
                                 aws_access_key_id=minio_access_key,
                                 aws_secret_access_key=minio_secret_key)

        # Lista para consolidar apenas as respostas válidas
        consolidated_data = []

        # Iterar sobre prefixos de 0 a 9 para parada e linha
        for prefix_parada in range(10):
            # Chamar a API para previsão de chegada com cada combinação de prefixos
            url = f"http://api.olhovivo.sptrans.com.br/v2.1/Previsao/Parada?codigoParada={prefix_parada}"
            response = requests.get(url, cookies=auth_response.cookies)
            
            if response.status_code == 200:
                data = response.json()
                
                # Verificar se a resposta contém dados válidos em "p"
                if data.get("p"):  # Adiciona se o campo "p" não for nulo
                    consolidated_data.append(data)
                    print(f"Dados válidos adicionados para parada {prefix_parada}")
                else:
                    print(f"Nenhum dado válido para parada {prefix_parada}  neste horário.")
            else:
                print(f"Erro ao buscar dados para parada {prefix_parada} : {response.text}")
        
        # Salvar os dados consolidados em um único arquivo JSON no MinIO
        if consolidated_data:
            # Configuração do fuso horário para GMT-3
            timezone = pytz.timezone("America/Sao_Paulo")
            now = datetime.now(timezone)
            
            ano = now.strftime('%Y')
            mes = now.strftime('%m')
            dia = now.strftime('%d')
            hora = now.strftime('%H%M')
            file_name = f"previsao_parada_consolidado_{now.strftime('%Y%m%d_%H%M')}.json"
            path = f"previsao-parada/ano={ano}/mes={mes}/dia={dia}/{file_name}"
            
            # Salvar o arquivo consolidado no MinIO
            s3_client.put_object(Bucket=bucket_raw, Key=path, Body=json.dumps(consolidated_data))
            print(f"Arquivo consolidado salvo no MinIO em {path}.")
        else:
            print("Nenhum dado consolidado para salvar.")
    else:
        print("Falha na autenticação com a API")

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('Data_API_BuscarPrevisaoParada_SaveMinIO', default_args=default_args, schedule_interval='*/5 * * * *', catchup=False) as dag:
    # Tarefa de coleta de dados para todas as combinações de prefixos de parada e linha de 0 a 9
    fetch_task = PythonOperator(
        task_id='GetData_API_PrevisaoParada',
        python_callable=GetData_API_PrevisaoParada
    )
