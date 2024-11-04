# import requests
# import json
# import boto3
# import pytz
# from datetime import datetime
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago

# # Configurações do MinIO
# minio_endpoint = "http://host.docker.internal:9050"
# minio_access_key = "datalake"
# minio_secret_key = "datalake"
# bucket_raw = "raw"
# api_key = "9aa2fcbfb81e92aaf26c640c539848fa69193acd16d9784ec862d1d42b29d28c"

# # Função para buscar posição de veículos em garagens para todas as combinações de empresas e linhas com prefixos 0-9
# def GetData_API_garagemPositions(**kwargs):
#     auth_url = "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar"
#     params = {"token": api_key}
#     auth_response = requests.post(auth_url, params=params)
    
#     if auth_response.status_code == 200 and auth_response.text == 'true':
#         print("Autenticação bem-sucedida!")
#         s3_client = boto3.client('s3',
#                                  endpoint_url=minio_endpoint,
#                                  aws_access_key_id=minio_access_key,
#                                  aws_secret_access_key=minio_secret_key)

#         # Lista para armazenar todos os dados consolidados
#         all_data = []

#         # Iterar sobre prefixos de 0 a 9 para empresa e linha
#         for prefix_empresa in range(10):
#             for prefix_linha in range(10):
#                 # Chamar a API para posição de veículos na garagem com cada combinação de prefixos
#                 url = f"http://api.olhovivo.sptrans.com.br/v2.1/Posicao/Garagem?codigoEmpresa={prefix_empresa}&codigoLinha={prefix_linha}"
#                 response = requests.get(url, cookies=auth_response.cookies)
                
#                 if response.status_code == 200:
#                     data = response.json()
#                     print(f"Results: {data}")
                    
#                     # Verificar se há dados na resposta antes de adicionar à lista consolidada
#                     if data["l"]:  # Só adiciona se "l" não estiver vazio
#                         all_data.append(data)
#                     else:
#                         print(f"Nenhum dado para empresa {prefix_empresa} e linha {prefix_linha} neste horário.")
#                 else:
#                     print(f"Erro ao buscar posição de veículos na garagem para empresa {prefix_empresa} e linha {prefix_linha}: {response.text}")
        
#         # Salvar o arquivo consolidado no MinIO se houver dados
#         if all_data:
#             # Configuração do fuso horário para GMT-3
#             timezone = pytz.timezone("America/Sao_Paulo")
#             now = datetime.now(timezone)
            
#             # Estrutura do nome e caminho do arquivo
#             ano = now.strftime('%Y')
#             mes = now.strftime('%m')
#             dia = now.strftime('%d')
#             hora = now.strftime('%H%M')
#             file_name = f"garagem_posicao_consolidado_{now.strftime('%Y%m%d_%H%M')}.json"
#             path = f"garagem-posicao/ano={ano}/mes={mes}/dia={dia}/hora={now.strftime('%H')}/{file_name}"
            
#             # Salvar JSON consolidado no MinIO
#             s3_client.put_object(Bucket=bucket_raw, Key=path, Body=json.dumps(all_data))
#             print(f"Dados consolidados de posição de garagem salvos no MinIO em {path}.")
#         else:
#             print("Nenhum dado encontrado para salvar.")
#     else:
#         print("Falha na autenticação com a API")

# # Configuração da DAG
# default_args = {
#     'owner': 'airflow',
#     'start_date': days_ago(1),
#     'retries': 0,
# }

# with DAG('Data_API_BuscarGaragemPositions_toRaw', default_args=default_args, schedule_interval='*/15 * * * *', catchup=False) as dag:
#     # Tarefa de coleta de dados para todas as combinações de empresas e linhas com prefixos 0-9
#     fetch_task = PythonOperator(
#         task_id='GetData_API_garagemPositions',
#         python_callable=GetData_API_garagemPositions
#     )
