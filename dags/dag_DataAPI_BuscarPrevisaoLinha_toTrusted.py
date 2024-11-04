import json
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
bucket_raw = "raw"
bucket_trusted = "trusted"

# Função para verificar e processar arquivos
def save_BuscarPrevisaoLinha_toTrusted(**kwargs):
    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)

    # Listar arquivos no bucket raw
    arquivos = s3_client.list_objects_v2(Bucket=bucket_raw, Prefix="previsao-linha/")

    for obj in arquivos.get('Contents', []):
        arquivo_raw = obj['Key']
        ano, mes, dia, nome_arquivo = arquivo_raw.split('/')[-4:]

        # Corrigir o path_trusted para não duplicar 'ano=', 'mes=' e 'dia='
        path_trusted = f"previsao-linha/ano={ano}/mes={mes}/dia={dia}/{nome_arquivo.replace('.json', '.parquet')}"
        try:
            s3_client.head_object(Bucket=bucket_trusted, Key=path_trusted)
            print(f"Arquivo {path_trusted} já existe na camada trusted. Pulando.")
            continue
        except s3_client.exceptions.ClientError:
            print(f"Processando arquivo {arquivo_raw}")

        # Baixar o arquivo JSON da camada raw
        response = s3_client.get_object(Bucket=bucket_raw, Key=arquivo_raw)
        conteudo = response['Body'].read().decode('utf-8')
        dados_json = json.loads(conteudo)

        # Transformar JSON em DataFrame conforme a estrutura da API Olho Vivo
        linhas = []
        
        for item in dados_json:
            codigo_linha = item.get("codigo_linha")
            
            for previsao in item.get("previsoes", []):
                # Ignorar se `vs` estiver vazio
                if not previsao.get("vs"):
                    continue
                
                # Extraindo dados de parada e veículo
                codigo_parada = previsao.get("cp")
                nome_parada = previsao.get("np")
                latitude_parada = previsao.get("py")
                longitude_parada = previsao.get("px")
                
                for veiculo in previsao.get("vs", []):
                    linhas.append({
                        "codigo_linha": codigo_linha,
                        "codigo_parada": codigo_parada,
                        "nome_parada": nome_parada,
                        "latitude_parada": latitude_parada,
                        "longitude_parada": longitude_parada,
                        "prefixo_veiculo": veiculo.get("p"),
                        "horario_chegada": veiculo.get("t"),
                        "acessivel": veiculo.get("a"),
                        "horario_localizacao": veiculo.get("ta"),
                        "latitude_veiculo": veiculo.get("py"),
                        "longitude_veiculo": veiculo.get("px")
                    })

        # Criar o DataFrame apenas se houver dados a serem salvos
        if linhas:
            df = pd.DataFrame(linhas)

            # Configurações para exibir todas as colunas
            pd.set_option('display.max_columns', None)
            pd.set_option('display.expand_frame_repr', False)

            # Converter 'horario_localizacao' para GMT-3
            try:
                df['horario_localizacao'] = pd.to_datetime(df['horario_localizacao']).dt.tz_convert('America/Sao_Paulo')
                print("Coluna 'horario_localizacao' convertida para GMT-3 com sucesso.")
            except Exception as e:
                print(f"Erro ao converter 'horario_localizacao' para GMT-3: {e}")
            print(df.head(3))

            # Salvar em Parquet
            try:
                if not os.path.exists(f"/tmp/{ano}/{mes}/{dia}"):
                    os.makedirs(f"/tmp/{ano}/{mes}/{dia}")

                arquivo_parquet = f"/tmp/{ano}/{mes}/{dia}/{nome_arquivo.replace('.json', '.parquet')}"
                df.to_parquet(arquivo_parquet)
                print(f"Arquivo Parquet {arquivo_parquet} criado com sucesso.")

                # Upload para a camada trusted
                with open(arquivo_parquet, "rb") as data:
                    s3_client.upload_fileobj(data, bucket_trusted, path_trusted)
                print(f"Arquivo {path_trusted} salvo na camada trusted.")
            except Exception as e:
                print(f"Erro ao salvar {arquivo_raw} na camada trusted: {e}")
        else:
            print(f"Não há previsões válidas no arquivo {arquivo_raw}. Nenhum dado foi salvo.")

# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

with DAG('DataAPI_BuscarPrevisaoLinha_toTrusted', default_args=default_args, 
         schedule_interval=None,
         catchup=False) as dag:
    
    process_data_task = PythonOperator(
        task_id='save_BuscarPrevisaoLinha_toTrusted',
        python_callable=save_BuscarPrevisaoLinha_toTrusted
    )

    # Trigger para a próxima DAG
    trigger_refined = TriggerDagRunOperator(
        task_id='trigger_refined',
        trigger_dag_id='DataAPI_BuscarPrevisaoLinha_toRefined',
        wait_for_completion=True,
    )

    process_data_task >> trigger_refined