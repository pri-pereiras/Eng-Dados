import json
import os
import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime

# Configurações do MinIO
minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_raw = "raw"
bucket_trusted = "trusted"

# Função para verificar e processar arquivos
def save_BuscarPosicao_toTrusted(**kwargs):
    s3_client = boto3.client('s3',
                             endpoint_url=minio_endpoint,
                             aws_access_key_id=minio_access_key,
                             aws_secret_access_key=minio_secret_key)

    # Listar arquivos no bucket raw
    arquivos = s3_client.list_objects_v2(Bucket=bucket_raw, Prefix="veiculos-posicao/")

    for obj in arquivos.get('Contents', []):
        try:
            arquivo_raw = obj['Key']
            ano, mes, dia, nome_arquivo = arquivo_raw.split('/')[-4:]

            # Corrigir o path_trusted para não duplicar 'ano=', 'mes=' e 'dia='
            path_trusted = f"veiculos-posicao/ano={ano}/mes={mes}/dia={dia}/{nome_arquivo.replace('.json', '.parquet')}"
            
            # Verificar se o arquivo já existe na trusted
            try:
                s3_client.head_object(Bucket=bucket_trusted, Key=path_trusted)
                print(f"Arquivo {path_trusted} já existe na camada trusted. Pulando.")
                continue
            except s3_client.exceptions.ClientError:
                print(f"Processando arquivo {arquivo_raw}")

            # Baixar o arquivo JSON da camada raw
            response = s3_client.get_object(Bucket=bucket_raw, Key=arquivo_raw)
            conteudo = response['Body'].read().decode('utf-8')
            json_data = json.loads(conteudo)

            # Transformar JSON em DataFrame conforme a estrutura da API Olho Vivo
            dados = []

            # Extrair os dados do JSON para a tabela
            horario_referencia = json_data["hr"]
            for linha in json_data["l"]:
                letreiro_completo = linha["c"]
                codigo_linha = linha["cl"]
                sentido_operacao = linha["sl"]
                letreiro_destino = linha["lt0"]
                letreiro_origem = linha["lt1"]
                quantidade_veiculos = linha["qv"]

                # Para cada veículo na linha, extrair dados
                for veiculo in linha["vs"]:
                    try:
                        dados.append({
                            "horario_referencia": horario_referencia,
                            "letreiro_completo": letreiro_completo,
                            "codigo_linha": codigo_linha,
                            "sentido_operacao": sentido_operacao,
                            "letreiro_destino": letreiro_destino,
                            "letreiro_origem": letreiro_origem,
                            "quantidade_veiculos": quantidade_veiculos,
                            "prefixo_veiculo": veiculo["p"],
                            "acessivel": veiculo["a"],
                            "horario_localizacao": veiculo["ta"],
                            "latitude_veiculo": veiculo["py"],
                            "longitude_veiculo": veiculo["px"]
                        })
                    except KeyError as e:
                        print(f"Erro ao processar veículo. Campo ausente: {e}. Pulando para o próximo veículo.")
                    except Exception as e:
                        print(f"Erro inesperado ao processar veículo: {e}. Pulando para o próximo veículo.")

            # Converter para DataFrame
            if dados:
                df = pd.DataFrame(dados)

                # Configurações para exibir todas as colunas
                pd.set_option('display.max_columns', None)
                pd.set_option('display.expand_frame_repr', False)

                # # Converter 'horario_localizacao' para GMT-3
                # try:
                #     df['horario_localizacao'] = pd.to_datetime(df['horario_localizacao']).dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo')
                #     print("Coluna 'horario_localizacao' convertida para GMT-3 com sucesso.")
                # except Exception as e:
                #     print(f"Erro ao converter 'horario_localizacao' para GMT-3: {e}")

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
                    print(f"Erro ao salvar {arquivo_raw} na camada trusted: {e}. Pulando para o próximo arquivo.")
            else:
                print(f"Não há previsões válidas no arquivo {arquivo_raw}. Nenhum dado foi salvo.")

        except Exception as e:
            print(f"Erro ao processar o arquivo {obj['Key']}: {e}. Pulando para o próximo arquivo.")

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

# Configuração do DAG
with DAG('DataAPI_BuscarPosicao_toTrusted', default_args=default_args, 
         schedule_interval=None,
         catchup=False) as dag:
    
    process_data_task = PythonOperator(
        task_id='save_BuscarPosicao_toTrusted',
        python_callable=save_BuscarPosicao_toTrusted
    )

    # Trigger para a próxima DAG
    trigger_refined = TriggerDagRunOperator(
        task_id='trigger_refined',
        trigger_dag_id='DataAPI_BuscarPosicao_toRefined',
        wait_for_completion=True,
    )

    process_data_task >> trigger_refined