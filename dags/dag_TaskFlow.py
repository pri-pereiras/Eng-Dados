from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

# Funções dummy para exemplificar o fluxo de trabalho
def task_example():
    pass

# Configurações padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
}

# DAG 1: Executa primeiro
with DAG(
    'DataAPI_BuscaLinhas_SaveMinIO',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag_linhas:
    
    start = DummyOperator(task_id="start")
    task = PythonOperator(
        task_id='run_busca_linhas',
        python_callable=task_example
    )
    end = DummyOperator(task_id="end")
    
    start >> task >> end

# DAG 2: Executa após DAG 1
with DAG(
    'DataAPI_BuscarParadasPorLinha_SaveMinIO',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag_paradas_linha:

    wait_for_linhas = ExternalTaskSensor(
        task_id="wait_for_linhas",
        external_dag_id='DataAPI_BuscaLinhas_SaveMinIO',
        external_task_id="end",
        mode='poke'
    )
    task = PythonOperator(
        task_id='run_buscar_paradas_linha',
        python_callable=task_example
    )
    
    wait_for_linhas >> task

# DAGs independentes
dag_names = [
    'DataAPI_BuscarCorredores_SaveMinIO',
    'DataAPI_BuscarEmpresas_SaveMinIO',
    'Data_API_BuscarGaragemPositions_SaveMinIO',
    'DataAPI_BuscarParadasPorCorredor_SaveMinIO',
    'Data_API_BuscarParadas_SaveMinIO',
    'DataAPI_BuscarPosicao_SaveMinIO',
    'Data_API_BuscarPrevisaoLinha_SaveMinIO',
    'Data_API_BuscarPrevisaoParada_SaveMinIO',
    'DataAPI_Velocidade_KMZ_SaveMinIO',
    'StaticData_GTFS_toMinio',
    'StaticData_OpenStreetMap_toMinio',
    'StaticsData_passageirosTransportados_toMinio'
]

for name in dag_names:
    with DAG(
        name,
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
    ) as dag:
        
        task = PythonOperator(
            task_id=f'run_{name}',
            python_callable=task_example
        )
