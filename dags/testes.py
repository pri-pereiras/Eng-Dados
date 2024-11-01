import boto3
import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pyspark

from airflow.utils.dates import days_ago

from pyspark.sql import SparkSession

import os


logger = logging.getLogger("airflow")

minio_endpoint = "http://host.docker.internal:9050"
minio_access_key = "datalake"
minio_secret_key = "datalake"
bucket_name = 'raw'


spark = SparkSession.builder.getOrCreate()

def open_xlsx():
    s3_client = boto3.client('s3',
                                 endpoint_url=minio_endpoint,
                                 aws_access_key_id=minio_access_key,
                                 aws_secret_access_key=minio_secret_key)
    s3_client.download_file(bucket_name, "shapes.txt", '/tmp/shapes.txt')

    data = spark.read.option("header", True).csv('/tmp/shapes.txt')

    data = spark.sql('SELECT shape_id as sp,shape_pt_lat as sp_pt,shape_pt_sequence sp_sq FROM {data} ', data=data)

    data.write.mode("overwrite").parquet('/tmp/shapes')

    i=0
    for each in os.listdir('/tmp/shapes'):
        s3_client.upload_file('/tmp/shapes/'+each, 'trusted', 'shapes/shapes{}.parquet'.format(i))
        i+=1


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}


with DAG('save_DataAPI_BuscaLinhas_to_minio',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    
    save_task = PythonOperator(
        task_id='save_DataAPI_BuscaLinhas_to_minio',
        python_callable=open_xlsx,
        provide_context=True
    )
