import os
import zipfile
import logging
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task

from airflow.providers.google.cloud.hooks.gcs import GCSHook
# 'https://gist.githubusercontent.com/kabab/95d2c25b32909367a7a93fb9708349c5/raw/c9c6cbfb8a0166d322db868867ed9eda0156672a/houses_prices.csv'
# Caminhos dos arquivos
caminho = 'https://gist.githubusercontent.com/kabab/95d2c25b32909367a7a93fb9708349c5/raw/c9c6cbfb8a0166d322db868867ed9eda0156672a/houses_prices.csv'
conn_id = 'gcs_default'

hook = GCSHook(gcp_conn_id=conn_id)


@dag(
    dag_id="load",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def my_dag():

    @task(task_id="load")
    def load():
        # Lê o arquivo CSV
        df = pd.read_csv(caminho, sep=",")
        logging.info(f"Arquivo lido de {caminho}")
        return df.to_json(orient='split')

    @task(task_id="save")
    def save(data):
        df = pd.read_json(data, orient='split')
        print(df)
        logging.info(f"Arquivo salvo")

    @task(task_id="list")
    def listar():
        # Lista os arquivos do bucket/2025
        files = hook.list(
            prefix='cnpj/2025-02/estabelecimentos', bucket_name='dataita')
        logging.info(f"Arquivos no bucket: {files}")
        return files

    dados = load()
    salvar = save(dados)
    lista = listar()

    dados >> salvar >> lista


# Instancia a DAG
dag_instance = my_dag()
