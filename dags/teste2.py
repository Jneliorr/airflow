import os
import zipfile
import logging
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery


# URL do CSV em formato bruto
caminho = 'https://gist.githubusercontent.com/kabab/95d2c25b32909367a7a93fb9708349c5/raw/c9c6cbfb8a0166d322db868867ed9eda0156672a/houses_prices.csv'


@dag(
    dag_id="teste2",
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
        # Retorna o DataFrame convertido para JSON com orient='split'
        return df.to_json(orient='split')

    @task(task_id="transform")
    def transform(data):
        # Reconstrói o DataFrame a partir do JSON recebido
        df = pd.read_json(data, orient='split')
        # Seleciona as linhas onde o valor da coluna 'price' é igual a 980000
        df_filtered = df[df['price'] == 980000]
        logging.info("Seleção realizada com sucesso.")
        # Retorna o DataFrame filtrado em JSON
        return df_filtered.to_json(orient='split')

    @task(task_id="save")
    def save(data):
        # Reconstrói o DataFrame a partir do JSON recebido
        df = pd.read_json(data, orient='split')
        # Utiliza o BigQueryHook para obter um cliente autenticado com as credenciais configuradas em "gcs_default"
        bq_hook = BigQueryHook(gcp_conn_id='gcs_default')
        client = bq_hook.get_client()
        # Define o ID completo da tabela: projeto.dataset.tabela
        table_id = "infra-itaborai.teste2.airflow_price"
        # Configuração do job para carregar os dados (aqui estamos sobrescrevendo a tabela a cada execução)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # sobrescreve a tabela
            source_format=bigquery.SourceFormat.CSV,  # especifica o formato do arquivo
        )
        # Carrega o DataFrame diretamente na tabela do BigQuery
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config)
        job.result()  # Aguarda a conclusão do job
        logging.info("Dados carregados no BigQuery com sucesso.")
        return "BigQuery load complete"

    # Encadeamento das tasks: load >> transform >> save
    dados = load()
    dados_transformados = transform(dados)
    salvar = save(dados_transformados)

    dados >> dados_transformados >> salvar


# Instancia a DAG
dag_instance = my_dag()
