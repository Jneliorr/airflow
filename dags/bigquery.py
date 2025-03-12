import os
import zipfile
import logging
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

# Variáveis com os nomes das tabelas
leituraTabela = "infra-itaborai.dbt_cnpj_rfb.d_situacao_cadastral"
salvarTabela = "infra-itaborai.teste2.airflow_situacao_cadastral"


@dag(
    dag_id="BIGQUERY",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def my_dag():

    @task(task_id="load")
    def load():
        # Lê dados da tabela de origem no BigQuery
        bq_hook = BigQueryHook(gcp_conn_id='gcs_default')
        client = bq_hook.get_client()
        query = f"SELECT * FROM `{leituraTabela}`"  # Usa a variável
        df = client.query(query).to_dataframe()
        # df["id_situacao"] = df["id_situacao"].astype(str)
        # df["descricao_situacao"] = df["descricao_situacao"].astype(str)
        logging.info(f"Dados lidos da tabela {leituraTabela}")
        return df.to_json(orient='split')

    @task(task_id="transform")
    def transform(data):
        # Reconstrói o DataFrame a partir do JSON recebido
        df = pd.read_json(data, orient='split').astype(
            {'id_situacao': str, 'descricao_situacao': str})
        # Filtra as linhas onde id_situacao é igual a "8"
        df_filtered = df[df['id_situacao'] == "8"]
        logging.info("Seleção realizada com sucesso.")
        return df_filtered.to_json(orient='split')

    @task(task_id="save")
    def save(data):
        # Reconstrói o DataFrame a partir do JSON recebido
        df = pd.read_json(data, orient='split')
        bq_hook = BigQueryHook(gcp_conn_id='gcs_default')
        client = bq_hook.get_client()
        table_id = salvarTabela  # Utiliza a variável
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Sobrescreve a tabela
            source_format=bigquery.SourceFormat.CSV,
        )
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config)
        job.result()  # Aguarda a conclusão do job
        logging.info("Dados carregados no BigQuery com sucesso.")
        return "BigQuery load complete"

    # Encadeamento das tasks: load >> transform >> save
    dados = load()
    dados_transformados = transform(dados)
    resultado = save(dados_transformados)

    dados >> dados_transformados >> resultado


# Instancia a DAG
dag_instance = my_dag()
