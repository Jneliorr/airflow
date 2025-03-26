import logging
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.models.param import Param


DEFAULT_TABELA_VERIFICADA = "infra-itaborai.teste_k8s.daspag"
DEFAULT_TABELA_SALVA = "infra-itaborai.teste_k8s.daspag_drop_distinct"

chave = BigQueryHook(gcp_conn_id='gcs_default')


@dag(
     dag_id="drop_duplicate_gbq"
    ,schedule="@daily"
    ,start_date=datetime(2025, 3, 1)
    ,catchup=False
    ,tags=["drop", "duplicate", "gbq"]
    ,params={
        "verify_table": Param(
            default=DEFAULT_TABELA_VERIFICADA,
            type='string',
            enum=["teste1", "teste2", DEFAULT_TABELA_VERIFICADA]
        ),
        "save_table": Param(
            default=DEFAULT_TABELA_SALVA,
            type='string',
            enum=["infra-itaborai.teste_k8s.daspag_drop_distinct1", "infra-itaborai.teste_k8s.daspag_drop_distinct2", DEFAULT_TABELA_SALVA])}
    ,description="""
    Essa DAG verifica a existência de duplicatas na tabela especificada no BigQuery, se TRUE uma cria uma tabela contendo apenas registros distintos)
    """
)


def load_to_gbq():

    start = EmptyOperator(task_id='start')

    @task(
        task_id="access_gbq",
        doc_md="Testa a conexão com o BigQuery"
    )
    def access():
        client = chave.get_client()
        logging.info("Conexão com o BigQuery realizada com sucesso!")
        return "Acesso ao BigQuery realizado com sucesso!"

    @task(
        task_id="load_to_gbq",
        doc_md="Carrega dados do BigQuery buscando registros duplicados"
    )
    def load():
        context = get_current_context()
        verify_table = context["params"]["verify_table"]
        client = chave.get_client()
        query = f"SELECT numero_das,COUNT(*) AS duplicate FROM `{verify_table}` GROUP BY numero_das HAVING COUNT(*) > 1"
        df = client.query(query).to_dataframe()
        logging.info(f"Dados lidos da tabela")
        return df.to_json(orient='split')

    @task.branch(
        task_id="branch_on_duplicates",
        doc_md="Decide o caminho da DAG com base na existência de duplicatas")
    def branch_on_duplicates(data: str) -> str:
        df = pd.read_json(data, orient='split')
        if not df.empty:
            logging.info("Duplicatas encontradas")
            return "create_distinct_table"
        else:
            logging.info("Nenhuma duplicata encontrada")
            return "end"

    @task(
        task_id="create_distinct_table",
        doc_md="Cria uma nova tabela com registros distintos removendo duplicatas."
    )
    def create_distinct_table():
        context = get_current_context()
        save_table = context["params"]["save_table"]
        verify_table = context["params"]["verify_table"]
        client = chave.get_client()
        query = f"CREATE OR REPLACE TABLE `{save_table}` AS SELECT DISTINCT numero_das FROM `{verify_table}`"
        query_job = client.query(query)
        query_job.result()
        logging.info("Tabela criada com sucesso!")
        return "Tabela criada"

    end = EmptyOperator(task_id='end', trigger_rule="none_failed_or_skipped")

    acesso_out = access()
    data_out = load()
    branch_out = branch_on_duplicates(data_out)
    start >> acesso_out >> data_out >> branch_out
    branch_out >> create_distinct_table() >> end
    branch_out >> end


dag_instance = load_to_gbq()
