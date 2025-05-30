import logging
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.models.param import Param


chave = BigQueryHook(gcp_conn_id='gcs_default')

default_args = { 
    "owner": "José Nélio"
    ,"start_date": None
    ,"retries": 3
    # "retry_delay": datetime.timedelta(minutes=5)
    }

@dag(
    dag_id="nelio_project_student"
    ,default_args=default_args
    ,schedule=None
    ,start_date=datetime(2025, 3, 1)
    ,catchup=False
    ,tags=["Teste", "Estudo"]
    ,params={
        "verify_table": Param(
            default="infra-itaborai.teste_k8s.daspag",
            type='string',
            enum=["infra-itaborai.teste_k8s.daspag"]
        ),
        "save_table": Param(
            default="infra-itaborai.teste_k8s.daspag_drop_distinct",
            type='string',
            enum=["infra-itaborai.teste_k8s.daspag_drop_distinct"]),
        "Usuario": Param(
            default="José Nélio"
            ,type='string'
            ,enum=["José Nélio", "Vinicius B. Soares"])
        }
    ,description="""Cria estagios de tabelas decred"""
)


def load_to_gbq():

    start = EmptyOperator(task_id='start')

    @task(
        task_id="access_gbq"
        ,doc_md="Testa a conexão com o BigQuery"
    )
    def access():
        chave.get_client()
        logging.info("Conexão com o BigQuery realizada com sucesso!")
        return "Acesso ao BigQuery realizado com sucesso!"

    end = EmptyOperator(task_id='end', trigger_rule="none_failed_or_skipped")

  
    verificacao_acesso = access()
    
    start [verificacao_acesso ] >> end


load_to_gbq()
