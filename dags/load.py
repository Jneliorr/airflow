import os
import zipfile
import logging
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task

# Caminhos dos arquivos
caminho = r"C:\Users\macie\Documents\GitHub\airflow\data\empresa.csv"
save_path = r"C:\Users\macie\Documents\GitHub\airflow\data\empresa1.csv"


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
        df = pd.read_csv(caminho, sep=";")
        logging.info(f"Arquivo lido de {caminho}")
        # Converte o DataFrame para JSON para passar via XCom
        return df.to_json(orient='split')

    @task(task_id="save")
    def save(data):
        # Converte o JSON de volta para DataFrame
        df = pd.read_json(data, orient='split')
        # Salva o DataFrame em outro arquivo CSV
        df.to_csv(save_path, index=False)
        logging.info(f"Arquivo salvo em {save_path}")

    # Define a dependência entre as tasks
    dados = load()
    save(dados)


# Instancia a DAG
dag_instance = my_dag()
