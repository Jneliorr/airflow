import os
import zipfile
import logging
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task


caminho = r"C:\Users\macie\Documents\GitHub\airflow\data\empresa.csv"


@dag(
    dag_id="teste1",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def main():

    @task(task_id="load", retries=2)
    def load(caminho):
        logging.info(f"Carregando {caminho}")
        return pd.read_csv(caminho)

    load(caminho)


main()
