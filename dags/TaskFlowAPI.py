import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task


API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include%20market%20cap=true&include%2024hr_vol=true&include%2024hr%20change=true&include%20last%20updated_at=true"


@dag(
    dag_id="TaskFlowAPI",
    schedule="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False
)
def main():
    @task(task_id="extracao", retries=2)
    def extracao_biticoin():
        return requests.get(API).json()["bitcoin"]

    extracao_biticoin()


main()