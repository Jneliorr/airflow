import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include%20market%20cap=true&include%2024hr_vol=true&include%2024hr%20change=true&include%20last%20updated_at=true"


@dag(
    dag_id="bitcoin",
    start_date=datetime(2023, 8, 1),
    schedule="@daily",
    catchup=False,
)


def main():

    @task(task_id="extract", retries=2)
    def extract_bitcoin():
        return requests.get(API).json()["bitcoin"]

    @task(task_id="transform")
    def process_bitcoin(response):
        return {"usd": response["usd"]}

    @task(task_id="store")
    def store_bitcoin(data):
        logging.info(f"Bitcoin price: {data['usd']}")

    store_bitcoin(process_bitcoin(extract_bitcoin()))


main()
