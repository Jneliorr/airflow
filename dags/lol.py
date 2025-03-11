import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup


API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"


@dag(
    dag_id="lol",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False
)
def main():
    with TaskGroup("Transformers") as transformers:

        @task(task_id="extract", retries=2)
        def extract_bitcoin():
            return requests.get(API).json()["bitcoin"]

        @task(task_id="trasnform")
        def process_bitcoin(response):
            return {"usd": response["usd"], "change": response["usd_24h_change"]}

        processed_data = process_bitcoin(extract_bitcoin())

    with TaskGroup("store") as stores:
        @task(task_id="store")
        def store_bitcoin(data):
            logging.info(
                f"Biticon price: {data['usd']}, change: {data['change']}")

        store_bitcoin(processed_data)


main()


# @dag(
#     dag_id="criptob",
#     schedule="@daily",
#     start_date=datetime(2023, 1, 1),
#     catchup=False
# )
# def main():

#     @task(task_id="extract", retries=2)
#     def extract_bitcoin():
#         return requests.get(API).json()["bitcoin"]

#     @task(task_id="trasnform")
#     def process_bitcoin(response):
#         return {"usd": response["usd"], "change": response["usd_24h_change"]}

#     @task(task_id="store")
#     def store_bitcoin(data):
#         logging.info(f"Biticon price: {data['usd']}, change: {data['change']}")

#     store_bitcoin(process_bitcoin(extract_bitcoin()))


# main()
