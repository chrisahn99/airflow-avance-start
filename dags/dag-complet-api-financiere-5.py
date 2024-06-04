import logging
from datetime import datetime, timedelta
from typing import Dict

import numpy as np
import pandas as pd
import random
import requests
from airflow.decorators import dag, task
from airflow.models.taskinstance import TaskInstance

API = "https://api.coingecko.com/api/v3/coins/bitcoin/history?date={}localization=fr"

N = 100

@dag(dag_id="dag-complet-api-financiere-5", schedule="@once", start_date=datetime(2023, 3, 25), catchup=False)
def taskflow():
    @task(task_id="extract", retries=2)
    def extract_bitcoin_price(task_instance) -> Dict[str, float]:
        execution_date = task_instance.execution_date
        dates = []
        prices = []
        formatted_date = execution_date.strftime("%d-%m-%Y")
        response = requests.get(API.format(formatted_date)).json()["market_data"]
        initial_price = response["current_price"]["usd"]

        for n_day_before in range(0, N):
            date = execution_date - timedelta(days=n_day_before)
            formatted_date = date.strftime("%d-%m-%Y")
            random_variation = round(random.uniform(-1000, 1000), 2)
            dates.append(date)
            prices.append(initial_price + random_variation)
        
        return {"dates": dates, "prices": prices}

    @task(multiple_outputs=True)
    def process_data(extraction: Dict[str, float]) -> Dict[str, float]:
        logging.info(extraction)
        prices = extraction["prices"][::-1]
        print(prices)
        deltas = np.diff(prices)
        print(deltas)
        ups = [delta for delta in deltas if delta > 0]
        downs = [-delta for delta in deltas if delta < 0]
        up_avg = sum(ups) / len(ups)
        print(up_avg)
        down_avg = sum(downs) / len(downs)
        print(down_avg)
        rsi = 100 - 100/(1 + up_avg/down_avg)

        print(len(extraction["dates"]))
        print(len(prices))

        return {
            "dates": extraction["dates"],
            "prices": prices,
            "rsi": rsi,
        }

    @task
    def create_dataframe(data: Dict[str, float]) -> pd.DataFrame:
        df = pd.DataFrame({
            "date": data["dates"],
            "price": data["prices"],
            "rsi": [data["rsi"]] * N,
        })
        return df

    @task
    def calculate_weekly_average(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        df["date"] = pd.to_datetime(df["date"])
        df["week"] = df["date"].dt.strftime("%Y-%W")
        weekly_average = df.groupby("week")["price"].mean().reset_index()
        return {
            "daily_data": df,
            "weekly_average": weekly_average,
        }

    @task
    def store_data(data: Dict[str, pd.DataFrame]):
        logging.info(
            f"Store: Last price {data['daily_data']['price'].iloc[-1]} with rsi {data['daily_data']['rsi'].iloc[-1]}"
        )
        logging.info(
            f"Store: Weekly averages {data['weekly_average'].head()}"
        )

    store_data(
        calculate_weekly_average(
            create_dataframe(
                process_data(
                    extract_bitcoin_price()
    ))))


taskflow()
