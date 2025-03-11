import requests
import os
import json
import time
import pandas as pd

from dotenv import load_dotenv
from confluent_kafka import Producer
from functions import delivery_callback

load_dotenv()

api_key = os.environ.get("API-KEY")
url = 'https://api.currentsapi.services/v1/latest-news'

params = {
    "apiKey": api_key
}

#KAFKA PRODUCER
conf={
    "bootstrap.servers":"kafka:9092"
}
producer = Producer(conf)

try:
    while True:
        try:
            res = requests.get(url, params=params)
            res.raise_for_status()
            latest_news = res.json()

            producer.produce(
                "raw-latest-news",
                json.dumps(latest_news).encode("utf-8"),
                callback=delivery_callback
            )
            producer.flush()

            time.sleep(60)
        except requests.RequestException as e:
            print(f"Error in API request: {e}")
            time.sleep(120)

except KeyboardInterrupt:
    print("Closing producer.")
finally:
    producer.flush()