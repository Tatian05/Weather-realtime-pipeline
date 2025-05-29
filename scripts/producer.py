import requests
import os
import json
import time
import pandas as pd

from dotenv import load_dotenv
from confluent_kafka import Producer
from functions import delivery_callback

load_dotenv()

#KAFKA PRODUCER
conf={
    "bootstrap.servers":"kafka:9092"
}
producer = Producer(conf)


CITY = "Buenos Aires"
API_KEY = os.getenv("API_KEY")

GEOCODING_URL = f"https://api.openweathermap.org/geo/1.0/direct?q={CITY}&appid={API_KEY}"

def get_coords():
     try:
          res = requests.get(GEOCODING_URL)
          json_data = res.json()

          if isinstance(json_data, list) and len(json_data) > 0:
               data = json_data[0]
               lat = data.get("lat")
               lon = data.get("lon")
               return lat, lon
          else:
               raise ValueError("Respuesta inesperada de la API: {}".format(json_data))
     except requests.RequestException as e:
          print(f"Error in API request: {e}")


lat, lon = get_coords()
WEATHER_URL = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"


try:
    while True:
        try:
            res = requests.get(WEATHER_URL)
            res.raise_for_status()
            latest_news = res.json()

            producer.produce(
                "raw_weather_data",
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

#docker exec -it spark-master python /app/producer.py