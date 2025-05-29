import os
import psycopg2
import json

from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType, DoubleType, IntegerType

with open("/app/spark_config.json", 'r') as f:
    spark_config = json.load(f)

conf = SparkConf()
conf.setAll(spark_config.items())

#SPARK SESSION
spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

#KAFKA BROKER
bootstrap_server = "kafka:9092"

#RAW KAFKA TOPIC
topic = "raw_weather_data"

#TRANSFORM DATA
df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_server) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

schema = StructType([
    StructField('coord', StructType([
        StructField('lon',DoubleType(),True),
        StructField('lat',DoubleType(),True)]), True),
    StructField('weather', ArrayType(
        StructType([
            StructField('id',IntegerType(),True),  
            StructField('main',StringType(),True),  
            StructField('description',StringType(),True),  
            StructField('icon',StringType(),True)])), True),  
    StructField('base',StringType(),True),  
    StructField('main', StructType([
        StructField('temp',DoubleType(),True),  
        StructField('feels_like',DoubleType(),True),  
        StructField('temp_min',DoubleType(),True),  
        StructField('temp_max',DoubleType(),True),  
        StructField('pressure',IntegerType(),True),  
        StructField('humidity',IntegerType(),True),  
        StructField('sea_level',IntegerType(),True),  
        StructField('grnd_level',IntegerType(),True)]), True),  
    StructField('visibility',IntegerType(),True),  
    StructField('wind',StructType([
        StructField('speed',DoubleType(),True),  
        StructField('deg',IntegerType(),True), 
        StructField('gust',DoubleType(),True)]), True), 
    StructField('rain',StructType([
        StructField('1h',DoubleType(),True)]), True),
    StructField('snow',StructType([
        StructField('1h',DoubleType(),True)]), True),
    StructField('clouds',StructType([
        StructField('all',IntegerType(),True)]), True),  
    StructField('dt',TimestampType(),True),  
    StructField('sys',StructType([
        StructField('type',IntegerType(),True),  
        StructField('id',IntegerType(),True),  
        StructField('country',StringType(),True),  
        StructField('sunrise',TimestampType(),True),  
        StructField('sunset',TimestampType(),True)]),True),  
    StructField('timezone',IntegerType(),True),  
    StructField('id',IntegerType(),True),  
    StructField('name',StringType(),True),  
    StructField('cod',IntegerType(),True)])

df = df.selectExpr("CAST(value AS STRING)").select("value")
df = df.withColumn("data", from_json(col("value"), schema)).select("data.*")

df_weather = df.select(
    col("id").alias("city_id"),
    col("coord.lat").alias("latitude"),
    col("coord.lon").alias("longitude"),
    col("name").alias("city_name"),
    col("sys.country").alias("country_code"),
    col("dt").alias("datetime"),
    col("timezone").alias("timezone"),
    col("weather").getItem(0).getField("main").alias("weather_main"),
    col("weather").getItem(0).getField("description").alias("weather_description"),
    coalesce(col("main.temp"), lit(0)).alias("temperature"),
    coalesce(col("main.feels_like"), lit(0)).alias("feels_like"),
    coalesce(col("main.temp_min"), lit(0)).alias("temp_min"),
    coalesce(col("main.temp_max"), lit(0)).alias("temp_max"),
    coalesce(col("visibility")).alias("visibility"),
    coalesce(col("main.pressure"), lit(0)).alias("pressure"),
    coalesce(col("main.humidity"), lit(0)).alias("humidity"),
    coalesce(col("main.sea_level"), lit(0)).alias("sea_level"),
    coalesce(col("main.grnd_level"), lit(0)).alias("grnd_level"),
    coalesce(col("wind.speed"), lit(0.0)).alias("wind_speed"),
    coalesce(col("wind.deg"), lit(0)).alias("wind_deg"),
    coalesce(col("wind.gust"), lit(0)).alias("wind_gust"),
    coalesce(col("rain.1h"), lit(0)).alias("rain_mm_h"),
    coalesce(col("snow.1h"), lit(0)).alias("snow_mm_h"),
    col("clouds.all").alias("cloudiness"),
    col("sys.sunrise").alias("sunrise"),
    col("sys.sunset").alias("sunset")
)

#CLEANED DATA KAFKA TOPIC
df_kafka = df_weather.selectExpr("CAST (city_id AS STRING) AS key", "to_json(struct(*)) AS value")
cleaned_topic = "clean_weather_data"

#SEND TO KAFKA
kafka_query = df_kafka \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_server) \
    .option("topic", cleaned_topic) \
    .option("checkpointLocation", "/opt/spark-checkpoints") \
    .option("failOnDataLoss", "false")  \
    .start()

#POSTGRESQL CONNECTION
load_dotenv()

db_name = os.environ.get("POSTGRES_DB")
db_host = os.environ.get("POSTGRES_HOST")
db_port = os.environ.get("POSTGRES_PORT")
db_user = os.environ.get("POSTGRES_USER")
db_password = os.environ.get("POSTGRES_PASSWORD")

postgres_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
db_properties = {
    "user": db_user,
    "password": db_password,
    "driver":"org.postgresql.Driver"
}

#SQL TABLE
QUERY_SCHEMA = "CREATE SCHEMA IF NOT EXISTS weather"

QUERY_TABLE = """
    CREATE TABLE IF NOT EXISTS weather.weather_data(
               id SERIAL PRIMARY KEY,
               city_id INTEGER,
               city_name VARCHAR(100),
               country_code CHAR(2),
               longitude FLOAT,
               latitude FLOAT,
               datetime TIMESTAMP,
               timezone INTEGER,
               weather_main VARCHAR(50),
               weather_description VARCHAR(255),
               temperature FLOAT DEFAULT 0.0,
               feels_like FLOAT DEFAULT 0.0,
               temp_min FLOAT DEFAULT 0.0,
               temp_max FLOAT DEFAULT 0.0,
               visibility INTEGER DEFAULT 0,
               pressure INTEGER DEFAULT 0,
               humidity INTEGER DEFAULT 0,
               sea_level INTEGER DEFAULT 0,
               grnd_level INTEGER DEFAULT 0,
               wind_speed FLOAT DEFAULT 0.0,
               wind_deg INTEGER DEFAULT 0,
               wind_gust FLOAT DEFAULT 0.0,
               rain_mm_h FLOAT DEFAULT 0.0,
               snow_mm_h FLOAT DEFAULT 0.0,
               cloudiness INTEGER DEFAULT 0,
               sunrise TIMESTAMP,
               sunset TIMESTAMP,
               ingestion_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
          );
"""

#INSERT DATA TO POSTGRESQL TEMP TABLE
def insert_to_postgres(df, batch_id):
    try:
        with psycopg2.connect(
            dbname = db_name,
            host = db_host,
            port = db_port,
            user = db_user,
            password = db_password
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(QUERY_SCHEMA)
                cursor.execute(QUERY_TABLE)

                conn.commit()
        
        df.write.jdbc(url=postgres_url, table="weather.weather_data", mode="append", properties=db_properties)

        print("Insert completed successfully")
    except Exception as e:
        print(f"Error during PostgreSQL operation: {e}")


sql_query = df_weather.writeStream \
        .foreachBatch(insert_to_postgres) \
        .option("checkpointLocation", "/opt/spark-checkpoints/postgres") \
        .start()

kafka_query.awaitTermination()
sql_query.awaitTermination()


#docker exec -it spark-master python /scripts/consumer.py