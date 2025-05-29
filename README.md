# Real-Time News Pipeline
"Real-Time Weather Pipeline" is a portfolio Data Engineering project designed to process real-time weather data. It extracts information from the OpenWeatherMap API, sends raw data to Kafka, processes it with Spark Streaming, and stores the transformed data in PostgreSQL. This pipeline enables real-time data ingestion and transformation for analytical use cases.

---

## Project Architecture:
1. **Data Extraction**:
    - Weather metrics are collected from the OpenWeatherMap API using a Kafka producer.
2. **Kafka Streaming**:
    - Raw data is sent to the Kafka topic 'raw_weather_data'.
3. **Processing with Apache Spark Streaming**:
    - Data is consumed from Kafka, cleaned, and transformed in real-time.
    - Duplicated removal, text normalization, and metadata enrichment are performed.
4. **Kafka Output**:
    - Processed data is sent to a second Kafka topic 'clean_weather_data'.
5. **Load to PostgreSQL**:
    - Transformed data is stored in PostgreSQL for further analysis.

---

## Conclusion:
This project showcases my ability to work with real-time data streaming technologies like Kafka and Spark Streaming. By processing live news data, I gained hands-on experience in building scalable, event-driven data pipelines. This project highlights my skills in integrating different tools to enable real-time data processing and analytics.

---

## Contributions:
If you have any suggestions or improvements, feel free to open an issue or submit a pull request. Your feedback is always welcome!

---

## Contact:
- **Name**: Sebastián Esnaola
- **LinkedIn**: [www.linkedin.com/in/sebastian-esnaola]
- **Email:** isp2014asje@gmail.com