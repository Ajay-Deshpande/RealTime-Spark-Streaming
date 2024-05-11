# RealTime-Spark-Streaming

This application demonstrates a real-time data processing pipeline using Apache Kafka, Apache Spark Structured Streaming, and MySQL. It is designed to process streaming data representing e-commerce transactions, perform aggregations, and store the results in a MySQL database for further analysis.

## Overview

The application comprises two main components:
0. **Kafka Producer (`kafka_producer.ipynb`):**
   - Creates a dummy dataset randomly chosing between various hardcoded values and appends the data to a kafka topic.

1. **Kafka Consumer (`kafka_consumer.ipynb`):**
   - This component consumes streaming data from Kafka topics. 
   - It listens to Kafka topics where e-commerce transaction data is being produced.
   - Upon receiving messages, it processes and forwards them to the Spark Structured Streaming component for further processing.

2. **Spark Structured Streaming (`spark_streaming.ipynb`):**
   - This component processes the streaming data received from Kafka.
   - It performs aggregations on the data, such as calculating total sales by card type and country.
   - The aggregated results are then stored in MySQL tables for analysis.

## Workflow

1. **Data Ingestion:**
   - E-commerce transaction data is produced to Kafka topics in JSON format. (This is dummy data created randomly)
   - Each message represents a transaction and includes details like the order ID, product name, card type, order amount, timestamp, country, city, and e-commerce website name.

2. **Kafka Consumer:**
   - The `kafka_consumer.ipynb` notebook consumes messages from Kafka topics.
   - It processes the messages and forwards them to Spark Structured Streaming for further processing.

3. **Spark Structured Streaming:**
   - The `spark_streaming.ipynb` notebook receives streaming data from the Kafka consumer.
   - It uses Spark Structured Streaming to process the data in real-time.
   - Aggregations such as total sales by card type and country are calculated.
   - The aggregated results are saved to MySQL tables for analysis.

## Prerequisites

Before running the application, ensure you have the following installed:

- Apache Kafka
- Apache Spark
- MySQL database server
- Jupyter Notebook with necessary Python dependencies

## Usage

1. **Setup:**
   - Clone the repository to your local machine.
   - Install the required Python dependencies using `pip install -r kafka-python pyspark findspark`.
   - Configure the `streaming_app.conf` file with Kafka, Spark, and MySQL connection details.

2. **Data Ingestion:**
   - Start producing e-commerce transaction data to Kafka topics.

3. **Kafka Consumer:**
   - Open and execute the `kafka_consumer.ipynb` notebook to start consuming data from Kafka.

4. **Spark Structured Streaming:**
   - Open and execute the `spark_streaming.ipynb` notebook to process streaming data, perform aggregations, and store results in MySQL.

5. **Analysis:**
   - Analyze the aggregated data stored in MySQL for insights into e-commerce sales trends.

## Configuration

The `streaming_app.conf` file contains configuration parameters for Kafka, Spark, MySQL, and other settings used by the application. Update this file with your environment-specific details.
