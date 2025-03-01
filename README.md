<!-- README file in MD for the Multination Retail Data Centre repository-->
<a name="readme-top"></a>

# Pinterest Data Pipeline.

<!-- SHIELDS FOR REPO -->
<p align="left">
  <a>
    <img src="https://img.shields.io/badge/language-Python-red" alt="Language">
  </a>
  <a>
    <img src="https://img.shields.io/badge/language-SQL-blue" alt="Language">
  </a>
</p>


<!-- ABOUT THE PROJECT -->
## About the Project

### Summary
Pinterest is a visual discovery platform that helps users find ideas and inspiration. With billions of data points generated daily through views, follows, and uploads, Pinterest continuously analyzes user interactions to deliver more relevant content.

This project builds a scalable, end-to-end data pipeline leveraging AWS cloud services and Databricks to process and analyze real-time and historical Pinterest-emulated data. The pipeline is designed to handle batch and streaming data ingestion, transformation, and analysis, enabling deeper insights into user engagement patterns.


- `Key platforms and technologies`: AWS (AIM, EC2, S3, API Gateway, Kinesis, MWAA), Kafka, Apache (Spark, Airflow).
- `Languages`: Python, SQL

<p align="right">(<a href="#readme-top">back to top</a>)</p>


### Files & File Content
- `user_posting_emulation.py` The script emulates the stream of POST requests by users on Pinterest. Data is formatted and sent via API Invoke URL to Kafka topics in batches of 500 records per execution.
- `user_posting_emulation_streaming.py` This script emulates a continuous stream of POST requests by users on Pinterest. Sends requests to the API adding one record at a time to the stream and utilizes PartitionKey to identify what table record belongs to.
- `9105411ea84a_dag.py` An Airflow DAG that triggers a Databricks Notebook daily.
- `delta_table_setup.ipynb` Databricks Notebook that sets up Delta tables for the clean data.
- `process_batch_data.ipynb` Obtains data from the AWS S3 bucket, cleans it and writes it to the Delta table. This is the Databricks notebook that is run by `9105411ea84a_dag.py` daily.
- `process_stream_data.ipynb` Obtains the stream data from AWS Kinesis, cleans it, and writes it to the Delta table.
- `query_batch_data.ipynb` Contains SQL queries performed on cleaned batch data.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- DATABASE SCHEMA -->
