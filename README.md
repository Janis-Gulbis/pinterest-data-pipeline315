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

### Introduction
Pinterest is a visual discovery platform that helps users find ideas and inspiration. With billions of data points generated daily through views, follows, and uploads, Pinterest continuously analyzes user interactions to deliver more relevant content.

This project builds a scalable, end-to-end data pipeline leveraging AWS cloud services and Databricks to process and analyze real-time and historical Pinterest-emulated data. The pipeline is designed to handle batch and streaming data ingestion, transformation, and analysis, enabling deeper insights into user engagement patterns.


- `Key platforms and technologies`: AWS (AIM, EC2, S3, API Gateway, Kinesis, MWAA), Kafka, Apache (Spark, Airflow).
- `Languages`: Python, SQL

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Project Aim
This project applies AWS, Kafka, Spark, Airflow, and Databricks to build an end-to-end data pipeline. It demonstrates key concepts in real-time data streaming, ETL processing, and workflow orchestration using Python, PySpark, and SQL.

As part of the implementation:

- AWS: Configured a Kafka client and created Kafka topics on an EC2 instance, set up an API Gateway, and worked within Managed Workflows for Apache Airflow (MWAA) and Kinesis Data Streams.
- Databricks: Used the platform for data extraction, transformation, and loading (ETL) to process and analyze data efficiently.
- Airflow: Designed and deployed a DAG to schedule a Databricks Notebook, ensuring automation and monitoring of tasks.
  
This project showcases hands-on experience with cloud-based data engineering tools, reinforcing best practices in scalability, automation, and real-time data processing.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Files & File Content
- `user_posting_emulation.py` The script emulates the stream of POST requests by users on Pinterest. Data is formatted and sent via API Invoke URL to Kafka topics in batches of 500 records per execution.
- `user_posting_emulation_streaming.py` This script emulates a continuous stream of POST requests by users on Pinterest. Sends requests to the API, adding one record at a time to the stream, and utilizes PartitionKey to identify what table record belongs to.
- `9105411ea84a_dag.py` An Airflow DAG that triggers a Databricks Notebook daily.
- `delta_table_setup.ipynb` Databricks Notebook that sets up Delta tables for the clean data.
- `process_batch_data.ipynb` Obtains data from the AWS S3 bucket, cleans it, and writes it to the Delta table. This is the Databricks notebook that is run by `9105411ea84a_dag.py` daily.
- `process_stream_data.ipynb` Obtains the stream data from AWS Kinesis, cleans it, and writes it to the Delta table.
- `query_batch_data.ipynb` Contains SQL queries performed on cleaned batch data.
- `arch.png` An image of the diagram of the architecture.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## High-level Architecture
<!-- DATABASE SCHEMA -->
<img src="https://github.com/Janis-Gulbis/pinterest-data-pipeline315/blob/main/arch.png#" alt="diagram of the architecture" width="1000">

- `RDS` Stores Pinterest data. Source of data for the emulation script. 
- `Data emulation` Python script feeds API Gateway with data. 
- `API Gateway` provides an API for data transfer. Batch data will be sent to Kafka and streamed to Kinesis DS.
- `Kafka` Ingests and processes data.
- `Kinesis DS` Manages streams of incoming real-time data from the API.
- `S3` S3 buckets store Kafka topic data and a DAG files.
- `MWAA` MWAA environment schedules Airflow workflows for Databricks environments.
- `Databricks` Platform for processing and transforming batch and stream data.
- `Spark` is used to clean and analyze data within Databricks.


<p align="right">(<a href="#readme-top">back to top</a>)</p>


## Project Workflow

### Initial exploration

I modified the supplied `user_posting_emulation.py` to hide the AWS RDS log-in details and wrote the output to a file. I forced the JSON to output information in strings only, as datetime was also used in two of the three tables.

These are:
- `pinterest_data` - data about user posts on Pinterest

```python
{
  "index": 7528,
  "unique_id": "fbe53c66-3442-4773-b19e-d3ec6f54dddf",
  "title": "No Title Data Available",
  "description": "No description available Story format",
  "poster_name": "User Info Error",
  "follower_count": "User Info Error",
  "tag_list": "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",
  "is_image_or_video": "multi-video(story page format)",
  "image_src": "Image src error.",
  "downloaded": 0,
  "save_location": "Local save in /data/mens-fashion",
  "category": "mens-fashion",
}
```
- `geolocation_data` - data about the geographical location of the post

```python
{
  "ind": 7528,
  "timestamp": "2020-08-28 03:52:47",
  "latitude": -89.9787,
  "longitude": -173.293,
  "country": "Albania",
}
```
- `user_data` - data about user who posted

```python
{
  "ind": 7528,
  "first_name": "Abigail",
  "last_name": "Ali",
  "age": 20,
  "date_joined": "2015-10-24 11:23:51",
}
```
* Note the inconsistencies with the index/ind column header

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### AWS

An AWS IAM role had been created for me. I used the supplied username and password to log into AWS. A pem key file had been created to allow us to connect to an existing EC2 instance.

### Kafka

The EC2 instance had Kafa pre-installed on it. The next step was to create three Kafka topics - one for each data table:

```python
- 9105411ea84a.geo
- 9105411ea84a.pin
- 9105411ea84a.user
```
### API Gateway - Batch Processing

Next up is configuring an API within the API Gateway console. I created a /{proxy+} resource and added an HTTP method. PublicDNS from our ec2 instance was the Endpoint URL. I then deployed the API and stored the Invoke URL for later use in my script. The endpoint will be the main point of communication with the Kafka rest proxy. `http://KafkaClientEC2InstancePublicDNS:8082/{proxy}`

#### Sending Data to the API using Python
I modified the supplied user_posting_emulation.py script to send data to the newly created API. The basis of communicating with the REST proxy is the following:

```python
invoke_url = "https://YourAPIInvokeURL/YourDeploymentStage/topics/YourTopicName"
#To send JSON messages, you need to follow this structure
payload = json.dumps({
    "records": [
        {
        #Data should be send as pairs of column_name:value, with different columns separated by commas
        "value": {"index": df["index"], "name": df["name"], "age": df["age"], "role": df["role"]}
        }
    ]
})

headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
response = requests.request("POST", invoke_url, headers=headers, data=payload)
```
I encountered difficulties converting the payload data to the required standard, namely converting `datetime` objects, which JSON does not handle natively. Eventually, I dealt with it by including these parameters within my `send_to_kafka` function

```python
# Convert datetime objects to strings in the data
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.strftime("%Y-%m-%d %H:%M:%S")
```
Eventually, data was successfully streamed from the DB and sent to Kafka (with an imposed limit of 500 rows per table), which was then stored in an S3 bucket

```Python
topics/<your_UserId>.pin/partition=0/
topics/<your_UserId>.geo/partition=0/
topics/<your_UserId>.user/partition=0/
```

### Batch Processing Data with Databricks

#### Spark on Databricks

Data was red from S3 into Databricks, and using Spark SQL, I cleaned the data of each of the three tables I had loaded: `pin`, `geo`, and `user`. Cleaning involved ensuring columns had correct data types and eliminating invalid values. Also, several columns were merged - namely, the `longitude` and `latitude` columns became the `coordinates` column (`geo` table), and the `first_name` and `last_name` columns were combined to form a new column named `user_name`. 

A series of queries were performed to answer a meriad of questions. The full list of questions and the code can be found [here](https://github.com/Janis-Gulbis/pinterest-data-pipeline315/blob/main/Databricks/query_batch_data.ipynb)

<p align="right">(<a href="#readme-top">back to top</a>)</p>







