<!-- README file in MD for the Multination Retail Data Centre repository-->
<a name="readme-top"></a>

# Pinterest Data Pipeline.

<!-- SHIELDS FOR REPO -->
<p align="left">
    <a>
        <img src="https://img.shields.io/badge/language-Python-red"
            alt="Language"></a>
   
</p>

<!-- ABOUT THE PROJECT -->
## About the Project

### Summary
Pinterest is a visual discovery platform that helps users find ideas and inspiration. With billions of data points generated daily through views, follows, and uploads, Pinterest continuously analyzes user interactions to deliver more relevant content.

This project builds a scalable, end-to-end data pipeline leveraging AWS cloud services and Databricks to process and analyze both real-time and historical Pinterest-emulated data. The pipeline is designed to handle streaming data ingestion, transformation, and analysis, enabling deeper insights into user engagement patterns.


- `Key platforms and technologies`: AWS (AIM, EC2, S3, API Gateway, Kinesis, MWAA), Kafka, Apache (Spark, Airflow).
- `Languages`: Python, SQL

<p align="right">(<a href="#readme-top">back to top</a>)</p>


### Project Structure
 - Data Extraction: The `data_extraction.py` file contains methods for loading data from various sources into Pandas DataFrames.
- Data Cleaning: In `data_cleaning.py`, I implement the DataCleaning class to clean and preprocess the tables imported via `data_extraction.py`.
- Database Upload: The `database_utils.py` file includes the DatabaseConnector class, which creates a database engine using credentials from a .yml file.
- Main Script: The `main.py` file integrates all functionality, enabling seamless data upload to the local PostgreSQL database.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- DATABASE SCHEMA -->
