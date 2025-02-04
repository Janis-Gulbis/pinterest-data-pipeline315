#%%
import json
import requests
from time import sleep
import random
from multiprocessing import Process
import yaml
import sqlalchemy
from sqlalchemy import text
from datetime import datetime

random.seed(100)

API_URL = "https://34xfeixteb.execute-api.us-east-1.amazonaws.com/dev/topics"
KAFKA_TOPICS = {
    "geo": "9105411ea84a.geo",
    "pin": "9105411ea84a.pin",
    "user": "9105411ea84a.user"
}

def load_db_credentials(file_path="db_creds.yaml"):
    with open(file_path, "r") as file:
        return yaml.safe_load(file)

class AWSDBConnector:
    def __init__(self, creds_file="db_creds.yaml"):
        creds = load_db_credentials(creds_file)
        self.HOST = creds["HOST"]
        self.USER = creds["USER"]
        self.PASSWORD = creds["PASSWORD"]
        self.DATABASE = creds["DATABASE"]
        self.PORT = creds["PORT"]

    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine


def send_to_kafka(topic, data):
    endpoint_url = f"{API_URL}/{KAFKA_TOPICS[topic]}"
    
    # Convert datetime objects to strings in the data
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.strftime("%Y-%m-%d %H:%M:%S") 

    # Wrap the data in the required Kafka payload format
    payload = {
        "records": [
            {
                "value": json.dumps(data)
            }
        ]
    }
    
    headers = {
        "Content-Type": "application/vnd.kafka.json.v2+json"
    }
    
    # Send the POST request
    response = requests.post(endpoint_url, json=payload, headers=headers)
    if response.status_code == 200:
        print(f"Successfully sent to {topic}")
    else:
        print(f"Failed to send to {topic}: {response.status_code}, {response.text}")



def test_api():
    """Test if API is responding."""
    response = requests.get(API_URL)
    print(f"API Test Response: {response.status_code} - {response.text}")

def stream_data():
    """Stream data from DB and send to Kafka."""
    new_connector = AWSDBConnector()
    engine = new_connector.create_db_connector()
    
    row_index = 0
    sent_counts = {"geo": 0, "pin": 0, "user": 0}
    limit = 500
    
    while all(count < limit for count in sent_counts.values()):
        sleep(random.uniform(0, 2))
        with engine.connect() as connection:
            for table, topic_key in [("pinterest_data", "pin"), ("geolocation_data", "geo"), ("user_data", "user")]:
                if sent_counts[topic_key] < limit:
                    query = text(f"SELECT * FROM {table} LIMIT {row_index}, 1")
                    result = connection.execute(query)
                    row = [dict(row._mapping) for row in result]
                    if row:
                        send_to_kafka(topic_key, row[0])
                        sent_counts[topic_key] += 1
        row_index += 1

if __name__ == "__main__":
    test_api()  # Test if the API responds
    stream_data()  # Stream if no issues


#%%