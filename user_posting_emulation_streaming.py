#%%
import requests
import json
import random
import yaml
import sqlalchemy
from sqlalchemy import text
from time import sleep
from datetime import datetime

random.seed(100)

def load_db_credentials(filepath='db_creds.yaml'):
    """Load database credentials from a YAML file."""
    with open(filepath, 'r') as file:
        creds = yaml.safe_load(file)
    return creds

class AWSDBConnector:
    def __init__(self, creds):
        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = creds['DATABASE']
        self.PORT = creds['PORT']
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(
            f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        )
        return engine

def convert_value(value):
    """Convert datetime objects to ISO format strings."""
    if isinstance(value, datetime):
        return value.isoformat()
    return value

def send_record(payload, table_name):
    

    API_ENDPOINT = "https://34xfeixteb.execute-api.us-east-1.amazonaws.com/dev/streams/Kinesis-Prod-Stream/record"
    
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.put(API_ENDPOINT, data=payload, headers=headers)
        
        if response.status_code in (200, 201):
            print(f"Successfully sent record from {table_name} to API.")
        else:
            print(f"Failed to send record from {table_name} to API. Status code: {response.status_code}")
        
        try:
            print("Response JSON:", response.json())
        except Exception as parse_err:
            print("No valid JSON in response or error parsing JSON.", parse_err)
            
    except requests.exceptions.RequestException as e:
        print(f"Failed to send record from {table_name} to API. Error: {e}")

def run_infinite_post_data_loop():
    creds = load_db_credentials()
    db_connector = AWSDBConnector(creds)
    
    # Mapping of table names to partition keys
    tables = {
        "pinterest_data": "9105411ea84a_pin",
        "geolocation_data": "9105411ea84a_geo",
        "user_data": "9105411ea84a_user"
    }
    
    # Expected column structures for each table
    table_columns = {
        "pinterest_data": [
            "index", "unique_id", "title", "description", "poster_name", "follower_count",
            "tag_list", "is_image_or_video", "img_src", "downloaded", "save_location", "category"
        ],
        "geolocation_data": [
            "ind", "timestamp", "latitude", "longitude", "country"
        ],
        "user_data": [
            "ind", "first_name", "last_name", "age", "date_joined"
        ]
    }
    
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        
        try:
            engine = db_connector.create_db_connector()
            with engine.connect() as connection:
                for table, partition_key in tables.items():
                    query = text(f"SELECT * FROM {table} LIMIT {random_row}, 1")
                    result = connection.execute(query)
                    
                    row_data = None
                    for row in result:
                        row_data = dict(row._mapping)
                    
                    if row_data is None:
                        print(f"No data found for table {table} at offset {random_row}.")
                        continue
                    
                    # Build the data payload using only the specified columns
                    columns = table_columns[table]
                    data_payload = {col: convert_value(row_data.get(col)) for col in columns}
                    
                
                    payload_dict = {
                        "StreamName": "kinesis-prod-stream",  
                        "Data": data_payload,
                        "PartitionKey": partition_key
                    }
                    
                    payload = json.dumps(payload_dict)
                    send_record(payload, table)
                    print(f"Connection succeeded for table {table}.")
        
        except Exception as e:
            print("Connection fails.", e)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')






# %%
