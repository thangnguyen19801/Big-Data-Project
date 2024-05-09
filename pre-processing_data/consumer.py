from kafka import KafkaConsumer
import json

import pymongo

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")  # mongodb is the service name from docker-compose.yml
db = client["E-Commerce"]  # Replace "mydatabase" with your desired database name

# Example: Insert a document into a collection
collection = db["Tiki"]

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer(
        'tiki',
        bootstrap_servers='localhost:9092',
        max_poll_records = 100,
        value_deserializer=lambda m: json.loads(m.decode('ascii')),
        key_deserializer=lambda m: json.loads(m.decode('ascii')),
        auto_offset_reset='earliest'#,'smallest'
    )

import boto3
from botocore.exceptions import NoCredentialsError

def upload_variable_to_s3(variable_content, bucket_name, s3_object_key):
    # Set up AWS credentials and S3 client
    aws_access_key_id = 'AKIA2X572HTNVBUK3FSA'
    aws_secret_access_key = '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'
    
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    try:
        # Convert the variable content to bytes
        variable_content_bytes = json.dumps(variable_content)

        # Upload the variable content as an object to S3
        s3.put_object(Body=variable_content_bytes, Bucket=bucket_name, Key=s3_object_key)
        print(f"Variable uploaded successfully to '{bucket_name}/{s3_object_key}'")

    except NoCredentialsError:
        print("Credentials not available or incorrect.")

# Example usage
bucket_name = 'tiki-big-data-project'
s3_object_key = 'uploaded-variable.txt'

if __name__ == "__main__":
    i = 1
    for message in consumer:
        message_data = message.value
        collection.insert_one(message_data)
        i += 1