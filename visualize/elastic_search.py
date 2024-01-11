import boto3
from elasticsearch import Elasticsearch, helpers
import json
import os
import pandas as pd

# AWS S3 settings
aws_access_key_id = 'AKIA2X572HTNVBUK3FSA'
aws_secret_access_key = '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'
s3_bucket_name = 'tiki-big-data-project'

# Elasticsearch settings
elasticsearch_url = 'http://localhost:9200'

# Connect to Elasticsearch
es = Elasticsearch([elasticsearch_url])

# Create an S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# List objects in the S3 bucket
response = s3.list_objects_v2(Bucket=s3_bucket_name)
# Index each JSON file into Elasticsearch
phone_and_tablet = []
electric_accessory = []
camera = []
man_fashion = []
jewelry = []
i = 1
for obj in response.get('Contents', []):
    file_key = obj['Key']
    if file_key.endswith('.json') & file_key.startswith("phone_and_tablet"):
        # Download JSON file from S3
        response = s3.get_object(Bucket=s3_bucket_name, Key=file_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        phone_and_tablet.append(json_data)
        with open('data.json', 'w') as f:
            json.dump(phone_and_tablet, f)
        print(i)
        i += 1
    if file_key.endswith('.json') & file_key.startswith("electric_accessory"):
        # Download JSON file from S3
        response = s3.get_object(Bucket=s3_bucket_name, Key=file_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        electric_accessory.append(json_data)
        with open('data.json', 'w') as f:
            json.dump(electric_accessory, f)
        print(i)
        i += 1
    if file_key.endswith('.json') & file_key.startswith("camera"):
        # Download JSON file from S3
        response = s3.get_object(Bucket=s3_bucket_name, Key=file_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        camera.append(json_data)
        with open('data.json', 'w') as f:
            json.dump(camera, f)
        print(i)
        i += 1
    if file_key.endswith('.json') & file_key.startswith("man_fashion"):
        # Download JSON file from S3
        response = s3.get_object(Bucket=s3_bucket_name, Key=file_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        man_fashion.append(json_data)
        with open('data.json', 'w') as f:
            json.dump(man_fashion, f)
        print(i)
        i += 1
    if file_key.endswith('.json') & file_key.startswith("jewelry"):
        # Download JSON file from S3
        response = s3.get_object(Bucket=s3_bucket_name, Key=file_key)
        json_data = json.loads(response['Body'].read().decode('utf-8'))
        jewelry.append(json_data)
        with open('data.json', 'w') as f:
            json.dump(jewelry, f)
        print(i)
        i += 1
df = pd.DataFrame(phone_and_tablet)
df.to_csv('phone_and_tablet.csv', index=False, header=True)
df = pd.DataFrame(electric_accessory)
df.to_csv('electric_accessory.csv', index=False, header=True)
df = pd.DataFrame(camera)
df.to_csv('camera.csv', index=False, header=True)
df = pd.DataFrame(man_fashion)
df.to_csv('man_fashion.csv', index=False, header=True)
df = pd.DataFrame(jewelry)
df.to_csv('jewelry.csv', index=False, header=True)

print("Data indexed successfully into Elasticsearch.")
