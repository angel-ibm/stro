#!/usr/bin/python

import subprocess
import json
import os

kafka_compose = '''
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - 22181:2181
  
  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on:
      - zookeeper
    ports:
      - 29092:29092
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://watsonxdata:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
'''
with open("kafka-compose.yaml","w") as fd:
    fd.write(kafka_compose)


command = "docker compose -p kafka -f kafka-compose.yaml down"
result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
print(result.stdout)

os.sleep(5)

command = "docker compose -p kafka -f kafka-compose.yaml up --detach"
result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
print(result.stdout)

os.sleep(5)

command = "python3 -m pip install confluent-kafka"
result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
print(result.stdout)

command = "firewall-cmd --add-port=29092/tcp --zone=public --permanent"
result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
print(result.stdout)

command = "firewall-cmd --reload"
result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
print(result.stdout)

text_to_print = '''
Please configure a kafka database in watsonx.data with the following parameters:
  Display name - kafka
  Hostname - watsonxdata
  Port - 29092
  Catalog name - kafka
'''
print(text_to_print)

fits_schema_file = {
    "tableName": "fits-images",
    "schemaName": "fits-images",
    "topicName": "fits-images",
    "key": {
        "dataFormat": "raw",
        "fields": [
            {
                "name": "key",
                "dataFormat": "LONG",
                "type": "BIGINT",
                "hidden": "false"
            }
        ]
    },
    "message": {
        "dataFormat": "json",
        "fields": [
            {
                "name": "image_format",
                "mapping": "image_format",
                "type": "VARCHAR"
            },
            {
                "name": "file",
                "mapping": "file",
                "type": "VARCHAR"
            },
            {
                "name": "image_data",
                "mapping": "image_data",
                "type": "VARCHAR"
            }
        ]
    }
}

with open("fits-images.fits-images.json","w") as fd:
    fd.write(json.dumps(fits_schema_file))

print("and then, use the schema file fits-images.fits-images.json to register a topic file in watsonx.data")



# SELECT
#     json_extract_scalar(_message, '$.image_format') AS format,
#     json_extract_scalar(_message, '$.file') AS file
# FROM
#   "kafka"."default"."fits-images"
# LIMIT
#   100;

# create table iceberg_data.angel."fits-images" as
# (
#     SELECT
#         json_extract_scalar(_message, '$.image_format') AS "image_format",
#         json_extract_scalar(_message, '$.file') AS "file",
#         json_extract_scalar(_message, '$.image_data') AS "image_data"
#     FROM
#         "kafka"."default"."fits-images"
# )