#!/usr/bin/python

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
import json
import base64

BROKER = 'watsonxdata:29092' 

def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):

    admin_client = AdminClient({'bootstrap.servers': BROKER})
    
    topic_list = [NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
    
    existing_topics = admin_client.list_topics(timeout=10).topics
    if topic_name not in existing_topics:
        fs = admin_client.create_topics(new_topics=topic_list)
        for topic, f in fs.items():
            try:
                f.result()  
                print(f"Created Kafka topic: {topic}")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
    else:
        print(f"Kafka topic {topic_name} already exists.")

def create_kafka_producer():
    conf = {
        'bootstrap.servers': BROKER,
        'client.id': 'fits_image_producer',
    }
    return Producer(conf)

def read_fits_image_as_base64(fits_image_path) :

    with open(fits_image_path, 'rb') as file :
        file_content = file.read()
    
    encoded_file_content = base64.b64encode(file_content).decode('utf-8')

    return encoded_file_content

def send_file_image_to_kafka(producer, topic, file, image_base64):

    event = {
        'image_format': 'FITS',
        'file': file,
        'image_data': image_base64        
    }
    
    producer.produce(topic, key="fits_image", value=json.dumps(event), callback=delivery_report)
    producer.flush()

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

#----------------------------------#

topic = 'fits-images'  
create_kafka_topic(topic)
producer = create_kafka_producer()

file_image_path = './images/m31dot.fits'  
image_base64 = read_fits_image_as_base64(file_image_path)

send_file_image_to_kafka(producer, topic, file_image_path, image_base64)

print(f'File sent to Kafka topic "{topic}".')
