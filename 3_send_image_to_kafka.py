#!/usr/bin/python

import json
import base64

from astropy.io import fits
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

BROKER = 'watsonxdata:29092' 

def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):

    admin_client = AdminClient({'bootstrap.servers': BROKER})
    
    existing_topics = admin_client.list_topics(timeout=10).topics
    if topic_name in existing_topics:
        metadata = admin_client.delete_topics([topic_name])
        for topic, f in metadata.items():
            print(f"Topic {topic} existed and has been deleted")

    topic_list = [NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]

    fs = admin_client.create_topics(new_topics=topic_list)

    for topic, f in fs.items():
        try:
            f.result()  
            print(f"Created Kafka topic: {topic}")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
    

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

    with fits.open(fits_image_path) as hdul:
        image_header = hdul[0].header

    return image_header, encoded_file_content

def send_file_image_to_kafka(producer, topic, file, image_base64, header):

    image_width = str(header['NAXIS1'])
    image_height = str(header['NAXIS2'])
    image_utz = header['UT-OBS']
    object_name = header['OBJECT']
    object_ra = str(header['RA'])
    object_dec = str(header['DEC'])
    object_alt =str( header['TELALT'])
    object_az = str(header['TELAZ'])
    camera_focus = str(header['CAMFOCUS'])
    local_temp = str(header['TELTEMP'])
    local_lat = str(header['LATITUDE'])
    local_long = str(header['LONGITUD'])
    local_weather = str(header['WEATHER'])

    event = {
        'file':         file,
        'image_width':  image_width,
        'image_height': image_height,
        'image_utz':    image_utz,
        'object_name':  object_name,
        'object_ra':    object_ra,
        'object_dec':   object_dec,
        'object_alt':   object_alt,
        'object_az':    object_az,
        'camera_focus': camera_focus,
        'local_temp':   local_temp,
        'local_lat':    local_lat,
        'local_long':   local_long,
        'local_weather':local_weather,   
        'image_data':   image_base64        
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
image_header, image_base64 = read_fits_image_as_base64(file_image_path)

send_file_image_to_kafka(producer, topic, file_image_path, image_base64, image_header)

print(f'File sent to Kafka topic "{topic}".')
