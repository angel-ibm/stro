#!/usr/bin/python

from confluent_kafka import Consumer, KafkaError
import json
import base64
import numpy as np
from astropy.io import fits

def create_kafka_consumer():
    conf = {
        'bootstrap.servers': 'watsonxdata:29092',  
        'group.id': 'fits_image_group',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(conf)

def save_base64_fits_image(base64_image, output_fits_path):
    
    image_bytes = base64.b64decode(base64_image)

    with open(output_fits_path, 'wb') as f:
        f.write(image_bytes)

#-----------------------------------------------------# 

topic = 'fits-images'  
output_fits_path = 'received_image.fits'  


consumer = create_kafka_consumer()
consumer.subscribe([topic])

print(f'Waiting for messages on topic "{topic}"...')


try:
    while True:
        msg = consumer.poll(1.0)  

        if msg is None:
            continue 

        if msg.error():           
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of partition for topic {msg.topic()}, partition {msg.partition()}")
            elif msg.error():
                print(f"Error occurred: {msg.error()}")
                break

        else:
            event = json.loads(msg.value().decode('utf-8'))
            image_format = event.get('image_format')
            image_base64 = event.get('image_data')
            description = event.get('file')

            print(f'Received message: {description}, format: {image_format}')

            if image_format == 'FITS' and image_base64:
                # Save the image to a FITS file
                save_base64_fits_image(image_base64, output_fits_path)
                print(f'FITS image saved to "{output_fits_path}".')

except KeyboardInterrupt:
    print("Shutting down consumer...")

finally:
    consumer.close()
