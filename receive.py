#!/usr/bin/python

from confluent_kafka import Consumer, KafkaError
import json
import base64
import numpy as np
from astropy.io import fits

# Kafka Consumer configuration
def create_kafka_consumer():
    conf = {
        'bootstrap.servers': 'watsonxdata:29092',  # Adjust to your Kafka broker
        'group.id': 'fits_image_group',
        'auto.offset.reset': 'earliest'  # Read messages from the beginning if no offset is stored
    }
    return Consumer(conf)

# Function to convert base64 string back to image and save it as a FITS file
def save_base64_fits_image(base64_image, output_fits_path):
    # Decode base64 to bytes
    image_bytes = base64.b64decode(base64_image)

    with open(output_fits_path, 'wb') as f:
        f.write(image_bytes)

if __name__ == '__main__':
    topic = 'fits-images'  # Kafka topic to consume from
    output_fits_path = 'received_image.fits'  # Path where the FITS file will be saved

    # Step 1: Create Kafka consumer
    consumer = create_kafka_consumer()
    
    # Step 2: Subscribe to the topic
    consumer.subscribe([topic])

    print(f'Waiting for messages on topic "{topic}"...')

    # Step 3: Poll for messages
    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for messages (waits for 1 second)

            if msg is None:
                continue  # No message received, continue polling

            if msg.error():
                # Handle any errors that might arise
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition reached
                    print(f"Reached end of partition for topic {msg.topic()}, partition {msg.partition()}")
                elif msg.error():
                    print(f"Error occurred: {msg.error()}")
                    break

            else:
                # Process the message (decode JSON, decode image, and save it)
                event = json.loads(msg.value().decode('utf-8'))
                image_format = event.get('image_format')
                image_base64 = event.get('image_data')
                description = event.get('description')

                print(f'Received message: {description}, format: {image_format}')

                if image_format == 'FITS' and image_base64:
                    # Save the image to a FITS file
                    save_base64_fits_image(image_base64, output_fits_path)
                    print(f'FITS image saved to "{output_fits_path}".')

    except KeyboardInterrupt:
        # Gracefully stop the consumer
        print("Shutting down consumer...")

    finally:
        # Close down the consumer on exit
        consumer.close()
