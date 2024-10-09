#!/usr/bin/python

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from astropy.io import fits
import json
import base64

# Kafka configuration
BROKER = 'watsonxdata:29092'  # Adjust to your Kafka broker address

# Function to create a Kafka topic if it doesn't exist
def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': BROKER})
    
    topic_list = [NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
    
    # Check if the topic already exists before creating it
    existing_topics = admin_client.list_topics(timeout=10).topics
    if topic_name not in existing_topics:
        fs = admin_client.create_topics(new_topics=topic_list)
        for topic, f in fs.items():
            try:
                f.result()  # Ensure the topic creation succeeded
                print(f"Created Kafka topic: {topic}")
            except Exception as e:
                print(f"Failed to create topic {topic}: {e}")
    else:
        print(f"Kafka topic {topic_name} already exists.")

# Function to read a FITS image and convert it to a base64 string
def read_fits_image_as_base64(fits_file_path):
    with fits.open(fits_file_path) as hdul:
        # Assuming we are reading the primary HDU (HDU 0) which contains the image data
        image_data = hdul[0].data
        # Convert the image data to bytes (you could also compress it if needed)
        image_bytes = image_data.tobytes()
        # Convert to base64 for sending through Kafka
        image_base64 = base64.b64encode(image_bytes).decode('utf-8')
    return image_base64

# Kafka Producer configuration
def create_kafka_producer():
    conf = {
        'bootstrap.servers': BROKER,
        'client.id': 'fits_image_producer',
    }
    return Producer(conf)

# Function to send FITS image to Kafka using `produce()`
def send_fits_image_to_kafka(producer, topic, file, image_base64):
    event = {
        'image_format': 'FITS',
        'file': file,
        'image_data': image_base64        
    }
    # Convert the event to JSON and produce it to the Kafka topic
    producer.produce(topic, key="fits_image", value=json.dumps(event), callback=delivery_report)
    producer.flush()

# Delivery report callback to check if the message was delivered successfully
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == '__main__':
    # Path to the FITS image file
    fits_image_path = 'm31.fits'  # Replace with your FITS file path
    topic = 'fits-images'  # Kafka topic where images will be sent

    # Step 1: Create the Kafka topic
    create_kafka_topic(topic)

    # Step 2: Read the image and prepare it for sending
    image_base64 = read_fits_image_as_base64(fits_image_path)

    # Step 3: Create Kafka producer
    producer = create_kafka_producer()

    # Step 4: Send the image using `produce()`
    send_fits_image_to_kafka(producer, topic, fits_image_path, image_base64)

    print(f'FITS image sent to Kafka topic "{topic}".')
