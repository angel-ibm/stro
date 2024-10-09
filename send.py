from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from astropy.io import fits
import json
import base64

# Function to create a Kafka topic if it doesn't exist
def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = KafkaAdminClient(
        bootstrap_servers='watsonxdata:29092',  # Adjust to your Kafka broker address
        client_id='fits_image_producer'
    )
    
    topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
    
    # Check if the topic already exists before creating it
    existing_topics = admin_client.list_topics()
    if topic_name not in existing_topics:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Created Kafka topic: {topic_name}")
    else:
        print(f"Kafka topic {topic_name} already exists.")
    
    admin_client.close()

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
    return KafkaProducer(
        bootstrap_servers='localhost:9092',  # Adjust to your Kafka broker address
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize messages as JSON
    )

# Function to send FITS image to Kafka
def send_fits_image_to_kafka(producer, topic, image_base64):
    event = {
        'image_format': 'FITS',
        'image_data': image_base64,
        'description': 'Small FITS image'
    }
    producer.send(topic, event)
    producer.flush()

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

    # Step 4: Send the image
    send_fits_image_to_kafka(producer, topic, image_base64)

    print(f'FITS image sent to Kafka topic "{topic}".')
