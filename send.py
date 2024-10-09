from kafka import KafkaProducer
from astropy.io import fits
import json
import base64

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
        bootstrap_servers='watsonxdata:29092',  # Adjust to your Kafka broker address
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
    fits_image_path = 'small_image.fits'  # Replace with your FITS file path
    topic = 'fits-images'  # Kafka topic where images will be sent

    # Read the image and prepare it for sending
    image_base64 = read_fits_image_as_base64(fits_image_path)

    # Create Kafka producer
    producer = create_kafka_producer()

    # Send the image
    send_fits_image_to_kafka(producer, topic, image_base64)

    print(f'FITS image sent to Kafka topic "{topic}".')
