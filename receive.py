from kafka import KafkaConsumer
import json
import base64
import numpy as np
from astropy.io import fits

# Kafka Consumer configuration
def create_kafka_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers='watsonxdata:29092',  # Adjust to your Kafka broker address
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Deserialize JSON messages
        auto_offset_reset='earliest',  # Start reading at the earliest offset
        enable_auto_commit=True,
        group_id='fits_image_group'  # Consumer group ID
    )

# Function to convert base64 string back to image and save it as a FITS file
def save_base64_fits_image(base64_image, output_fits_path):
    # Decode base64 to bytes
    image_bytes = base64.b64decode(base64_image)
    
    # Convert bytes back to a numpy array (adjusting the shape as needed)
    # You need to know the shape of the original data to correctly reshape
    # For example, assuming the image is 100x100 pixels:
    image_data = np.frombuffer(image_bytes, dtype=np.float32)  # Adjust dtype if needed
    image_data = image_data.reshape((100, 100))  # Replace with actual shape of the image

    # Create a FITS file and save it
    hdu = fits.PrimaryHDU(image_data)
    hdu.writeto(output_fits_path, overwrite=True)

if __name__ == '__main__':
    topic = 'fits-images'  # Kafka topic to consume from
    output_fits_path = 'received_image.fits'  # Path where the FITS file will be saved

    # Create Kafka consumer
    consumer = create_kafka_consumer(topic)

    print(f'Waiting for messages on topic "{topic}"...')

    # Consume messages from Kafka
    for message in consumer:
        event = message.value
        image_format = event.get('image_format')
        image_base64 = event.get('image_data')
        description = event.get('description')

        print(f'Received message: {description}, format: {image_format}')

        if image_format == 'FITS' and image_base64:
            # Save the image to a FITS file
            save_base64_fits_image(image_base64, output_fits_path)
            print(f'FITS image saved to "{output_fits_path}".')

