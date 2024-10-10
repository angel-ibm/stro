import numpy as np
from astropy.io import fits
from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType

# Step 1: Read FITS image and generate a simple embedding
def generate_fits_embedding(fits_file_path):
    # Open the FITS file
    with fits.open(fits_file_path) as hdul:
        # Assuming the image data is in the primary HDU (index 0)
        image_data = hdul[0].data

        # Check if image dimensions are 500x650
        if image_data.shape != (500, 650):
            raise ValueError(f"Expected image of shape (500, 650), but got {image_data.shape}")

        # Flatten the 2D image into a 1D vector to use as a basic embedding
        embedding = image_data.flatten()
        
        # Optionally, normalize or preprocess the embedding
        embedding = embedding / np.linalg.norm(embedding)  # Normalizing the embedding
        return embedding

# Step 2: Set up Milvus connection and collection
def setup_milvus_collection(collection_name):
    # Connect to Milvus
    connections.connect(alias="default", host='localhost', port='19530')  # Adjust if needed

    # Define fields for the collection (id and vector embedding)
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=500*650)  # Embedding dimension is 500*650
    ]

    # Define collection schema
    schema = CollectionSchema(fields, description="FITS image embeddings")
    
    # Create the collection
    collection = Collection(name=collection_name, schema=schema)
    return collection

# Step 3: Insert embedding into Milvus
def insert_embedding_into_milvus(collection, embedding):
    # Prepare data (Milvus requires data to be in list format)
    data = [
        [embedding],  # Embedding data
    ]

    # Insert data into the collection
    collection.insert(data)
    collection.load()

    print("Embedding inserted into Milvus")

if __name__ == '__main__':
    # File path of the FITS image file
    fits_file_path = 'path_to_your_fits_image.fits'  # Replace with actual file path

    # Collection name in Milvus
    collection_name = 'fits_image_embeddings'

    # Step 1: Generate embedding from the FITS image
    embedding = generate_fits_embedding(fits_file_path)

    # Step 2: Set up Milvus collection
    collection = setup_milvus_collection(collection_name)

    # Step 3: Insert the embedding into Milvus
    insert_embedding_into_milvus(collection, embedding)

    print("FITS image embedding stored in Milvus!")
