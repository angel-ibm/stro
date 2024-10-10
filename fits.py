import glob
import numpy as np

from pymilvus import(
    Milvus,
    IndexType,
    Status,
    connections,
    FieldSchema,
    DataType,
    Collection,
    CollectionSchema,
    utility,
    MilvusClient
)

from astropy.io import fits
from skimage.transform import resize

def connect_to_milvus() :
    
    host         = 'eu-de.services.cloud.techzone.ibm.com'
    port         = 25782
    user         = 'ibmlhadmin'
    key          = 'password'
    server_pem_path = 'presto.crt'
    
    # This is for Baklarz's image
    connections.connect(alias='default',
                       host=host,
                       port=port,
                       user=user,
                       password=key,
                       server_pem_path=server_pem_path,
                       server_name='watsonxdata',
                       secure=True)  

    # This is for SaaS
    # host         = 'acb3dba1-2c32-4c99-9833-6d060a2e32b4.cqh2jh8d00ae3kp0jmpg.lakehouse.appdomain.cloud'
    # port         = 30969
    # user         = 'ibmlhapikey'
    # key          = 'Xndw8q4VKrLoqM2SB_zwbEuqfyH-9d2zwCyaKFIsEElF'
    # connections.connect(         
    #     host=host, 
    #     port=port,
    #     user=user,
    #     password=key,
    #     secure=True,
    # )
    
    print(f"\nList connections:")
    print(connections.list_connections())

def create_collection():
    
    utility.drop_collection("image_embeddings")
    
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="file_path", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=16600)       
    ]
    schema = CollectionSchema(fields, "Embedding of FITS image file")
    
    fits_coll = Collection("image_embeddings", schema)

    index_params = {
            'metric_type':'L2',
            'index_type':"IVF_FLAT",
            'params':{"nlist":2048}
    }
    fits_coll.create_index(field_name="embedding", index_params=index_params)

    fits_coll.flush()
    
    return(fits_coll)
    
def load_fits_file(file_path) :
    
    with fits.open(file_path) as hdul:
   
        image_data = hdul[0].data
        
        # image_data = (image_data - np.min(image_data)) / (np.max(image_data) - np.min(image_data))
        image_resized = resize(image_data, (166, 100), mode='reflect')

    return (image_resized ) 

def generate_embedding(image_data) : 
    
    embedding = image_data.flatten()
    embedding = embedding / np.linalg.norm(embedding)  # Normalizing the embedding
    
    return embedding
    

def insert_embedding(fits_coll, file_path, embedding):
    fits_coll.insert([[file_path], [embedding]])
    fits_coll.load()

def display_collection(fits_coll):
    print(f"Collection name: {fits_coll.name}")
    print(f"Collection description: {fits_coll.description}")
    print("Fields:")
    for field in fits_coll.schema.fields:
        print(f" - Field name: {field.name}, Data type: {field.dtype}, Is primary: {field.is_primary}")
    # Get and print the number of entities in the collection
    print(f"Number of entities: {fits_coll.num_entities}")
    # Search the collection (optional, example to retrieve data)
    results = fits_coll.query(expr="id >= 0", output_fields=["id", "file_path", "embedding"], limit=5)
    for result in results:
        print(result)

def search_image(search_collection, image_file) :

    image_data = load_fits_file(image_file)

    embedding_vector = generate_embedding(image_data)

    query_embedding = [embedding_vector]
    search_params = {"metric_type": "L2", "params": {"nprobe": 100}}
    search_collection.load()
    results = search_collection.search(
        data=query_embedding,
        anns_field="embedding",
        param=search_params,
        limit=5,
        output_fields=["id", "file_path"],  
        expr=None
    )

    for result in results[0]:
        print(f"Image ID: {result.id}, Image File: {result.file_path}, Distance: {result.distance}")


def initialize_collection():
    fits_coll = create_collection()
    file_paths = glob.glob("m31*.FITS")
    for image_file in sorted(file_paths):
        print("Inserting file: ", image_file)
        image_data = load_fits_file(image_file)
        embedding_vector = generate_embedding(image_data)
        insert_embedding(fits_coll, image_file, embedding_vector)
    return fits_coll


def search_collection(fits_coll) :
    file_paths = glob.glob("m31*.FITS")
    for image_file in sorted(file_paths):
        print("Searching file:", image_file)
        search_image(fits_coll, image_file) 

#----------------------------#

connect_to_milvus()

fits_coll = initialize_collection()
# display_collection(fits_coll)
fits_coll = Collection("image_embeddings")
search_collection(fits_coll)

connections.disconnect(alias="default")