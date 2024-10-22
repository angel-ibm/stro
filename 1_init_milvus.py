#!/usr/bin/python

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
    
    # This is for Baklarz's image
    host         = 'eu-de.services.cloud.techzone.ibm.com'
    port         = 25782
    user         = 'ibmlhadmin'
    key          = 'password'
    server_pem_path = 'presto.crt'
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
    
    print(f"\Available connections:")
    print(connections.list_connections())

def create_collection():
    
    utility.drop_collection("image_embeddings")
    
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="file_path", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=16600),
        FieldSchema(name="image_width", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="image_height", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="image_utz", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="object_name", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="object_ra", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="object_dec", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="object_alt", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="object_az", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="camera_focus", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="local_temp", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="local_lat", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="local_long", dtype=DataType.VARCHAR, max_length=128),
        FieldSchema(name="local_weather", dtype=DataType.VARCHAR, max_length=128)

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
        image_header = hdul[0].header
        image_data = hdul[0].data
        image_resized = resize(image_data, (166, 100), mode='reflect')

    return (image_header, image_resized) 

def generate_embedding(image_data) : 
    
    embedding = image_data.flatten()
    embedding = embedding / np.linalg.norm(embedding)  
    
    return embedding
    

def insert_embedding(fits_coll, file_path, header, embedding):

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

    fits_coll.insert([  [file_path], 
                        [embedding],
                        [image_width], 
                        [image_height], 
                        [image_utz], 
                        [object_name], 
                        [object_ra], 
                        [object_dec], 
                        [object_alt],
                        [object_az], 
                        [camera_focus], 
                        [local_temp], 
                        [local_lat], 
                        [local_long], 
                        [local_weather]   
                      ])
    fits_coll.load()

def initialize_collection():
    fits_coll = create_collection()
    file_paths = glob.glob("./images/m31*.FITS")
    for image_file in sorted(file_paths):
        print("Inserting file: ", image_file)
        image_header, image_data = load_fits_file(image_file)
        embedding_vector = generate_embedding(image_data)
        insert_embedding(fits_coll, image_file, image_header, embedding_vector)
    return fits_coll

#----------------------------#

connect_to_milvus()
fits_coll = initialize_collection()
connections.disconnect(alias="default")