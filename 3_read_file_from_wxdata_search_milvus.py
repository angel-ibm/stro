#!/usr/bin/python

import glob
import prestodb
from prestodb import transaction

import numpy as np
import pandas as pd

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
    
    print(f"\nList connections:")
    print(connections.list_connections())

    
def load_fits_file(file_path) :
    
    with fits.open(file_path) as hdul:
   
        image_data = hdul[0].data
        image_resized = resize(image_data, (166, 100), mode='reflect')

    return (image_resized ) 

def generate_embedding(image_data) : 
    
    embedding = image_data.flatten()
    embedding = embedding / np.linalg.norm(embedding)  # Normalizing the embedding
    
    return embedding

def search_image(search_collection, image_file) :

    image_data = load_fits_file(image_file)

    embedding_vector = generate_embedding(image_data)

    query_embedding = [embedding_vector]
    search_params = {"metric_type": "L2", "params": {"nprobe": 1000}}
    search_collection.load()
    results = search_collection.search(
        data=query_embedding,
        anns_field="embedding",
        param=search_params,
        limit=3,
        output_fields=["id", "file_path"],  
        expr=None
    )

    for result in results[0]:
        print(f"Image ID: {result.id}, Image File: {result.file_path}, Difference: {result.distance:.2%}")


def search_collection(fits_coll) :
    file_paths = glob.glob("m31*.fits")
    for image_file in sorted(file_paths):
        print("Searching file:", image_file)
        search_image(fits_coll, image_file) 

def connect_to_watsonxdata() :

    # Connection Parameters
    userid     = 'ibmlhadmin'
    password   = 'password'
    hostname   = 'watsonxdata'
    port       = '8443'
    catalog    = 'tpch'
    schema     = 'tiny'
    certfile   = "/certs/lh-ssl-ts.crt"

    # Connect Statement
    try:
        wxdconnection = prestodb.dbapi.connect(
                host=hostname,
                port=port,
                user=userid,
                catalog=catalog,
                schema=schema,
                http_scheme='https',
                auth=prestodb.auth.BasicAuthentication(userid, password)
        )
        if (certfile != None):
            wxdconnection._http_session.verify = certfile
        cursor = wxdconnection.cursor()
        print("Connection successful")
        return wxdconnection
    except Exception as e:
        print("Unable to connect to the database.")
        print(repr(e))

def get_image_from_watsonxdata(wxdconnection) :

    sql = '''
    SELECT json_extract_scalar(_message, '$.file') AS "image_data" 
    FROM "kafka"."default"."fits-images" 
    LIMIT 1 
    '''
    try:
        df = pd.read_sql(sql,wxdconnection)
        if (len(df) == 0):
            print("No rows found.")
    except Exception as e:
        print(repr(e))
    

    return df



#----------------------------#


wxdconnection = connect_to_watsonxdata()


image_data = get_image_from_watsonxdata(wxdconnection)

print(image_data)



exit()

connect_to_milvus()

fits_coll = Collection("image_embeddings")
search_collection(fits_coll)

connections.disconnect(alias="default")