#!/usr/bin/python

from confluent_kafka import Consumer, KafkaError
import json
import base64
import numpy as np
from astropy.io import fits
import prestodb

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
        print("Connection successful")
        return wxdconnection
    except Exception as e:
        print("Unable to connect to the database.")
        print(repr(e))


def insert_into_watsonxdata(wxdconnection, image_format, file, image_base64) :

    cursor = wxdconnection.cursor()

    sql = '''
        drop table if exists iceberg_data.angel."fits-images-from-message"
    '''
    try:
        cursor.execute(sql)
    except Exception as err:
        print(repr(err))

    # sql = '''
    #     drop schema if exists iceberg_data.angel
    # '''
    # try:
    #     cursor.execute(sql)
    # except Exception as err:
    #     print(repr(err))

    # sql = '''
    #     CREATE SCHEMA iceberg_data.angel WITH (location = 's3a://iceberg-bucket/angel')
    # '''
    # try:
    #     cursor.execute(sql)
    # except Exception as err:
    #     print(repr(err))
    
    sql = '''
        create table iceberg_data.angel."fits-images-from-message" (     
            "image_format" varchar,
            "file" varchar,
            "image_data" varchar
        )
    '''
    try:
        cursor.execute(sql)
    except Exception as err:
        print(repr(err))


    # I know this is a crime
    sql = f'''
        INSERT INTO iceberg_data.angel."fits-images-from-message" (image_format, file, image_data)
        VALUES ( '{image_format}', '{file}','{image_base64}' )
    '''  
    try:
        cursor.execute(sql)
        wxdconnection.commit()
    except Exception as err:
        print(f"Error executing SQL: {repr(err)}")
    finally:
        cursor.close() 

    print(f'Inserted: {image_format} {file}')


# def read_from_kafka_table_into_watsonxdata(wxdconnection) : 

#     cursor = wxdconnection.cursor()
    
#     sql = '''
#         drop table if exists iceberg_data.angel."fits-images-from-table"
#     '''
#     try:
#         cursor.execute(sql)
#     except Exception as err:
#         print(repr(err))

#     sql = '''
#         drop schema if exists iceberg_data.angel
#     '''
#     try:
#         cursor.execute(sql)
#     except Exception as err:
#         print(repr(err))

#     sql = '''
#         CREATE SCHEMA iceberg_data.angel WITH (location = 's3a://iceberg-bucket/angel')
#     '''
#     try:
#         cursor.execute(sql)
#     except Exception as err:
#         print(repr(err))

#     sql = '''
#         create table iceberg_data.angel."fits-images-from-table" as
#         (
#             SELECT
#                 json_extract_scalar(_message, '$.image_format') AS "image_format",
#                 json_extract_scalar(_message, '$.file') AS "file",
#                 json_extract_scalar(_message, '$.image_data') AS "image_data"
#             FROM
#                 "kafka"."default"."fits-images"
#         )
#         '''
#     try:
#         cursor.execute(sql)
#     except Exception as err:
#         print(repr(err))

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
            file = event.get('file')
            image_base64 = event.get('image_data')
            
            print(f'Received message: {file}, format: {image_format}')

            if image_format == 'FITS' and image_base64:
                # Save the image to a FITS file
                # save_base64_fits_image(image_base64, output_fits_path)
                # print(f'FITS image saved to "{output_fits_path}".')

                wxdconnection = connect_to_watsonxdata()
                insert_into_watsonxdata(wxdconnection, image_format, file, image_base64)

except KeyboardInterrupt:
    print("Shutting down consumer...")

finally:
    consumer.close()
