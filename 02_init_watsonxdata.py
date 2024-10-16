#!/usr/bin/python

import base64
import prestodb
import glob

import numpy as np
import pandas as pd

# def load_fits_file(file_path) :
    
#     with fits.open(file_path) as hdul:
   
#         image_data = hdul[0].data
       

#     return (image_resized ) 


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

def create_staging_table(wxdconnection) :

    cursor = wxdconnection.cursor()

    sql = '''
        CREATE SCHEMA IF NOT EXISTS 
            iceberg_data.hello 
        WITH (location = 's3a://iceberg-bucket/hello') 
    '''
    try:
        cursor.execute(sql)
    except Exception as err:
        print(repr(err))
    
    sql = '''
        DROP TABLE IF EXISTS 
            iceberg_data.hello."fits-images"
    '''
    try:
        cursor.execute(sql)
    except Exception as err:
        print(repr(err))
        
    sql = '''
        CREATE TABLE 
            iceberg_data.hello."fits-images" (
                filename   VARCHAR,
                filebytes  VARCHAR
            )
    '''
    try:
        cursor.execute(sql)
    except Exception as err:
        print(repr(err))

#----------------------------#

wxdconnection = connect_to_watsonxdata()

create_staging_table(wxdconnection)

# file_paths = glob.glob("./images/m31*.FITS")
#     for image_file in sorted(file_paths):
#         print("Inserting file: ", image_file)
#         insert_file


