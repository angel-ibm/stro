#!/usr/bin/python

import base64
import prestodb
import glob

import numpy as np
import pandas as pd

from astropy.io import fits

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
            iceberg_data.fits 
        WITH (location = 's3a://iceberg-bucket/fits') 
    '''
    try:
        cursor.execute(sql)
    except Exception as err:
        print(repr(err))
    
    sql = '''
        DROP TABLE IF EXISTS 
            iceberg_data.fits."fits-images"
    '''
    try:
        cursor.execute(sql)
    except Exception as err:
        print(repr(err))
        
    sql = '''
        CREATE TABLE 
            iceberg_data.fits."fits-images" (
                filename   VARCHAR,
                filebytes  VARCHAR,
                image_width VARCHAR,
                image_height VARCHAR,
                image_utz VARCHAR,
                object_name VARCHAR,
                object_ra VARCHAR,
                object_dec VARCHAR,
                object_alt VARCHAR,
                object_az VARCHAR,
                camera_focus VARCHAR,
                local_temp VARCHAR,
                local_lat VARCHAR,
                local_long VARCHAR,
                local_weather VARCHAR
            )
    '''
    try:
        cursor.execute(sql)
    except Exception as err:
        print(repr(err))

    cursor.close()

def insert_file(wxdconnection, image_file):

    with open(image_file, 'rb') as file:
        file_content = file.read()
    encoded_file_content = base64.b64encode(file_content).decode('utf-8')

    with fits.open(image_file) as hdul:
        image_header = hdul[0].header

        image_width = str(image_header['NAXIS1'])
        image_height = str(image_header['NAXIS2'])
        image_utz = image_header['UT-OBS']
        object_name = image_header['OBJECT']
        object_ra = str(image_header['RA'])
        object_dec = str(image_header['DEC'])
        object_alt =str(image_header['TELALT'])
        object_az = str(image_header['TELAZ'])
        camera_focus = str(image_header['CAMFOCUS'])
        local_temp = str(image_header['TELTEMP'])
        local_lat = str(image_header['LATITUDE'])
        local_long = str(image_header['LONGITUD'])
        local_weather = str(image_header['WEATHER'])

    cursor = wxdconnection.cursor()

    # I know this is a crime
    sql = f'''
        INSERT INTO iceberg_data.fits."fits-images" (
            filename, 
            filebytes,
            image_width ,
            image_height ,
            image_utz ,
            object_name ,
            object_ra ,
            object_dec ,
            object_alt ,
            object_az ,
            camera_focus ,
            local_temp ,
            local_lat ,
            local_long ,
            local_weather 
        )
        VALUES ( 
            '{image_file}',
            '{encoded_file_content}',
            '{image_width}' ,
            '{image_height} ',
            '{image_utz}' ,
            '{object_name}' ,
            '{object_ra} ',
            '{object_dec}' ,
            '{object_alt}' ,
            '{object_az} ',
            '{camera_focus}' ,
            '{local_temp} ',
            '{local_lat}' ,
            '{local_long}' ,
            '{local_weather}'                     
        )
    '''  

    try:
        cursor.execute(sql)
        wxdconnection.commit() 
    except Exception as err:
        print(f"Error executing SQL: {repr(err)}")
    finally:
        cursor.close()  

#----------------------------#

wxdconnection = connect_to_watsonxdata()

create_staging_table(wxdconnection)

file_paths = glob.glob("./images/m31*.FITS")
for image_file in sorted(file_paths):
        print("Inserting file: ", image_file)
        insert_file(wxdconnection, image_file)


