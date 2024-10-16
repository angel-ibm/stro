#!/usr/bin/python

import requests
import base64
import subprocess
import json
import pandas as pd
from confluent_kafka.admin import AdminClient, NewTopic

BROKER = 'watsonxdata:29092'
topic_name ='angeltopic'
schema_name = 'default'
table_name = 'angeltable'
num_partitions=1
replication_factor=1

admin_client = AdminClient({'bootstrap.servers': BROKER})
topic_list = [NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]
existing_topics = admin_client.list_topics(timeout=10).topics
if topic_name not in existing_topics:
    fs = admin_client.create_topics(new_topics=topic_list)
    for topic, f in fs.items():
        try:
            f.result()  
            print(f"Created Kafka topic: {topic}")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
else:
    print(f"Kafka topic {topic_name} already exists.")    


# presto_userid     = %system docker exec ibm-lh-presto printenv PRESTO_USER
command = "docker exec ibm-lh-presto printenv PRESTO_USER"
result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
presto_userid = result.stdout.strip()
# presto_password   = %system docker exec ibm-lh-presto printenv LH_INSTANCE_SECRET

command = "docker exec ibm-lh-presto printenv LH_INSTANCE_SECRET"
result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
presto_password   = result.stdout.strip()
print(f"Presto user: {presto_userid} Presto password: {presto_password}")

credentials_bytes = f"{presto_userid}:{presto_password}".encode("ascii") 
credentials       = base64.b64encode(credentials_bytes).decode("ascii")
print(f"Credentials Base64: {credentials}")

host              = "https://watsonxdata"
port              = 8443
api               = "/v1"
certfile          = "/certs/lh-ssl-ts.crt"

auth_header = {
    "Content-Type"  : "text/javascript",
    "Authorization" : f"Basic {credentials}",
    "X-Presto-User" : presto_userid
}

service = "/info"
request = {}
r = requests.get(f"{host}:{port}{api}{service}", headers=auth_header, verify=certfile)
print(f"Reason: {r.reason} status: {r.status_code} starting: {r.json()['starting']}")


service = "/kafka/register-topic"

columns = [
        {"name": "event_id", "type": "VARCHAR"},
        {"name": "event_name", "type": "VARCHAR"},
        {"name": "event_time", "type": "TIMESTAMP"}
]
payload = {
        "topic_name": topic_name,
        "kafka_broker_url": BROKER,
        "schema_name": schema_name,
        "table_name": table_name,
        "partitions": num_partitions,
        "replication_factor": replication_factor,
        "columns" : columns
    }

response = requests.post(f"{host}:{port}{api}{service}", headers=auth_header, data=json.dumps(payload),verify=certfile)
        
try :
    if response.status_code == 200:
        print(f"Kafka topic '{topic_name}' registered successfully!")
    else:
        print(f"Failed to register Kafka topic. Status Code: {response.status_code}")
        print(f"Response: {response.text}")

except Exception as e:
    print(f"Error while registering Kafka topic: {str(e)}")



exit()


def restfulSQL(host,port,api,auth_header,certfile,sql):
    
    from time import sleep

    service = "/statement"
    data    = []
    columns = []
    error   = False
    
    URI = f"{host}:{port}{api}{service}"
    r = requests.post(URI, headers=auth_header, data=sql, verify=certfile)
    if (r.ok == False):
        print(r.reason)
        return None

    while r.ok == True: 
        results = r.json()
        collect = False
        stats = results.get('stats',None)
        state = stats['state']
        print(state)
        if (state in ["FINISHED","RUNNING"]):
            collect = True
        elif (state == "FAILED"):
            errormsg = results.get('error',None)
            if (errormsg != None):
                print(f"Error: {errormsg.get('message')}")
            error = True
            break
        else:
            collect = False
    
        if (collect == True):
            columns = results.get('columns',None)
            result  = results.get('data',None)
            if (result not in [None]):
                data.append(result)
    
        URI = results.get('nextUri',None)
        if (URI != None):    
            sleep(.1)
            r = requests.get(URI, headers=auth_header, verify=certfile)
        else:
            break

    if (error == True):
        return None
  
    column_names = []
    for col in columns:
        column_names.append(col.get("name"))
    
    data_values = []
    for row in data[0]:
        data_values.append(row)
    
    df = pd.DataFrame(data=data_values, columns=column_names)
    return df

df = restfulSQL(host,port,api,auth_header,certfile,'select * from "tpch"."tiny"."customer" limit 10')


print(df)