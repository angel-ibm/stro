#!/usr/bin/python

import requests
import base64
import subprocess

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