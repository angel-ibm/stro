#!/usr/bin/python

import requests
import base64
import subprocess

# presto_userid     = %system docker exec ibm-lh-presto printenv PRESTO_USER
command = "echo docker exec ibm-lh-presto printenv PRESTO_USER"
result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
presto_userid = result.stdout.strip()

command = "echo docker exec ibm-lh-presto printenv PRESTO_USER"
result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)

# presto_password   = %system docker exec ibm-lh-presto printenv LH_INSTANCE_SECRET
presto_password   = result.stdout.strip()
print(f"Presto user: {presto_userid} Presto password: {presto_password}")
