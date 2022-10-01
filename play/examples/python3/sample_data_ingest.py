############################
# Import Required Modules #
############################

import argparse
import base64
from datetime import datetime, timedelta
import http.client
import json
import numpy as np
import random
from decimal import Decimal
import struct
import time
from time import sleep
import urllib.request, urllib.error, urllib.parse
import requests
import pandas as pd


##################################
# Define Variables and Functions #
##################################

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--host')
arg_parser.add_argument('--access_token')
arg_parser.add_argument('--device_id')
arg_parser.add_argument('--device_token')
args = arg_parser.parse_args()

HOST = args.host or '127.0.0.1:9443'
# AUTHENTICATION_URL = 'https://' + HOST + '/authenticate/app'
# AUTHENTICATION_ROUTE = '/authenticate/app'
# AUTHENTICATION_URL = 'https://' + HOST + '/login/device'
# AUTHENTICATION_ROUTE = '/login/device'

AUTHENTICATION_URL = 'https://' + HOST + '/authenticate/deviceApp'
AUTHENTICATION_ROUTE = '/authenticate/deviceApp'

EPI_STREAM = True
LOG_ITERATION = 1

if EPI_STREAM:
    CREATE_MEASUREMENT_URL = 'https://' + HOST + '/stream/measurements'
    CREATE_MEASUREMENT_ROUTE = '/stream/measurements'
else:
    CREATE_MEASUREMENT_URL = 'https://' + HOST + '/measurements'
    CREATE_MEASUREMENT_ROUTE = '/measurements'

#####################
# EDIT THIS SECTION #
#####################

# Replace quoted string with API Token or GitHub Personal Access Token (REQUIRED)
# ACCESS_TOKEN = args.access_token or 'epidata123'
DEVICE_ID = args.device_id or 'iot_device_1'
DEVICE_TOKEN = args.device_token or 'epidata_123'



#########################
# SKIP SSL VERIFICATION #
#########################
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    print("exception raised")
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    print("exception raised - else")
    ssl._create_default_https_context = _create_unverified_https_context


#############################
# Authenticate with EpiData #
#############################

# Create session object for HTTP requests
session = requests.Session()

# Authentication is achieved by posting to the AUTHENTICATION_URL.
url = AUTHENTICATION_URL
print(url)

# An HTTP POST with JSON content requires the HTTP Content-type header.
json_header = {'Content-type': 'application/json', 'Set-Cookie': "epidata", 'device_id': DEVICE_ID,
               'device_token': DEVICE_TOKEN}

# The access token is povided via JSON.
json_body = json.dumps({'device_id': DEVICE_ID,
                        'device_token': DEVICE_TOKEN})

# Send the POST request and receive the HTTP response.
req = requests.Request('POST', AUTHENTICATION_URL, headers=json_header)
prepped = session.prepare_request(req)
resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)
json_web_token = resp.headers.get('device_jwt')
# Check that the response's HTTP response code is 200 (OK).
assert resp.status_code == 200

# Parse the JSON response.
post_response = json.loads(resp.content)
print("response - ", post_response)


print("Generating Sample Sensor Data...")

try:
    #read dataframe from csv
    df = pd.read_csv('play/examples/python3/sample_data_industry.csv')

    #make a list of dictionary
    measurement_list = df.to_dict('records')

    url = CREATE_MEASUREMENT_URL

    # Request headers add parameters to the request.
    json_header = {
        'Content-type': 'application/json',
        'device_jwt': json_web_token
        }

    # Construct JSON body with data to be ingested.
    json_body = json.dumps(measurement_list)

    # Send the POST request and receive the HTTP response.
    req = requests.Request('POST', CREATE_MEASUREMENT_URL, data=json_body, headers=json_header)
    prepped = session.prepare_request(req)
    resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

    # Check that the response's HTTP response code is 201 (CREATED).
    print(resp.content)
    assert resp.status_code == 201

    
except (KeyboardInterrupt, SystemExit):
    print("\n...Program Stopped Manually!")
    raise


################################
# End of Data Ingestion Script #
################################
