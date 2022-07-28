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

# ACCESS_TOKEN = args.access_token or 'epidata123'
DEVICE_ID = args.device_id or 'iot_device_1'
DEVICE_TOKEN = args.device_token or 'epidata_123'

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

def get_time(time_string):
    date_object = datetime.strptime(time_string, '%m/%d/%Y %H:%M:%S.%f')
    return int(time.mktime(date_object.timetuple())
                * 1e3 + date_object.microsecond / 1e3)

def add_time(time_string, delta):
    date_object = datetime.strptime(
        time_string, '%m/%d/%Y %H:%M:%S.%f') + timedelta(seconds=delta)
    return int(time.mktime(date_object.timetuple())
                * 1e3 + date_object.microsecond / 1e3)

current_time_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")
current_time = get_time(current_time_string)

#####################
# EDIT THIS SECTION #
#####################

# Replace quoted string with API Token or GitHub Personal Access Token (REQUIRED)
ACCESS_TOKEN = args.access_token or 'epidata123'


#########################
# SKIP SSL VERIFICATION #
#########################
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context


##############################
# AUTHENTICATE with epidata. #
##############################

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
print(resp.text)
assert resp.status_code == 200

# Parse the JSON response.
post_response = json.loads(resp.content)
print("response - ", post_response)


#####################################################
# CREATE an automated test temperature measurement. #
#####################################################

# Measurements are created by sending an HTTP POST request to the create url.
url = CREATE_MEASUREMENT_URL

# Request headers add parameters to the request.
headers = {
    'Content-type': 'application/json',
    'json_web_token': json_web_token
}

# The measurement data is assembled in a python dictionary and converted
# to JSON.
json_body = json.dumps([{
    'company': 'Company-1',
    'site': 'Site-1',
    'device_group': '1000',
    'tester': 'Station-1',

    # 'ts' is a 64 bit integer representing a timestamp in milliseconds.
    'ts': current_time,
    'device_name': '100001',
    'test_name': 'Temperature Measurement Test',
    'meas_name': 'Temperature',

    # The meas_value field can contain a number or a text string, depending on
    # the measurement data type.
    'meas_value': float(round(random.normalvariate(65, 1.5), 2)),

    # The meas_datatype field represents the type of the meas_value provided (in
    # this case the type is double precision floating point).
    'meas_datatype': 'double',
    'meas_unit': 'degree C',
    'meas_status': 'PASS',
    'meas_lower_limit': 40.0,
    'meas_upper_limit': 90.0,
    'meas_description': '',
    'device_status': 'PASS',
    'test_status': 'PASS'
}])

# Construct and send the POST request.
#post_request = urllib2.Request(url, headers=headers, data=json_body)
req = requests.Request('POST', CREATE_MEASUREMENT_URL, data=json_body, headers=json_header)
prepped = session.prepare_request(req)
resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

# Send the POST request and receive the HTTP response.
#post_response2 = urllib2.urlopen(post_request)

# Check that the response's HTTP response code is 201 (CREATED).
#assert post_response2.getcode() == 201
print(resp.content)
assert resp.status_code == 201

# Print measurement details
print(json_body + "\n")


##################################################
# CREATE an automated test power on measurement. #
##################################################

url = CREATE_MEASUREMENT_URL
headers = {'Content-type': 'application/json'}
json_body = json.dumps([{
    'company': 'Company-2',
    'site': 'Site-2',
    'device_group': '2000',
    'tester': 'Station-2',
    'ts': current_time,
    'device_name': '200002',
    'test_name': 'Power On Test',
    'meas_name': 'Power On Status',

    # The meas_value field can contain a text string.
    'meas_value': 'Device Powered On Successfully',

    # The meas_datatype field is set to 'string' for string values.
    'meas_datatype': 'string',
    'meas_status': 'PASS',
    'meas_description': '',
    'device_status': 'PASS',
    'test_status': 'PASS'
}])

#post_request = urllib2.Request(url, headers=headers, data=json_body)
#post_response = urllib2.urlopen(post_request)
#assert post_response.getcode() == 201

# Construct and send the POST request.
req = requests.Request('POST', CREATE_MEASUREMENT_URL, data=json_body, headers=json_header)
prepped = session.prepare_request(req)
resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

# Check that the response's HTTP response code is 201 (CREATED).
print(resp.content)
assert resp.status_code == 201

# Print measurement details
print(json_body + "\n")
