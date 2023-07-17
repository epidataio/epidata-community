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
import urllib.request, urllib.parse, urllib.error
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
AUTHENTICATION_URL = 'https://' + HOST + '/authenticate/deviceApp'

QUERY_MEASUREMENTS_ORIGINAL_URL = 'https://' + HOST + '/measurements_original?'
QUERY_MEASUREMENTS_CLEANSED_URL = 'https://' + HOST + '/measurements_cleansed?'
QUERY_MEASUREMENTS_SUMMARY_URL = 'https://' + HOST + '/measurements_summary?'

def get_time(time_string):
    date_object = datetime.strptime(time_string, '%m/%d/%Y %H:%M:%S.%f')
    return int(time.mktime(date_object.timetuple()) * 1e3 + date_object.microsecond / 1e3)

def add_time(time_string, delta):
    date_object = datetime.strptime(time_string, '%m/%d/%Y %H:%M:%S.%f') + timedelta(seconds=delta)
    return int(time.mktime(date_object.timetuple()) * 1e3 + date_object.microsecond / 1e3)

current_time_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")
current_time = get_time(current_time_string)

#####################
# EDIT THIS SECTION #
#####################

# Replace quoted string with Device ID and Device Token (REQUIRED)
DEVICE_ID = args.device_id or 'iot_device_1'
DEVICE_TOKEN = args.device_token or 'epidata_123'

# Modify default values (OPTIONAL)
COMPANY ='EpiData'
SITE = 'San_Francisco'
STATION = 'WSN-1'
SENSOR = "Temperature_Probe"

#####################################
# Disable SSL verification warnings #
#####################################

requests.packages.urllib3.disable_warnings()

#############################
# Authenticate with EpiData #
#############################

# Create a session object for HTTPS requests
session = requests.Session()

# Authentication is achieved by posting to the AUTHENTICATION_URL.
url = AUTHENTICATION_URL

# An HTTPS POST with JSON content requires the HTTPS Content-type header.
# The access token is povided via JSON header.
json_header = {'Content-type': 'application/json', 'Set-Cookie': "epidata", 'device_id': DEVICE_ID,
               'device_token': DEVICE_TOKEN}

# Send the POST request and receive the HTTPS response.
req = requests.Request('POST', AUTHENTICATION_URL, headers=json_header)
prepped = session.prepare_request(req)
resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

# Check that the response's HTTPS response code is 200 (OK).
assert resp.status_code == 200

# Parse the JSON response.
json_web_token = resp.headers.get('device_jwt')

###################################################
# Query Orignal Data from EpiData in a While Loop #
###################################################

print("Sending Query Request for Original Measurements ...")

try:
    # Specify measurement query parameters
    begin_time = get_time("1/01/2023 00:00:00.000")
    end_time = get_time("1/01/2024 00:00:00.000")

    parameters = {'company': COMPANY, 'site': SITE, 'station': STATION, 'sensor': SENSOR, 'beginTime': begin_time, 'endTime': end_time}

    # Construct url with parameters
    url = QUERY_MEASUREMENTS_ORIGINAL_URL+urllib.parse.urlencode(parameters)

    json_header = {
        'Content-type': 'application/json',
        'device_jwt': json_web_token
    }

    # Send the GET request and receive the HTTPS response.
    req = requests.Request('GET', url, data="", headers=json_header)
    prepped = session.prepare_request(req)
    resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

    # Check that the response's HTTPS response code is 200 (OK).
    assert resp.status_code == 200

    # Parse the response.
    response_json = json.loads(resp.content)
    print("Measurement Query Results - Original Data:")
    print(response_json)
    print()

# Handle keyboard interrupt
except (KeyboardInterrupt, SystemExit):
    print("\nData Query Interrupted!")
    raise

###################################################
# Query Cleansed Data from EpiData in a While Loop #
###################################################

print("Sending Query Request for Cleansed Measurements ...")

try:
    # Specify measurement query parameters
    begin_time = get_time("1/01/2023 00:00:00.000")
    end_time = get_time("1/01/2024 00:00:00.000")

    parameters = {'company': COMPANY, 'site': SITE, 'station': STATION, 'sensor': SENSOR, 'beginTime': begin_time, 'endTime': end_time}

    # Construct url with parameters
    url = QUERY_MEASUREMENTS_CLEANSED_URL+urllib.parse.urlencode(parameters)
    json_header = {
        'Content-type': 'application/json',
        'device_jwt': json_web_token
    }

    # Send the GET request and receive the HTTPS response.
    req = requests.Request('GET', url, data="", headers=json_header)
    prepped = session.prepare_request(req)
    resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

    # Check that the response's HTTPS response code is 200 (OK).
    assert resp.status_code == 200

    # Parse the response.
    response_json = json.loads(resp.content)
    print("Measurement Query Results - Cleansed Data:")
    print(response_json)
    print()

# Handle keyboard interrupt
except (KeyboardInterrupt, SystemExit):
    print("\nData Query Interrupted!")
    raise

###################################################
# Query Summary Data from EpiData in a While Loop #
###################################################

print("Sending Query Request for Summary Measurements ...")

try:
    # Specify measurement query parameters
    begin_time = get_time("1/01/2023 00:00:00.000")
    end_time = get_time("1/01/2024 00:00:00.000")

    parameters = {'company': COMPANY, 'site': SITE, 'station': STATION, 'sensor': SENSOR, 'beginTime': begin_time, 'endTime': end_time}

    # Construct url with parameters
    url = QUERY_MEASUREMENTS_SUMMARY_URL+urllib.parse.urlencode(parameters)
    json_header = {
        'Content-type': 'application/json',
        'device_jwt': json_web_token
    }

    # Send the GET request and receive the HTTPS response.
    req = requests.Request('GET', url, data="", headers=json_header)
    prepped = session.prepare_request(req)
    resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

    # Check that the response's HTTPS response code is 200 (OK)
    assert resp.status_code == 200

    # Parse the response.
    response_json = json.loads(resp.content)
    print("Measurement Query Results - Summary Data:")
    print(response_json)
    print()

# Handle keyboard interrupt
except (KeyboardInterrupt, SystemExit):
    print("\nData Query Interrupted!")
    raise

############################
# End of Data Query Script #
############################
