############################
# Import Required Modules #
############################

import argparse
import base64
from datetime import datetime, timedelta
import http.client
import json
import numpy as np
from pytz import UTC, timezone
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
AUTHENTICATION_ROUTE = '/authenticate/deviceApp'

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
COMPANY ='Company-1'
SITE = 'Site-1'
DEVICE_GROUP = '1000'
TESTER = 'Station-1'


#########################
# SKIP SSL VERIFICATION #
#########################
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # print("exception raised -  AttributeError")
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # print("exception raised - other cases")
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context


#############################
# Authenticate with EpiData #
#############################

# Create a session object for HTTP requests
session = requests.Session()

# Authentication is achieved by posting to the AUTHENTICATION_URL.
url = AUTHENTICATION_URL

# An HTTP POST with JSON content requires the HTTP Content-type header.
# The access token is povided via JSON header.
json_header = {'Content-type': 'application/json', 'Set-Cookie': "epidata", 'device_id': DEVICE_ID,
               'device_token': DEVICE_TOKEN}

# Send the POST request and receive the HTTP response.
req = requests.Request('POST', AUTHENTICATION_URL, headers=json_header)
prepped = session.prepare_request(req)
resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

# Check that the response's HTTP response code is 200 (OK).
assert resp.status_code == 200

# Parse the JSON response.
json_web_token = resp.headers.get('device_jwt')
response_json = json.loads(resp.content)
# print("response - ", response_json)


###################################################
# Query Orignal Data from EpiData in a While Loop #
###################################################

# print("Sending Query Request to EpiData ...")
iteration = 0

while (True):

    try:
        # Specify automated tests query parameters
        begin_time = get_time("1/01/2022 00:00:00.000")
        end_time = get_time("1/01/2023 00:00:00.000")


        parameters = {'company': COMPANY, 'site': SITE, 'device_group': DEVICE_GROUP, 'tester': TESTER, 'beginTime': begin_time, 'endTime': end_time}

        # Construct url with parameters
        url = QUERY_MEASUREMENTS_ORIGINAL_URL+urllib.parse.urlencode(parameters)
        # print(url)

        json_header = {
                'Content-type': 'application/json',
                'device_jwt': json_web_token
        }

        # Send the GET request and receive the HTTP response.
        req = requests.Request('GET', url, data="", headers=json_header)
        prepped = session.prepare_request(req)
        # print("prepared statement header: \n", prepped.headers)
        resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

        # Check that the response's HTTP response code is 200 (OK) and read the response.
        response_json = json.loads(resp.content)
        print("Automated Test Query Results - Original Data:")
        print(response_json)
        assert resp.status_code == 200

        # increment iteration and current time
        iteration += 1

        # Exit the while loop
        break

    # Handle keyboard interrupt
    except (KeyboardInterrupt, SystemExit):
        print("\n...Program Stopped Manually!")
        raise

    break

###################################################
# Query Cleansed Data from EpiData in a While Loop #
###################################################

# print("Sending Query Request to EpiData ...")
iteration = 0

while (True):

    try:
        # Specify automated tests query parameters
        begin_time = get_time("1/01/2022 00:00:00.000")
        end_time = get_time("1/01/2023 00:00:00.000")

        parameters = {'company': COMPANY, 'site': SITE, 'device_group': DEVICE_GROUP, 'tester': TESTER, 'beginTime': begin_time, 'endTime': end_time}

        # Construct url with parameters
        url = QUERY_MEASUREMENTS_CLEANSED_URL+urllib.parse.urlencode(parameters)
        # print(url)
        json_header = {
                'Content-type': 'application/json',
                'device_jwt': json_web_token
        }

        # Send the GET request and receive the HTTP response.
        req = requests.Request('GET', url, data="", headers=json_header)
        prepped = session.prepare_request(req)
        # print("prepared statement header: \n", prepped.headers)
        resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

        # Check that the response's HTTP response code is 200 (OK) and read the response.
        response_json = json.loads(resp.content)
        print("Automated Test Query Results - Cleansed Data:")
        print(response_json)
        assert resp.status_code == 200

        # increment iteration and current time
        iteration += 1

        # Exit the while loop
        break

    # Handle keyboard interrupt
    except (KeyboardInterrupt, SystemExit):
        print("\n...Program Stopped Manually!")
        raise

    break


###################################################
# Query Summary Data from EpiData in a While Loop #
###################################################

# print("Sending Query Request to EpiData ...")
iteration = 0

while (True):

    try:
        # Specify automated tests query parameters
        begin_time = get_time("1/01/2022 00:00:00.000")
        end_time = get_time("1/01/2023 00:00:00.000")

        parameters = {'company': COMPANY, 'site': SITE, 'device_group': DEVICE_GROUP, 'tester': TESTER, 'beginTime': begin_time, 'endTime': end_time}

        # Construct url with parameters
        url = QUERY_MEASUREMENTS_SUMMARY_URL+urllib.parse.urlencode(parameters)
        # print(url)
        json_header = {
                'Content-type': 'application/json',
                'device_jwt': json_web_token
        }

        # Send the GET request and receive the HTTP response.
        req = requests.Request('GET', url, data="", headers=json_header)
        prepped = session.prepare_request(req)
        # print("prepared statement header: \n", prepped.headers)
        resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

        # Check that the response's HTTP response code is 200 (OK) and read the response.
        response_json = json.loads(resp.content)
        print("Automated Test Query Results - Summary Data:")
        print(response_json)
        assert resp.status_code == 200

        # increment iteration and current time
        iteration += 1

        # Exit the while loop
        break

    # Handle keyboard interrupt
    except (KeyboardInterrupt, SystemExit):
        print("\n...Program Stopped Manually!")
        raise

    break

################################
# End of Data Query Script #
################################
