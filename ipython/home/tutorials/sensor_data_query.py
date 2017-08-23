############################
# Import Required Modules #
############################

import argparse
import base64
from datetime import datetime, timedelta
import httplib
import json
import numpy as np
from pytz import UTC, timezone
import random
from decimal import Decimal
import struct
import time
from time import sleep
import urllib, urllib2


##################################
# Define Variables and Functions #
##################################

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--host')
args = arg_parser.parse_args()

HOST = args.host or '127.0.0.1'
AUTHENTICATION_URL = 'https://' + HOST + '/authenticate/app'
AUTHENTICATION_ROUTE = '/authenticate/app'
QUERY_MEASUREMENTS_ORIGINAL_URL = 'https://' + HOST + '/measurements_original?'
QUERY_MEASUREMENTS_CLEANSED_URL = 'https://' + HOST + '/measurements_cleansed?'
QUERY_MEASUREMENTS_SUMMARY_URL = 'https://' + HOST + '/measurements_summary?'


def get_time(time_string):
    date_object = datetime.strptime(time_string, '%m/%d/%Y %H:%M:%S.%f')
    return long(time.mktime(date_object.timetuple()) * 1e3 + date_object.microsecond / 1e3)

def add_time(time_string, delta):
    date_object = datetime.strptime(time_string, '%m/%d/%Y %H:%M:%S.%f') + timedelta(seconds=delta)
    return long(time.mktime(date_object.timetuple()) * 1e3 + date_object.microsecond / 1e3)

current_time_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")
current_time = get_time(current_time_string)


#####################
# EDIT THIS SECTION #
#####################

# Replace quoted string with API Token or GitHub Personal Access Token (REQUIRED)
ACCESS_TOKEN = 'API Token'

# Modify default values (OPTIONAL)
COMPANY ='EpiData'
SITE = 'San_Jose'
STATION = 'WSN-1'
SENSOR = "Temperature_Probe"


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


#############################
# Authenticate with EpiData #
#############################

conn = httplib.HTTPSConnection(HOST)

# Authentication is achieved by posting to the AUTHENTICATION_URL.
url = AUTHENTICATION_URL

# An HTTP POST with JSON content requires the HTTP Content-type header.
json_header = {'Content-type': 'application/json'}

# The access token is povided via JSON.
json_body = json.dumps({'accessToken': ACCESS_TOKEN})

# Send the POST request and receive the HTTP response.
conn.request('POST', AUTHENTICATION_ROUTE, json_body, json_header)
post_response = conn.getresponse()
response_status = post_response.status
response_text = post_response.read()

# Check that the response's HTTP response code is 200 (OK).
assert response_status == 200

# Parse the JSON response.
response_json = json.loads(response_text)

# Retrieve the new session id from the JSON response.
session_id = response_json['sessionId']

# Construct the session cookie.
session_cookie = 'epidata=' + session_id


###########################################
# Query Data from EpiData in a While Loop #
###########################################

print "Sending Query Request to EpiData ..."
iteration = 0

while (True):
    
    try:
        
        # Create instances that connect to the server
        conn = httplib.HTTPSConnection(HOST)

        # Specify measurement query parameters
        begin_time = get_time("8/1/2017 00:00:00.000")
        end_time = get_time("9/1/2017 00:00:00.000")
        
        parameters = {'company': COMPANY, 'site': SITE, 'station': STATION, 'sensor': SENSOR, 'beginTime': begin_time, 'endTime': end_time}

        # Construct url with parameters
        url = QUERY_MEASUREMENTS_ORIGINAL_URL+urllib.urlencode(parameters)
        print url
        json_header = {'Cookie': session_cookie, 'Accept': 'text/plain'}

        # Send the GET request and receive the HTTP response.
        conn.request('GET', url, "", json_header)
        get_response = conn.getresponse()
        response_status = get_response.status
        response_text = get_response.read()
        print response_status, response_text
               
        # Check that the response's HTTP response code is 200 (OK) and read the response.
        assert response_status == 200
        response_json = json.loads(response_text)
        print response_json

        # increment iteration and current time
        iteration += 1

        # Exit the while loop
        break

    # Handle keyboard interrupt
    except (KeyboardInterrupt, SystemExit):
        print '\n...Program Stopped Manually!'
        raise

    break


################################
# End of Data Query Script #
################################

