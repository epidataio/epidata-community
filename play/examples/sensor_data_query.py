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
import requests


##################################
# Define Variables and Functions #
##################################

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--host')
args = arg_parser.parse_args()

HOST = args.host or '127.0.0.1:9443'
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
ACCESS_TOKEN = 'epidata123'

# Modify default values (OPTIONAL)
COMPANY ='EpiData'
SITE = 'San_Francisco'
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

# Create a session object for HTTP requests
session = requests.Session()

# Authentication is achieved by posting to the AUTHENTICATION_URL.
url = AUTHENTICATION_URL

# An HTTP POST with JSON content requires the HTTP Content-type header.
json_header = {'Content-type': 'application/json'}

# The access token is povided via JSON.
json_body = json.dumps({'accessToken': ACCESS_TOKEN})

# Send the POST request and receive the HTTP response.
req = requests.Request('POST', AUTHENTICATION_URL, data=json_body, headers=json_header)
prepped = session.prepare_request(req)
resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

# Check that the response's HTTP response code is 200 (OK).
assert resp.status_code == 200

# Parse the JSON response.
response_json = json.loads(resp.content)
# print "response - ", response_json


###########################################
# Query Data from EpiData in a While Loop #
###########################################

print "Sending Query Request to EpiData ..."
iteration = 0

while (True):

    try:
        # Specify measurement query parameters
        begin_time = get_time("2/01/2021 00:00:00.000")
        #end_time = get_time("4/21/2021 00:00:00.000") #for empty data
        end_time = get_time("4/30/2021 00:00:00.000") #for non-empty data


        parameters = {'company': COMPANY, 'site': SITE, 'station': STATION, 'sensor': SENSOR, 'beginTime': begin_time, 'endTime': end_time}
    
        # Construct url with parameters
        url = QUERY_MEASUREMENTS_ORIGINAL_URL+urllib.urlencode(parameters)
        # print url
        # json_header = {'Cookie': session_cookie, 'Accept': 'text/plain'}
        json_header = {
                'Content-type': 'application/json'
        }

        # Send the GET request and receive the HTTP response.
        req = requests.Request('GET', url, data="", headers=json_header)
        prepped = session.prepare_request(req)
        # print "prepared statement header: \n", prepped.headers
        resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

        # Check that the response's HTTP response code is 200 (OK) and read the response.
        response_json = json.loads(resp.content)
        print "Measurement Query Results:"
        print response_json
        assert resp.status_code == 200

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
