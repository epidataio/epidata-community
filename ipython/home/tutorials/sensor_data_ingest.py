############################
# Import Required Modules #
############################

import argparse
import base64
from datetime import datetime, timedelta
import httplib
import json
import numpy as np
import random
from decimal import Decimal
import struct
import time
from time import sleep
import urllib2


##################################
# Define Variables and Functions #
##################################

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--host')
args = arg_parser.parse_args()

HOST = args.host or '127.0.0.1'
AUTHENTICATION_URL = 'https://' + HOST + '/api/authenticate/github'
AUTHENTICATION_ROUTE = '/api/authenticate/github'
USE_KAFKA = True
LOG_ITERATION = 1

if USE_KAFKA:
    CREATE_MEASUREMENT_URL = 'https://' + HOST + '/kafka/measurements'
    CREATE_MEASUREMENT_ROUTE = '/kafka/measurements'
    CREATE_MEASUREMENT_LIST_ROUTE = '/kafka/measurements_list'
else:
    CREATE_MEASUREMENT_URL = 'https://' + HOST + '/measurements'
    CREATE_MEASUREMENT_ROUTE = '/measurements'
    CREATE_MEASUREMENT_LIST_ROUTE = '/measurements_list'

iteration = 0
post_iteration = 0


def get_time(time_string):
    date_object = datetime.strptime(time_string, '%m/%d/%Y %H:%M:%S.%f')
    return long(time.mktime(date_object.timetuple())
                * 1e3 + date_object.microsecond / 1e3)


def add_time(time_string, delta):
    date_object = datetime.strptime(
        time_string, '%m/%d/%Y %H:%M:%S.%f') + timedelta(seconds=delta)
    return long(time.mktime(date_object.timetuple())
                * 1e3 + date_object.microsecond / 1e3)


current_time_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")
current_time = get_time(current_time_string)


#####################
# EDIT THIS SECTION #
#####################

# Replace quoted string with your Personal Access Token value (REQUIRED)
ACCESS_TOKEN = 'Personal Access Token'

# Modify default values (OPTIONAL)
COMPANY = 'EpiData'
SITE = 'San_Jose'
STATION = 'WSN-1'


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
# conn.request.verify=False
conn.request('POST', AUTHENTICATION_ROUTE, json_body, json_header)
post_response = conn.getresponse()

# Check that the response's HTTP response code is 200 (OK).
assert post_response.status == 200

# Parse the JSON response.
response_json = json.loads(post_response.read())

# Retrieve the new session id from the JSON response.
session_id = response_json['sessionId']

# Construct the session cookie.
session_cookie = 'epidata=' + session_id


############################################
# Generate and Ingest Data in a While Loop #
############################################

print "Generating Sample Sensor Data..."
while (True):
    try:

        # Construct an empty list of measurement objects
        measurement_list = []

        ####################################
        # Simulate Temperature Measurement #
        ####################################

        # Simulate Temperate measurement data
        meas_value = round(random.normalvariate(70, 6), 2)
        if (30 <= meas_value <= 110):
            meas_status = 'PASS'
        else:
            meas_status = 'FAIL'

        # Construct measurement object with data to be ingested.
        measurement = {
            'company': COMPANY,
            'site': SITE,
            'station': STATION,
            'sensor': 'Temperature_Probe',
            'ts': current_time,
            'event': 'none',
            'meas_name': 'Temperature',
            'meas_value': meas_value,
            'meas_unit': 'deg F',
            'meas_datatype': 'double',
            'meas_status': meas_status,
            'meas_lower_limit': 30,
            'meas_upper_limit': 110,
            'meas_description': ''
        }

        # Construct measurement list with data to be ingested.
        measurement_list.append(measurement)

        ###################################
        # Simulate Wind Speed Measurement #
        ###################################

        # Simulate Wind Speed measurement data
        meas_value = round(random.normalvariate(8, 2), 2)
        if (0 < meas_value < 25):
            meas_status = 'PASS'
        else:
            meas_status = 'FAIL'

        # Construct measurement object with data to be ingested.
        measurement = {
            'company': COMPANY,
            'site': SITE,
            'station': STATION,
            'sensor': 'Anemometer',
            'ts': current_time,
            'event': 'none',
            'meas_name': 'Wind_Speed',
            'meas_value': meas_value,
            'meas_unit': 'mph',
            'meas_datatype': 'double',
            'meas_status': meas_status,
            'meas_lower_limit': 0,
            'meas_upper_limit': 25,
            'meas_description': ''
        }

        # Construct measurement list with data to be ingested.
        measurement_list.append(measurement)

        ##########################################
        # Simulate Relative Humidity Measurement #
        ##########################################

        # Simulate Relative Humidity measurement data
        meas_value = round(random.normalvariate(60, 5), 2)
        if (0 <= meas_value <= 100):
            meas_status = 'PASS'
        else:
            meas_status = 'FAIL'

        # Construct measurement object with data to be ingested.
        measurement = {
            'company': COMPANY,
            'site': SITE,
            'station': STATION,
            'sensor': 'RH_Probe',
            'ts': current_time,
            'event': 'none',
            'meas_name': 'Relative_Humidity',
            'meas_value': meas_value,
            'meas_unit': '%',
            'meas_datatype': 'double',
            'meas_status': meas_status,
            'meas_lower_limit': 0,
            'meas_upper_limit': 100,
            'meas_description': ''
        }

        # Construct measurement list with data to be ingested.
        measurement_list.append(measurement)

        ###########################
        # Ingest All Measurements #
        ###########################

        # Measurements are created by sending an HTTP POST request to the create url.
        url = CREATE_MEASUREMENT_URL

        # Request headers add parameters to the request.
        json_header = {'Content-type': 'application/json', 'Cookie': session_cookie}

        # Construct JSON body with data to be ingested.
        json_body = json.dumps(measurement_list)

        # Send the POST request and receive the HTTP response.
        conn.request('POST', CREATE_MEASUREMENT_LIST_ROUTE, json_body, json_header)
        post_response = conn.getresponse()

        # Check that the response's HTTP response code is 201 (CREATED).
        assert post_response.status == 201
        
        post_response.read()

        # Print measurement details
        print json_body + "\n"

        ####################################
        # Increment iteration and continue #
        ####################################

        # increment iteration and current time
        time.sleep(1)
        iteration += 1
        current_time_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")
        current_time = get_time(current_time_string)

    # Handle keyboard interrupt
    except (KeyboardInterrupt, SystemExit):
        print '\n...Program Stopped Manually!'
        raise


################################
# End of Data Ingestion Script #
################################
