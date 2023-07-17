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
AUTHENTICATION_URL = 'https://' + HOST + '/authenticate/deviceApp'

STREAM_ENABLED = True

if STREAM_ENABLED:
    CREATE_MEASUREMENT_URL = 'https://' + HOST + '/stream/measurements'
else:
    CREATE_MEASUREMENT_URL = 'https://' + HOST + '/measurements'

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

# Replace quoted string with your application API Token
DEVICE_ID = args.device_id or 'iot_device_1'
DEVICE_TOKEN = args.device_token or 'epidata_123'

# Modify default values (OPTIONAL)
COMPANY = 'EpiData'
SITE = 'San_Francisco'
STATION = 'WSN-1'

#####################################
# Disable SSL verification warnings #
#####################################

requests.packages.urllib3.disable_warnings()

#############################
# Authenticate with EpiData #
#############################

# Create session object for HTTPS requests
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

############################################
# Generate and Ingest Data in a While Loop #
############################################

print("Generating Sample Sensor Data...")
while (True):

    try:
        # Initialize simulated measurement values
        meas_last_temperature_value = 70.0
        meas_last_windspeed_value = 8
        meas_last_rh_value = 60

        for iteration in range(1, 16):

            # Construct an empty list of measurement objects
            measurement_list = []

            current_time_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")
            current_time = get_time(current_time_string)

            ####################################
            # Simulate Temperature Measurement #
            ####################################

            # Simulate Temperate measurement data
            if ((iteration % 11 == 0)):
                # simulate anomalous measurement
                meas_value_diff = float(round(random.normalvariate(5.0, 0.25), 2))
                meas_value = meas_last_temperature_value + meas_value_diff
            elif (iteration % 7 == 0):
                # simulate missing measurement
                meas_value = None
            else:
                # simulate "normal" measurement
                meas_value_diff = float(round(random.normalvariate(0.5, 0.25), 2))
                if((iteration % 2) == 0):
                    meas_value = meas_last_temperature_value + meas_value_diff
                else:
                    meas_value = meas_last_temperature_value - meas_value_diff
                meas_last_temperature_value = meas_value

            if ((meas_value is not None) and (30 <= meas_value <= 120)):
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
                'meas_lower_limit': 10.1,
                'meas_upper_limit': 120.4,
                'meas_description': ''
            }

            # Construct measurement list with data to be ingested.
            measurement_list.append(measurement)

            ###################################
            # Simulate Wind Speed Measurement #
            ###################################

            # Simulate Wind Speed measurement data
            if (iteration % 11 == 0):
                # simulate anomalous measurement
                meas_value = float(30.0)
            elif (iteration % 7 == 0):
                # simulate missing measurement
                meas_value = None
            else:
                # simulate "normal" measurement
                meas_value = float(round(random.normalvariate(8, 2), 2))
                meas_last_windspeed_value = meas_value

            if ((meas_value is not None) and (0 < meas_value < 25)):
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
            meas_value = float(round(random.normalvariate(60, 5), 2))
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
                'meas_lower_limit': None,
                'meas_upper_limit': None,
                'meas_description': ''
            }

            # Construct measurement list with data to be ingested.
            measurement_list.append(measurement)
            meas_last_rh_value = meas_value

            # Pause and continue
            time.sleep(0.1)

            ###########################
            # Ingest All Measurements #
            ###########################

            # Measurements are created by sending an HTTPS POST request to the platform
            # create url.
            url = CREATE_MEASUREMENT_URL

            # Request headers add parameters to the request.
            json_header = {
                'Content-type': 'application/json',
                'device_jwt': json_web_token
                }

            # Construct JSON body with data to be ingested.
            json_body = json.dumps(measurement_list)

            # Send the POST request and receive the HTTPS response.
            req = requests.Request('POST', CREATE_MEASUREMENT_URL, data=json_body, headers=json_header)
            prepped = session.prepare_request(req)
            resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

            # Check that the response's HTTPS response code is 201 (CREATED).
            assert resp.status_code == 201

            # Print measurement details
            print("iteration: ", iteration)
            print(json_body + "\n")

    # Handle keyboard interrupt
    except (KeyboardInterrupt, SystemExit):
        print("\nStream Generation Interrupted!")
        raise

    # Exit the while loop
    break

################################
# End of Data Ingestion Script #
################################
