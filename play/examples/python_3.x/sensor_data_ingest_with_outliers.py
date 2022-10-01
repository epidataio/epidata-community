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
args = arg_parser.parse_args()

HOST = args.host or '127.0.0.1:9443'
AUTHENTICATION_URL = 'https://' + HOST + '/authenticate/app'
AUTHENTICATION_ROUTE = '/authenticate/app'
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

# Modify default values (OPTIONAL)
COMPANY = 'EpiData'
SITE = 'San_Francisco'
STATION = 'WSN-1'


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
json_header = {'Content-type': 'application/json', 'Set-Cookie': "epidata"}

# The access token is povided via JSON.
json_body = json.dumps({'accessToken': ACCESS_TOKEN})

# Send the POST request and receive the HTTP response.
req = requests.Request('POST', AUTHENTICATION_URL, data=json_body, headers=json_header)
prepped = session.prepare_request(req)
resp = session.send(prepped, stream=None, verify=None, proxies=None, cert=None, timeout=None)

# Check that the response's HTTP response code is 200 (OK).
assert resp.status_code == 200

# Parse the JSON response.
post_response = json.loads(resp.content)
print("response - ", post_response)


############################################
# Generate and Ingest Data in a While Loop #
############################################

print("Generating Sample Sensor Data...")
while (True):

    try:

        meas_last_temperature_value = 70.0
        meas_last_windspeed_value = 8
        meas_last_rh_value = 60

        for data_iteration in range(1, 2):

            # Construct an empty list of measurement objects
            measurement_list = []

            for log_iteration in range(1, 2):

                current_time_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")
                current_time = get_time(current_time_string)

                ####################################
                # Simulate Temperature Measurement #
                ####################################

                # Simulate Temperate measurement data
                if ((data_iteration % 4 == 0) and (log_iteration == 2)):
                    if(data_iteration <= 12):
                        meas_value_diff = float(round(random.normalvariate(3.0, 0.25), 2))
                        meas_value = meas_last_temperature_value + meas_value_diff
                    else:
                        meas_value_diff = float(round(random.normalvariate(3.0, 0.25), 2))
                        meas_value = meas_last_temperature_value - meas_value_diff
                elif ((data_iteration % 6 == 0) and (log_iteration == 3)):
                    meas_value = None
                else:
                    if(data_iteration <= 12):
                        meas_value_diff = float(round(random.normalvariate(0.5, 0.25), 2))
                        meas_value = meas_last_temperature_value + meas_value_diff
                    else:
                        meas_value_diff = float(round(random.normalvariate(0.5, 0.25), 2))
                        meas_value = meas_last_temperature_value - meas_value_diff
                    meas_last_temperature_value = meas_value
                if (30 <= meas_value <= 120):
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
                if ((data_iteration % 4 == 0) and (log_iteration == 2)):
                    meas_value = float(30)
                elif ((data_iteration % 21 == 0) and (log_iteration == 3)):
                    meas_value = None
                else:
                    meas_value = float(round(random.normalvariate(8, 2), 2))
                    meas_last_windspeed_value = meas_value
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
                meas_value = float(round(random.normalvariate(60, 5), 2))
                if (0 <= meas_value <= 100):
                    meas_status = 'PASS'
                else:
                    meas_status = 'PASS'

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

                ####################################
                # Increment iteration and continue #
                ####################################

                # increment iteration and current time
                time.sleep(0.1)

            ###########################
            # Ingest All Measurements #
            ###########################

            # Measurements are created by sending an HTTP POST request to the platform
            # create url.
            url = CREATE_MEASUREMENT_URL

            # Request headers add parameters to the request.
            json_header = {
                'Content-type': 'application/json'
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

            # Print measurement details
            print("iteration: ", data_iteration)
            print(json_body + "\n")

            #break

        break

    # Handle keyboard interrupt
    except (KeyboardInterrupt, SystemExit):
        print("\n...Program Stopped Manually!")
        raise


################################
# End of Data Ingestion Script #
################################
