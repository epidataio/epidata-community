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
<<<<<<< Updated upstream:play/examples/sensor_data_with_outliers.py
import urllib2
=======
import requests
import urllib3

>>>>>>> Stashed changes:play/examples/sensor_data_ingest_with_outliers.py


##################################
# Define Variables and Functions #
##################################

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--host')
args = arg_parser.parse_args()

HOST = args.host or '127.0.0.1'
AUTHENTICATION_URL = 'https://' + HOST + '/authenticate/app'
AUTHENTICATION_ROUTE = '/authenticate/app'
USE_KAFKA = False
LOG_ITERATION = 1

if USE_KAFKA:
    CREATE_MEASUREMENT_URL = 'https://' + HOST + '/kafka/measurements'
    CREATE_MEASUREMENT_LIST_ROUTE = '/kafka/measurements'
else:
    CREATE_MEASUREMENT_URL = 'https://' + HOST + '/measurements'
    CREATE_MEASUREMENT_LIST_ROUTE = '/measurements'

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

# Replace quoted string with API Token or GitHub Personal Access Token (REQUIRED)
ACCESS_TOKEN = 'API TOKEN'

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

<<<<<<< Updated upstream:play/examples/sensor_data_with_outliers.py
        for data_iteration in range(1, 6):
=======
        meas_last_temperature_value = 70.0
        meas_last_windspeed_value = 8
        meas_last_rh_value = 60

        for data_iteration in range(1, 3):
>>>>>>> Stashed changes:play/examples/sensor_data_ingest_with_outliers.py

            # Construct an empty list of measurement objects
            measurement_list = []

<<<<<<< Updated upstream:play/examples/sensor_data_with_outliers.py
            for log_iteration in range(1, 4):
=======
            for log_iteration in range(1, 3):
>>>>>>> Stashed changes:play/examples/sensor_data_ingest_with_outliers.py

                current_time_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")
                current_time = get_time(current_time_string)

                ####################################
                # Simulate Temperature Measurement #
                ####################################

                # Simulate Temperate measurement data
                if ((data_iteration % 3 == 0) and (log_iteration == 2)):
                    meas_value = 250
                elif ((data_iteration % 2 == 0) and (log_iteration == 2)):
                    meas_value = None
                else:
                    meas_value = round(random.normalvariate(70, 6), 2)
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
                    'meas_lower_limit': 10,
                    'meas_upper_limit': 120,
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
                    'meas_lower_limit': 0,
                    'meas_upper_limit': 100,
                    'meas_description': ''
                }

                # Construct measurement list with data to be ingested.
                measurement_list.append(measurement)
<<<<<<< Updated upstream:play/examples/sensor_data_with_outliers.py
=======
                meas_last_rh_value = meas_value
>>>>>>> Stashed changes:play/examples/sensor_data_ingest_with_outliers.py

                ####################################
                # Increment iteration and continue #
                ####################################

                # increment iteration and current time
                time.sleep(0.1)
                iteration += 1

            ###########################
            # Ingest All Measurements #
            ###########################

            # Measurements are created by sending an HTTP POST request to the
            # create url.
            url = CREATE_MEASUREMENT_URL

            # Request headers add parameters to the request.
            json_header = {
                'Content-type': 'application/json',
                'Cookie': session_cookie}

            # Construct JSON body with data to be ingested.
            print measurement_list
            json_body = json.dumps(measurement_list)

            # Send the POST request and receive the HTTP response.
            conn.request(
                'POST',
                CREATE_MEASUREMENT_LIST_ROUTE,
                json_body,
                json_header)
            post_response = conn.getresponse()

            # Check that the response's HTTP response code is 201 (CREATED).
<<<<<<< Updated upstream:play/examples/sensor_data_with_outliers.py
            status = post_response.status
            message = post_response.read()
            assert status == 201

            # post_response.read()
=======
            assert resp.status_code == 201
>>>>>>> Stashed changes:play/examples/sensor_data_ingest_with_outliers.py

            # Print measurement details
            print "iteration: ", data_iteration
            # print json_body + "\n"

        break

    # Handle keyboard interrupt
    except (KeyboardInterrupt, SystemExit):
        print '\n...Program Stopped Manually!'
        raise


################################
# End of Data Ingestion Script #
################################
