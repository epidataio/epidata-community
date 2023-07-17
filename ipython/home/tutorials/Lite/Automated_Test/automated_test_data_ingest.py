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
arg_parser.add_argument('--ate_id')
arg_parser.add_argument('--ate_token')
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

# Replace quoted string with your API Token
DEVICE_ID = args.ate_id or 'ate_device_1'
DEVICE_TOKEN = args.ate_token or 'epidata_456'

# Modify default values (OPTIONAL)
COMPANY = 'EpiData'
SITE = 'San_Jose'

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
# post_response = json.loads(resp.content)

####################################################
# Generate and ingest Automated Test measurements. #
####################################################

print("Generating Sample Automated Test Data...")
while (True):

    try:
        # Initialize simulated measurement values
        meas_last_temperature_value = 100.0
        meas_last_voltage_value = 5.0
        meas_last_current_value = 100

        for iteration in range(1, 16):

            # Construct an empty list of measurement objects
            measurement_list = []

            current_time_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")
            current_time = get_time(current_time_string)

            ######################################################
            # Simulate a Temperature Measurement on Equipment 1. #
            ######################################################

            # Simulate temperature measurement
            if ((iteration % 11 == 0)):
                # simulate anomalous measurement
                meas_value_diff = float(round(random.normalvariate(10.0, 0.25), 2))
                meas_value = meas_last_temperature_value + meas_value_diff
            elif (iteration % 7 == 0):
                # simulate missing measurement
                meas_value = None
            else:
                # simulate "normal" measurement
                meas_value_diff = float(round(random.normalvariate(0.5, 0.05), 2))
                if((iteration % 2) == 0):
                    meas_value = meas_last_temperature_value + meas_value_diff
                else:
                    meas_value = meas_last_temperature_value - meas_value_diff
                meas_last_temperature_value = meas_value

            if ((meas_value is not None) and (90 <= meas_value <= 110)):
                meas_status = 'PASS'
            else:
                meas_status = 'FAIL'

            # Construct measurement object with automated test data.
            measurement = {
                'company': COMPANY,
                'site': SITE,
                'device_group': 'DEV_GROUP_1',
                'tester': 'ATE-1',
                'ts': current_time,
                'device_name': str('DEV_') + str(1000+iteration),
                'test_name': 'Temperature_Test',
                'meas_name': 'Temperature_1',
                'meas_value': meas_value,
                'meas_datatype': 'double',
                'meas_unit': 'degree C',
                'meas_status': meas_status,
                'meas_lower_limit': 40.0,
                'meas_upper_limit': 90.0,
                'meas_description': '',
                'device_status': 'PASS',
                'test_status': 'PASS'
            }

            # Construct measurement list with data to be ingested.
            measurement_list.append(measurement)

            ##################################################
            # Simulate a Voltage Measurement on Equipment 1. #
            ##################################################

            # Simulate voltage measurement
            if ((iteration % 11 == 0)):
                # simulate anomalous measurement
                meas_value_diff = float(round(random.normalvariate(1.0, 0.25), 2))
                meas_value = meas_last_voltage_value + meas_value_diff
            elif (iteration % 7 == 0):
                # simulate missing measurement
                meas_value = None
            else:
                # simulate "normal" measurement
                meas_value_diff = float(round(random.normalvariate(0.025, 0.01), 2))
                if((iteration % 2) == 0):
                    meas_value = meas_last_voltage_value + meas_value_diff
                else:
                    meas_value = meas_last_voltage_value - meas_value_diff
                meas_last_voltage_value = meas_value

            if ((meas_value is not None) and (4.9 <= meas_value <= 5.1)):
                meas_status = 'PASS'
            else:
                meas_status = 'FAIL'

            measurement = {
                'company': COMPANY,
                'site': SITE,
                'device_group': 'DEV_GROUP_1',
                'tester': 'ATE-1',
                'ts': current_time,
                'device_name': str('DEV_') + str(1000+iteration),
                'test_name': 'Voltage_Test',
                'meas_name': 'Voltage_1',
                'meas_value': meas_value,
                'meas_datatype': 'double',
                'meas_unit': 'V',
                'meas_status': meas_status,
                'meas_lower_limit': 4.9,
                'meas_upper_limit': 5.1,
                'meas_description': '',
                'device_status': 'PASS',
                'test_status': 'PASS'
            }

            # Construct measurement list with data to be ingested.
            measurement_list.append(measurement)

            ##################################################
            # Simulate a Current Measurement on Equipment 1. #
            ##################################################

            # Simulate current measurement
            if ((iteration % 11 == 0)):
                # simulate anomalous measurement
                meas_value_diff = float(round(random.normalvariate(10.0, 0.25), 2))
                meas_value = meas_last_current_value + meas_value_diff
            elif (iteration % 7 == 0):
                # simulate missing measurement
                meas_value = None
            else:
                # simulate "normal" measurement
                meas_value_diff = float(round(random.normalvariate(0.5, 0.05), 2))
                if((iteration % 2) == 0):
                    meas_value = meas_last_current_value + meas_value_diff
                else:
                    meas_value = meas_last_current_value - meas_value_diff
                meas_last_current_value = meas_value

            if ((meas_value is not None) and (90.0 <= meas_value <= 110.0)):
                meas_status = 'PASS'
            else:
                meas_status = 'FAIL'

            measurement = {
                'company': COMPANY,
                'site': SITE,
                'device_group': 'DEV_GROUP_1',
                'tester': 'ATE-1',
                'ts': current_time,
                'device_name': str('DEV_') + str(1000+iteration),
                'test_name': 'Current_Test',
                'meas_name': 'Current_1',
                'meas_value': meas_value,
                'meas_datatype': 'double',
                'meas_unit': 'mA',
                'meas_status': meas_status,
                'meas_lower_limit': 110.0,
                'meas_upper_limit': 90.0,
                'meas_description': '',
                'device_status': 'PASS',
                'test_status': 'PASS'
            }

            # Construct measurement list with data to be ingested.
            measurement_list.append(measurement)

            # Pause and continue
            time.sleep(0.1)

            ###################################
            # Ingest Equipment 1 Measurements #
            ###################################

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
            print("iteration: ", iteration, "; Automated Test Equipment 1")
            print(json_body + "\n")

            ######################################################
            # Simulate a Temperature Measurement on Equipment 2. #
            ######################################################

            # Construct an empty list of measurement objects
            measurement_list = []

            current_time_string = datetime.now().strftime("%m/%d/%Y %H:%M:%S.%f")
            current_time = get_time(current_time_string)

            # Simulate temperature measurement
            if ((iteration % 11 == 0)):
                # simulate anomalous measurement
                meas_value_diff = float(round(random.normalvariate(10.0, 0.25), 2))
                meas_value = meas_last_temperature_value + meas_value_diff
            elif (iteration % 7 == 0):
                # simulate missing measurement
                meas_value = None
            else:
                # simulate "normal" measurement
                meas_value_diff = float(round(random.normalvariate(0.5, 0.05), 2))
                if((iteration % 2) == 0):
                    meas_value = meas_last_temperature_value + meas_value_diff
                else:
                    meas_value = meas_last_temperature_value - meas_value_diff
                meas_last_temperature_value = meas_value

            if ((meas_value is not None) and (90 <= meas_value <= 110)):
                meas_status = 'PASS'
            else:
                meas_status = 'FAIL'

            # Construct measurement object with automated test data.
            measurement = {
                'company': COMPANY,
                'site': SITE,
                'device_group': 'DEV_GROUP_1',
                'tester': 'ATE-2',
                'ts': current_time,
                'device_name': str('DEV_') + str(1000+iteration),
                'test_name': 'Temperature_Test',
                'meas_name': 'Temperature_2',
                'meas_value': meas_value,
                'meas_datatype': 'double',
                'meas_unit': 'degree C',
                'meas_status': meas_status,
                'meas_lower_limit': 40.0,
                'meas_upper_limit': 90.0,
                'meas_description': '',
                'device_status': 'PASS',
                'test_status': 'PASS'
            }

            # Construct measurement list with data to be ingested.
            measurement_list.append(measurement)

            ##################################################
            # Simulate a Voltage Measurement on Equipment 2. #
            ##################################################

            # Simulate voltage measurement
            if ((iteration % 11 == 0)):
                # simulate anomalous measurement
                meas_value_diff = float(round(random.normalvariate(1.0, 0.25), 2))
                meas_value = meas_last_voltage_value + meas_value_diff
            elif (iteration % 7 == 0):
                # simulate missing measurement
                meas_value = None
            else:
                # simulate "normal" measurement
                meas_value_diff = float(round(random.normalvariate(0.025, 0.01), 2))
                if((iteration % 2) == 0):
                    meas_value = meas_last_voltage_value + meas_value_diff
                else:
                    meas_value = meas_last_voltage_value - meas_value_diff
                meas_last_voltage_value = meas_value

            if ((meas_value is not None) and (4.9 <= meas_value <= 5.1)):
                meas_status = 'PASS'
            else:
                meas_status = 'FAIL'

            measurement = {
                'company': COMPANY,
                'site': SITE,
                'device_group': 'DEV_GROUP_1',
                'tester': 'ATE-2',
                'ts': current_time,
                'device_name': str('DEV_') + str(1000+iteration),
                'test_name': 'Voltage_Test',
                'meas_name': 'Voltage_2',
                'meas_value': meas_value,
                'meas_datatype': 'double',
                'meas_unit': 'V',
                'meas_status': meas_status,
                'meas_lower_limit': 4.9,
                'meas_upper_limit': 5.1,
                'meas_description': '',
                'device_status': 'PASS',
                'test_status': 'PASS'
            }

            # Construct measurement list with data to be ingested.
            measurement_list.append(measurement)

            ##################################################
            # Simulate a Current Measurement on Equipment 2. #
            ##################################################

            # Simulate current measurement
            if ((iteration % 11 == 0)):
                # simulate anomalous measurement
                meas_value_diff = float(round(random.normalvariate(10.0, 0.25), 2))
                meas_value = meas_last_current_value + meas_value_diff
            elif (iteration % 7 == 0):
                # simulate missing measurement
                meas_value = None
            else:
                # simulate "normal" measurement
                meas_value_diff = float(round(random.normalvariate(0.5, 0.05), 2))
                if((iteration % 2) == 0):
                    meas_value = meas_last_current_value + meas_value_diff
                else:
                    meas_value = meas_last_current_value - meas_value_diff
                meas_last_current_value = meas_value

            if ((meas_value is not None) and (90.0 <= meas_value <= 110.0)):
                meas_status = 'PASS'
            else:
                meas_status = 'FAIL'

            measurement = {
                'company': COMPANY,
                'site': SITE,
                'device_group': 'DEV_GROUP_1',
                'tester': 'ATE-2',
                'ts': current_time,
                'device_name': str('DEV_') + str(1000+iteration),
                'test_name': 'Current_Test',
                'meas_name': 'Current_2',
                'meas_value': meas_value,
                'meas_datatype': 'double',
                'meas_unit': 'mA',
                'meas_status': meas_status,
                'meas_lower_limit': 110.0,
                'meas_upper_limit': 90.0,
                'meas_description': '',
                'device_status': 'PASS',
                'test_status': 'PASS'
            }

            # Construct measurement list with data to be ingested.
            measurement_list.append(measurement)

            # Pause and continue
            time.sleep(0.1)

            ###################################
            # Ingest Equipment 2 Measurements #
            ###################################

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
            print("iteration: ", iteration, "; Automated Test Equipment 2")
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
