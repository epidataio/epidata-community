############################
# Import Required Modules #
############################

import argparse
import base64
from epidata_common.data_types import Waveform
from datetime import datetime
import json
import numpy as np
import struct
import time
import urllib2


##################################
# Define Variables and Functions #
##################################

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument('--host')
arg_parser.add_argument('--access_token')
args = arg_parser.parse_args()

HOST = args.host or '127.0.0.1'
ACCESS_TOKEN = args.access_token

AUTHENTICATION_URL = 'https://' + HOST + '/api/authenticate/github'
CREATE_MEASUREMENT_URL = 'https://' + HOST + '/measurements'


def get_current_time():
    now = datetime.now()
    return long(time.mktime(now.timetuple()) * 1e3 + now.microsecond / 1e3)


##############################
# AUTHENTICATE with epidata. #
##############################

# Authentication is achieved by posting to the AUTHENTICATION_URL.
url = AUTHENTICATION_URL

# An HTTP POST with JSON content requires the HTTP Content-type header.
json_header = {'Content-type': 'application/json'}

# The access token is povided via JSON.
json_body = json.dumps({'accessToken': ACCESS_TOKEN})

# Construct the POST request.
post_request = urllib2.Request(url, headers=json_header, data=json_body)

# Send the POST request and receive the HTTP response.
post_response = urllib2.urlopen(post_request)

# Check that the response's HTTP response code is 200 (OK).
assert post_response.getcode() == 200

# Parse the JSON response.
response_json = json.loads(post_response.read())

# Retrieve the new session id from the JSON response.
session_id = response_json['sessionId']

# Construct the session cookie.
session_cookie = 'epidata=' + session_id


#####################################################
# CREATE an automated test temperature measurement. #
#####################################################

# Measurements are created by sending an HTTP POST request to the create url.
url = CREATE_MEASUREMENT_URL

# Request headers add parameters to the request.
headers = {

    # An HTTP POST with JSON content requires the HTTP Content-type header.
    'Content-type': 'application/json',

    # The session cookie serves as an authentication credential.
    'Cookie': session_cookie
}

# The measurement data is assembled in a python dictionary and converted
# to JSON.
json_body = json.dumps([{
    'company': 'Company-1',
    'site': 'Site-1',
    'device_group': '1000',
    'tester': 'Station-1',

    # 'ts' is a 64 bit integer representing a timestamp in milliseconds.
    'ts': get_current_time(),
    'device_name': '100001',
    'test_name': 'Test-1',
    'meas_name': 'Meas-1',

    # The meas_value field can contain a number or a text string, depending on
    # the measurement data type.
    'meas_value': 45.7,

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

# Construct the POST request.
post_request = urllib2.Request(url, headers=headers, data=json_body)

# Send the POST request and receive the HTTP response.
post_response2 = urllib2.urlopen(post_request)

# Check that the response's HTTP response code is 201 (CREATED).
assert post_response2.getcode() == 201


##################################################
# CREATE an automated test power on measurement. #
##################################################

url = CREATE_MEASUREMENT_URL
headers = {'Content-type': 'application/json', 'Cookie': session_cookie}
json_body = json.dumps([{
    'company': 'Company-2',
    'site': 'Site-2',
    'device_group': '2000',
    'tester': 'Station-2',
    'ts': get_current_time(),
    'device_name': '200002',
    'test_name': 'Power On Test',
    'meas_name': 'Power On Test',

    # The meas_value field can contain a text string.
    'meas_value': 'Device Powered On Successfully',

    # The meas_datatype field is set to 'string' for string values.
    'meas_datatype': 'string',
    'meas_status': 'PASS',
    'meas_description': '',
    'device_status': 'PASS',
    'test_status': 'PASS'
}])
post_request = urllib2.Request(url, headers=headers, data=json_body)
post_response = urllib2.urlopen(post_request)
assert post_response.getcode() == 201


##########################################################
# CREATE an automated test voltage waveform measurement. #
##########################################################

# Prepare a voltage waveform.
waveform = Waveform(
    start_time=datetime.now(),
    sampling_rate=0.01,
    samples=np.sin(np.arange(0.0, 1.0, 0.01)))

# Pack the waveform into a binary string.
waveform_data = struct.pack(

    # Specify a format code for creating a packed binary string.
    '!' +  # Specify network byte order.
    'q' +  # One long long integer (the waveform timestamp).
    'd' +  # One double precision floating point value (the sampling rate).
    # 100 double precision floating point values (the waveform samples).
    '100d',

    # Now provide the values described in the format string above.

    # The waveform timestamp.
    long(time.mktime(waveform.start_time.timetuple()) * 1e3 + \
         waveform.start_time.microsecond / 1e3),

    # The sampling rate.
    waveform.sampling_rate,

    # The waveform samples.
    *waveform.samples)

url = CREATE_MEASUREMENT_URL
headers = {'Content-type': 'application/json', 'Cookie': session_cookie}
json_body = json.dumps([{
    'company': 'Company-3',
    'site': 'Site-3',
    'device_group': '3000',
    'tester': 'Station-3',
    'ts': get_current_time(),
    'device_name': '300003',
    'test_name': 'Test-3',
    'meas_name': 'Meas-3',

    # A binary measurement value must be converted to an ascii text string using
    # base64 encoding.
    'meas_value': base64.b64encode(waveform_data),

    # The meas_datatype field indicates the type of binary data provided (in this
    # case a waveform).
    'meas_datatype': 'waveform',
    'meas_unit': 'V',
    'meas_status': 'PASS',
    'meas_description': '',
    'device_status': 'PASS',
    'test_status': 'PASS'
}])
post_request = urllib2.Request(url, headers=headers, data=json_body)
post_response = urllib2.urlopen(post_request)
assert post_response.getcode() == 201
