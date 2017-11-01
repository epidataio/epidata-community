#
# Copyright (c) 2015-2017 EpiData, Inc.
#

from datetime import datetime, timedelta
from epidata.context import ec
from epidata.sensor_measurement import SensorMeasurement
from epidata.utils import ConvertUtils
from epidata.analytics import *
import numpy
from pyspark.sql import Row
from pyspark.sql import Column
from pyspark.tests import ReusedPySparkTestCase as PySparkTestCase
import unittest


json_string = []

json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "meas_value": 64.76, "meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-5"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "meas_value": 64.76, "meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-6"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "meas_value": 64.76, "sensor": "tester-8"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "meas_value": 64, "meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-9"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "meas_value": 64, "meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-10"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "meas_value": 64, "sensor": "tester-12"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "meas_value": "64", "meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-13"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "meas_value": "64", "meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-14"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "meas_value": "64", "sensor": "tester-16"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-17"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-18"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "double", "sensor": "tester-20"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","meas_value": 64.5,"meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-21"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","meas_value": 64.5,"meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-22"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","meas_value": 64.5,"sensor": "tester-24"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","meas_value": 64,"meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-25"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","meas_value": 64,"meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-26"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","meas_value": 64,"sensor": "tester-28"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","meas_value": "64","meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-29"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","meas_value": "64","meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-30"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","meas_value": "64","sensor": "tester-32"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-33"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-34"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "long","sensor": "tester-36"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","meas_value": 64.5,"meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-37"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","meas_value": 64.5,"meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-38"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","meas_value": 64.5,"sensor": "tester-40"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","meas_value": 64,"meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-41"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","meas_value": 64,"meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-42"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","meas_value": 64,"sensor": "tester-44"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","meas_value": "64","meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-45"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","meas_value": "64","meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-46"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","meas_value": "64","sensor": "tester-48"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-49"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-50"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "", "meas_datatype": "string","sensor": "tester-52"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","meas_value": 64.5,"meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-53"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","meas_value": 64.5,"meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-54"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","meas_value": 64.5,"sensor": "tester-56"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","meas_value": 64,"meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-57"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","meas_value": 64,"meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-58"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","meas_value": 64,"sensor": "tester-60"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","meas_value": "64","meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-61"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","meas_value": "64","meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-62"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","meas_value": "64","sensor": "tester-64"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","meas_lower_limit": -30.2, "meas_upper_limit": 200.2, "sensor": "tester-65"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","meas_lower_limit": -30, "meas_upper_limit": 200, "sensor": "tester-66"}')
json_string.append('{"meas_name": "Temperature", "company": "company0", "site": "site0", "station": "station-1", "test_name": "Temperature_Test", "meas_status": "PASS", "ts": 1505970910038, "event": "Device-1", "meas_unit": "deg F", "meas_description": "","sensor": "tester-68"}')


class Base(unittest.TestCase):

    def assertEqualRows(self, one, two):
        if not isinstance(one, Column):
            self.assertEqual(one.asDict(), two.asDict())

    def assertEqualDataFrames(self, one, two):
        self.assertEqual(one.count(), two.count())
        for i, j in zip(one, two):
            self.assertEqualRows(i, j)


class EpidataContextStremingTestsSensorMeasurement(Base):

    def test_sensor_measurement_streaming(self):

        def num_keys_different(dict1, dict2):
            diffkeys = [
                k for k in dict1 if k not in dict2 or dict1[k] != dict2[k]]
            return len(diffkeys)

        def create_map_from_panda_dataframe(dataframe, index_column):
            dataframe_map = {}
            for index, row in dataframe.iterrows():
                index_str = "-" + row[index_column].split("-")[1]

                if isinstance(
                    row['meas_value'],
                    basestring) or not np.isnan(
                        row['meas_value']):
                    dataframe_map['meas_value' + index_str] = row['meas_value']
                dataframe_map['meas_datatype' +
                              index_str] = row['meas_datatype']

                dataframe_map['meas_upper_limit' +
                              index_str] = row['meas_upper_limit']

                dataframe_map['meas_lower_limit' +
                              index_str] = row['meas_lower_limit']
            return dataframe_map

        def create_map_from_spark_panda_dataframe(dataframe, index_column):
            dataframe_map = {}
            for index, row in dataframe.iterrows():
                index_str = "-" + row[index_column].split("-")[1]

                if isinstance(
                    row['meas_value'],
                    basestring) or not np.isnan(
                        row['meas_value']):
                    dataframe_map['meas_value' + index_str] = row['meas_value']
                dataframe_map['meas_datatype' +
                              index_str] = row['meas_datatype']

                if isinstance(
                    row['meas_upper_limit'],
                    basestring) or not np.isnan(
                        row['meas_upper_limit']):
                    dataframe_map['meas_upper_limit' +
                                  index_str] = row['meas_upper_limit']

                if isinstance(
                    row['meas_lower_limit'],
                    basestring) or not np.isnan(
                        row['meas_lower_limit']):
                    dataframe_map['meas_lower_limit' +
                                  index_str] = row['meas_lower_limit']
            return dataframe_map

        measurements_rows = [SensorMeasurement.to_row(
            [0, x]) for x in json_string]
        rdd_df = ec._sc.parallelize(measurements_rows).toDF()
        panda_df = ConvertUtils.convert_to_pandas_dataframe_model(rdd_df, True)

        panda_df_map = create_map_from_panda_dataframe(panda_df, 'sensor')

        output_panda_map = {
            'meas_lower_limit-46': None,
            'meas_lower_limit-50': -30.0,
            'meas_value-18': '',
            'meas_value-13': '64',
            'meas_value-12': 64.0,
            'meas_value-10': 64.0,
            'meas_value-17': '',
            'meas_value-16': '64',
            'meas_value-14': '64',
            'meas_value-37': 64.5,
            'meas_upper_limit-57': 200.2,
            'meas_upper_limit-29': None,
            'meas_upper_limit-28': None,
            'meas_lower_limit-34': -30.0,
            'meas_lower_limit-33': -30.2,
            'meas_lower_limit-32': None,
            'meas_lower_limit-30': None,
            'meas_upper_limit-21': 200.2,
            'meas_upper_limit-20': None,
            'meas_upper_limit-22': 200.0,
            'meas_upper_limit-25': 200.2,
            'meas_upper_limit-24': None,
            'meas_upper_limit-26': 200.0,
            'meas_lower_limit-9': -30.2,
            'meas_lower_limit-8': None,
            'meas_datatype-48': 'string',
            'meas_datatype-49': 'string',
            'meas_lower_limit-5': -30.2,
            'meas_datatype-44': 'long',
            'meas_lower_limit-6': -30.0,
            'meas_datatype-42': 'long',
            'meas_datatype-40': 'double',
            'meas_datatype-41': 'long',
            'meas_lower_limit-45': None,
            'meas_lower_limit-24': None,
            'meas_lower_limit-25': -30.2,
            'meas_lower_limit-26': -30.0,
            'meas_lower_limit-20': None,
            'meas_lower_limit-21': -30.2,
            'meas_lower_limit-22': -30.0,
            'meas_upper_limit-36': None,
            'meas_upper_limit-37': 200.2,
            'meas_upper_limit-34': 200.0,
            'meas_upper_limit-32': None,
            'meas_lower_limit-29': None,
            'meas_upper_limit-30': None,
            'meas_datatype-50': 'string',
            'meas_datatype-53': 'double',
            'meas_datatype-52': 'string',
            'meas_datatype-54': 'double',
            'meas_datatype-57': 'long',
            'meas_datatype-56': 'double',
            'meas_datatype-58': 'long',
            'meas_upper_limit-49': 200.2,
            'meas_upper_limit-48': None,
            'meas_upper_limit-42': 200.0,
            'meas_upper_limit-41': 200.2,
            'meas_upper_limit-40': None,
            'meas_upper_limit-46': None,
            'meas_upper_limit-45': None,
            'meas_upper_limit-44': None,
            'meas_lower_limit-18': -30.0,
            'meas_lower_limit-10': -30.0,
            'meas_lower_limit-13': None,
            'meas_lower_limit-12': None,
            'meas_lower_limit-14': None,
            'meas_lower_limit-17': -30.2,
            'meas_lower_limit-16': None,
            'meas_upper_limit-38': 200.0,
            'meas_upper_limit-8': None,
            'meas_upper_limit-9': 200.2,
            'meas_upper_limit-6': 200.0,
            'meas_upper_limit-5': 200.2,
            'meas_datatype-24': 'double',
            'meas_datatype-25': 'long',
            'meas_datatype-26': 'long',
            'meas_value-61': '64',
            'meas_datatype-20': 'double',
            'meas_datatype-21': 'double',
            'meas_datatype-22': 'double',
            'meas_value-65': '',
            'meas_value-68': '',
            'meas_datatype-28': 'long',
            'meas_datatype-29': 'string',
            'meas_upper_limit-58': 200.0,
            'meas_value-62': '64',
            'meas_upper_limit-50': 200.0,
            'meas_lower_limit-28': None,
            'meas_upper_limit-52': None,
            'meas_upper_limit-53': 200.2,
            'meas_upper_limit-54': 200.0,
            'meas_value-60': 64.0,
            'meas_upper_limit-56': None,
            'meas_upper_limit-33': 200.2,
            'meas_value-66': '',
            'meas_value-64': '64',
            'meas_value-58': 64.0,
            'meas_value-57': 64.0,
            'meas_value-56': 64.5,
            'meas_value-54': 64.5,
            'meas_value-53': 64.5,
            'meas_value-52': '',
            'meas_value-50': '',
            'meas_datatype-37': 'double',
            'meas_datatype-36': 'long',
            'meas_datatype-34': 'long',
            'meas_datatype-33': 'long',
            'meas_datatype-32': 'string',
            'meas_datatype-30': 'string',
            'meas_datatype-38': 'double',
            'meas_lower_limit-37': -30.2,
            'meas_upper_limit-68': None,
            'meas_lower_limit-36': None,
            'meas_upper_limit-65': 200.2,
            'meas_upper_limit-64': None,
            'meas_upper_limit-66': 200.0,
            'meas_upper_limit-61': None,
            'meas_upper_limit-60': None,
            'meas_upper_limit-62': None,
            'meas_value-48': '64',
            'meas_value-49': '',
            'meas_value-40': 64.5,
            'meas_value-41': 64.0,
            'meas_value-42': 64.0,
            'meas_value-44': 64.0,
            'meas_value-45': '64',
            'meas_value-46': '64',
            'meas_lower_limit-68': None,
            'meas_lower_limit-60': None,
            'meas_lower_limit-61': None,
            'meas_lower_limit-62': None,
            'meas_lower_limit-64': None,
            'meas_lower_limit-65': -30.2,
            'meas_lower_limit-66': -30.0,
            'meas_lower_limit-52': None,
            'meas_lower_limit-38': -30.0,
            'meas_value-38': 64.5,
            'meas_datatype-18': 'double',
            'meas_value-34': '',
            'meas_datatype-14': 'string',
            'meas_datatype-17': 'double',
            'meas_datatype-16': 'string',
            'meas_datatype-10': 'long',
            'meas_datatype-13': 'string',
            'meas_value-32': '64',
            'meas_lower_limit-42': -30.0,
            'meas_value-36': '',
            'meas_lower_limit-58': -30.0,
            'meas_lower_limit-54': -30.0,
            'meas_lower_limit-57': -30.2,
            'meas_lower_limit-56': None,
            'meas_lower_limit-40': None,
            'meas_lower_limit-53': -30.2,
            'meas_value-30': '64',
            'meas_datatype-46': 'string',
            'meas_lower_limit-41': -30.2,
            'meas_value-33': '',
            'meas_upper_limit-10': 200.0,
            'meas_datatype-12': 'long',
            'meas_datatype-68': '',
            'meas_datatype-45': 'string',
            'meas_lower_limit-44': None,
            'meas_datatype-60': 'long',
            'meas_datatype-61': 'string',
            'meas_datatype-62': 'string',
            'meas_datatype-6': 'double',
            'meas_datatype-64': 'string',
            'meas_datatype-65': '',
            'meas_datatype-66': '',
            'meas_value-28': 64.0,
            'meas_value-29': '64',
            'meas_value-26': 64.0,
            'meas_value-24': 64.5,
            'meas_value-25': 64.0,
            'meas_value-22': 64.5,
            'meas_value-20': '',
            'meas_value-21': 64.5,
            'meas_value-9': 64.0,
            'meas_value-8': 64.76,
            'meas_value-6': 64.76,
            'meas_value-5': 64.76,
            'meas_upper_limit-14': None,
            'meas_upper_limit-16': None,
            'meas_upper_limit-17': 200.2,
            'meas_datatype-5': 'double',
            'meas_upper_limit-12': None,
            'meas_upper_limit-13': None,
            'meas_datatype-9': 'long',
            'meas_datatype-8': 'double',
            'meas_lower_limit-48': None,
            'meas_lower_limit-49': -30.2,
            'meas_upper_limit-18': 200.0}

        self.assertEqual(num_keys_different(panda_df_map, output_panda_map), 0)

        op = ec.create_transformation(
            substitute, [
                ["Temperature"], "rolling", 3], "measurements_cleansed_kafka")

        output_df = op.apply(panda_df, None)

        output_df_map = create_map_from_panda_dataframe(output_df, 'sensor')

        expected_output_df_map = {
            'meas_lower_limit-46': None,
            'meas_lower_limit-50': -30.0,
            'meas_value-18': '',
            'meas_value-13': '64',
            'meas_value-12': 64.0,
            'meas_value-10': 64.0,
            'meas_value-17': '',
            'meas_value-16': '64',
            'meas_value-14': '64',
            'meas_value-37': 64.5,
            'meas_upper_limit-57': 200.2,
            'meas_upper_limit-29': None,
            'meas_upper_limit-28': None,
            'meas_lower_limit-34': -30.0,
            'meas_lower_limit-33': -30.2,
            'meas_lower_limit-32': None,
            'meas_lower_limit-30': None,
            'meas_upper_limit-21': 200.2,
            'meas_upper_limit-20': None,
            'meas_upper_limit-22': 200.0,
            'meas_upper_limit-25': 200.2,
            'meas_upper_limit-24': None,
            'meas_upper_limit-26': 200.0,
            'meas_lower_limit-9': -30.2,
            'meas_lower_limit-8': None,
            'meas_datatype-48': 'string',
            'meas_datatype-49': 'string',
            'meas_lower_limit-5': -30.2,
            'meas_datatype-44': 'long',
            'meas_lower_limit-6': -30.0,
            'meas_datatype-42': 'long',
            'meas_datatype-40': 'double',
            'meas_datatype-41': 'long',
            'meas_lower_limit-45': None,
            'meas_lower_limit-24': None,
            'meas_lower_limit-25': -30.2,
            'meas_lower_limit-26': -30.0,
            'meas_lower_limit-20': None,
            'meas_lower_limit-21': -30.2,
            'meas_lower_limit-22': -30.0,
            'meas_upper_limit-36': None,
            'meas_upper_limit-37': 200.2,
            'meas_upper_limit-34': 200.0,
            'meas_upper_limit-32': None,
            'meas_lower_limit-29': None,
            'meas_upper_limit-30': None,
            'meas_datatype-50': 'string',
            'meas_datatype-53': 'double',
            'meas_datatype-52': 'string',
            'meas_datatype-54': 'double',
            'meas_datatype-57': 'long',
            'meas_datatype-56': 'double',
            'meas_datatype-58': 'long',
            'meas_upper_limit-49': 200.2,
            'meas_upper_limit-48': None,
            'meas_upper_limit-42': 200.0,
            'meas_upper_limit-41': 200.2,
            'meas_upper_limit-40': None,
            'meas_upper_limit-46': None,
            'meas_upper_limit-45': None,
            'meas_upper_limit-44': None,
            'meas_lower_limit-18': -30.0,
            'meas_lower_limit-10': -30.0,
            'meas_lower_limit-13': None,
            'meas_lower_limit-12': None,
            'meas_lower_limit-14': None,
            'meas_lower_limit-17': -30.2,
            'meas_lower_limit-16': None,
            'meas_upper_limit-38': 200.0,
            'meas_upper_limit-8': None,
            'meas_upper_limit-9': 200.2,
            'meas_upper_limit-6': 200.0,
            'meas_upper_limit-5': 200.2,
            'meas_datatype-24': 'double',
            'meas_datatype-25': 'long',
            'meas_datatype-26': 'long',
            'meas_value-61': '64',
            'meas_datatype-20': 'double',
            'meas_datatype-21': 'double',
            'meas_datatype-22': 'double',
            'meas_value-65': '',
            'meas_value-68': '',
            'meas_datatype-28': 'long',
            'meas_datatype-29': 'string',
            'meas_upper_limit-58': 200.0,
            'meas_value-62': '64',
            'meas_upper_limit-50': 200.0,
            'meas_lower_limit-28': None,
            'meas_upper_limit-52': None,
            'meas_upper_limit-53': 200.2,
            'meas_upper_limit-54': 200.0,
            'meas_value-60': 64.0,
            'meas_upper_limit-56': None,
            'meas_upper_limit-33': 200.2,
            'meas_value-66': '',
            'meas_value-64': '64',
            'meas_value-58': 64.0,
            'meas_value-57': 64.0,
            'meas_value-56': 64.5,
            'meas_value-54': 64.5,
            'meas_value-53': 64.5,
            'meas_value-52': '',
            'meas_value-50': '',
            'meas_datatype-37': 'double',
            'meas_datatype-36': 'long',
            'meas_datatype-34': 'long',
            'meas_datatype-33': 'long',
            'meas_datatype-32': 'string',
            'meas_datatype-30': 'string',
            'meas_datatype-38': 'double',
            'meas_lower_limit-37': -30.2,
            'meas_upper_limit-68': None,
            'meas_lower_limit-36': None,
            'meas_upper_limit-65': 200.2,
            'meas_upper_limit-64': None,
            'meas_upper_limit-66': 200.0,
            'meas_upper_limit-61': None,
            'meas_upper_limit-60': None,
            'meas_upper_limit-62': None,
            'meas_value-48': '64',
            'meas_value-49': '',
            'meas_value-40': 64.5,
            'meas_value-41': 64.0,
            'meas_value-42': 64.0,
            'meas_value-44': 64.0,
            'meas_value-45': '64',
            'meas_value-46': '64',
            'meas_lower_limit-68': None,
            'meas_lower_limit-60': None,
            'meas_lower_limit-61': None,
            'meas_lower_limit-62': None,
            'meas_lower_limit-64': None,
            'meas_lower_limit-65': -30.2,
            'meas_lower_limit-66': -30.0,
            'meas_lower_limit-52': None,
            'meas_lower_limit-38': -30.0,
            'meas_value-38': 64.5,
            'meas_datatype-18': 'double',
            'meas_value-34': '',
            'meas_datatype-14': 'string',
            'meas_datatype-17': 'double',
            'meas_datatype-16': 'string',
            'meas_datatype-10': 'long',
            'meas_datatype-13': 'string',
            'meas_value-32': '64',
            'meas_lower_limit-42': -30.0,
            'meas_value-36': '',
            'meas_lower_limit-58': -30.0,
            'meas_lower_limit-54': -30.0,
            'meas_lower_limit-57': -30.2,
            'meas_lower_limit-56': None,
            'meas_lower_limit-40': None,
            'meas_lower_limit-53': -30.2,
            'meas_value-30': '64',
            'meas_datatype-46': 'string',
            'meas_lower_limit-41': -30.2,
            'meas_value-33': '',
            'meas_upper_limit-10': 200.0,
            'meas_datatype-12': 'long',
            'meas_datatype-68': '',
            'meas_datatype-45': 'string',
            'meas_lower_limit-44': None,
            'meas_datatype-60': 'long',
            'meas_datatype-61': 'string',
            'meas_datatype-62': 'string',
            'meas_datatype-6': 'double',
            'meas_datatype-64': 'string',
            'meas_datatype-65': '',
            'meas_datatype-66': '',
            'meas_value-28': 64.0,
            'meas_value-29': '64',
            'meas_value-26': 64.0,
            'meas_value-24': 64.5,
            'meas_value-25': 64.0,
            'meas_value-22': 64.5,
            'meas_value-20': '',
            'meas_value-21': 64.5,
            'meas_value-9': 64.0,
            'meas_value-8': 64.76,
            'meas_value-6': 64.76,
            'meas_value-5': 64.76,
            'meas_upper_limit-14': None,
            'meas_upper_limit-16': None,
            'meas_upper_limit-17': 200.2,
            'meas_datatype-5': 'double',
            'meas_upper_limit-12': None,
            'meas_upper_limit-13': None,
            'meas_datatype-9': 'long',
            'meas_datatype-8': 'double',
            'meas_lower_limit-48': None,
            'meas_lower_limit-49': -30.2,
            'meas_upper_limit-18': 200.0}


    # clean up unnecessary column
        output_df = ConvertUtils.convert_meas_value(
            output_df, "measurements_cleansed")

        # convert it back to spark data frame
        spark_output_df = ec._sql_ctx_pyspark.createDataFrame(
            output_df, SensorMeasurement.get_schema())

        # convert to db model to save to cassandra
        output_df_db = SensorMeasurement.convert_to_db_model(
            spark_output_df, "measurements_cleansed")

        final_df = output_df_db.toPandas()

        final_df_map = create_map_from_spark_panda_dataframe(
            final_df, 'dataset')

        expected_df_map = {
            'meas_datatype-10': 'long',
            'meas_datatype-12': 'long',
            'meas_datatype-13': 'string',
            'meas_datatype-14': 'string',
            'meas_datatype-16': 'string',
            'meas_datatype-17': 'double',
            'meas_datatype-18': 'double',
            'meas_datatype-20': 'double',
            'meas_datatype-21': 'double',
            'meas_datatype-22': 'double',
            'meas_datatype-24': 'double',
            'meas_datatype-25': 'long',
            'meas_datatype-26': 'long',
            'meas_datatype-28': 'long',
            'meas_datatype-29': 'string',
            'meas_datatype-30': 'string',
            'meas_datatype-32': 'string',
            'meas_datatype-33': 'long',
            'meas_datatype-34': 'long',
            'meas_datatype-36': 'long',
            'meas_datatype-37': 'double',
            'meas_datatype-38': 'double',
            'meas_datatype-40': 'double',
            'meas_datatype-41': 'long',
            'meas_datatype-42': 'long',
            'meas_datatype-44': 'long',
            'meas_datatype-45': 'string',
            'meas_datatype-46': 'string',
            'meas_datatype-48': 'string',
            'meas_datatype-49': 'string',
            'meas_datatype-5': 'double',
            'meas_datatype-50': 'string',
            'meas_datatype-52': 'string',
            'meas_datatype-53': 'double',
            'meas_datatype-54': 'double',
            'meas_datatype-56': 'double',
            'meas_datatype-57': 'long',
            'meas_datatype-58': 'long',
            'meas_datatype-6': 'double',
            'meas_datatype-60': 'long',
            'meas_datatype-61': 'string',
            'meas_datatype-62': 'string',
            'meas_datatype-64': 'string',
            'meas_datatype-65': '',
            'meas_datatype-66': '',
            'meas_datatype-68': '',
            'meas_datatype-8': 'double',
            'meas_datatype-9': 'long',
            'meas_lower_limit-17': -30.2,
            'meas_lower_limit-18': -30.0,
            'meas_lower_limit-21': -30.2,
            'meas_lower_limit-22': -30.0,
            'meas_lower_limit-25': -30.2,
            'meas_lower_limit-33': -30.2,
            'meas_lower_limit-37': -30.2,
            'meas_lower_limit-38': -30.0,
            'meas_lower_limit-41': -30.2,
            'meas_lower_limit-49': -30.2,
            'meas_lower_limit-5': -30.2,
            'meas_lower_limit-50': -30.0,
            'meas_lower_limit-53': -30.2,
            'meas_lower_limit-54': -30.0,
            'meas_lower_limit-57': -30.2,
            'meas_lower_limit-6': -30.0,
            'meas_lower_limit-65': -30.2,
            'meas_lower_limit-66': -30.0,
            'meas_lower_limit-9': -30.2,
            'meas_upper_limit-17': 200.2,
            'meas_upper_limit-18': 200.0,
            'meas_upper_limit-21': 200.2,
            'meas_upper_limit-22': 200.0,
            'meas_upper_limit-25': 200.2,
            'meas_upper_limit-33': 200.2,
            'meas_upper_limit-37': 200.2,
            'meas_upper_limit-38': 200.0,
            'meas_upper_limit-41': 200.2,
            'meas_upper_limit-49': 200.2,
            'meas_upper_limit-5': 200.2,
            'meas_upper_limit-50': 200.0,
            'meas_upper_limit-53': 200.2,
            'meas_upper_limit-54': 200.0,
            'meas_upper_limit-57': 200.2,
            'meas_upper_limit-6': 200.0,
            'meas_upper_limit-65': 200.2,
            'meas_upper_limit-66': 200.0,
            'meas_upper_limit-9': 200.2,
            'meas_value-17': 64.00000000000006,
            'meas_value-20': 64.50000000000006,
            'meas_value-21': 64.5,
            'meas_value-22': 64.5,
            'meas_value-24': 64.5,
            'meas_value-37': 64.5,
            'meas_value-38': 64.5,
            'meas_value-40': 64.5,
            'meas_value-5': 64.76,
            'meas_value-53': 64.5,
            'meas_value-54': 64.5,
            'meas_value-56': 64.5,
            'meas_value-6': 64.76,
            'meas_value-65': 64.00000000000006,
            'meas_value-8': 64.76}

        self.assertEqual(num_keys_different(final_df_map, expected_df_map), 0)

        self.assertEqual(num_keys_different({'a': 1, 'b': 1}, {'c': 2}), 2)
        self.assertEqual(num_keys_different(
            {'a': 1, 'b': 1}, {'a': 1, 'b': 1}), 0)


if __name__ == "__main__":
    # NOTE Fixtures are added externally, by IPythonSpec.scala.
    unittest.main()
