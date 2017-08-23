#
# Copyright (c) 2015-2017 EpiData, Inc.
#

from datetime import datetime, timedelta
from epidata.context import ec
import numpy
from pyspark.sql import Row
from pyspark.sql import Column
from pyspark.tests import ReusedPySparkTestCase as PySparkTestCase
import unittest


SensorMeasurement = Row(
    'company',
    'site',
    'station',
    'sensor',
    'ts',
    'event',
    'meas_name',
    'meas_datatype',
    'meas_value',
    'meas_unit',
    'meas_status',
    'meas_lower_limit',
    'meas_upper_limit',
    'meas_description')


ts = [datetime.fromtimestamp(1428004316.123 + x) for x in range(5)]


class Base(unittest.TestCase):

    def assertEqualRows(self, one, two):
        if not isinstance(one, Column):
            self.assertEqual(one.asDict(), two.asDict())

    def assertEqualDataFrames(self, one, two):
        self.assertEqual(one.count(), two.count())
        for i, j in zip(one, two):
            self.assertEqualRows(i, j)


class EpidataContextTests(Base):

    def test_query_double(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'station': 'Station-1',
                                             'sensor': 'Sensor-1',
                                             'meas_name': 'Meas-1'},
                                            ts[0],
                                            ts[0] + timedelta(seconds=0.5)
                                            )
        self.assertEqual(1, df.count())
        self.assertEqualRows(
            SensorMeasurement(
                'Company-1',
                'Site-1',
                'Station-1',
                'Sensor-1',
                ts[0],
                'Event-1',
                'Meas-1',
                'double',
                45.7,
                'degree C',
                'PASS',
                40.0,
                90.0,
                'Description'),
            df.head())

    def test_query_two_results(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'station': 'Station-1',
                                             'sensor': 'Sensor-1',
                                             'meas_name': 'Meas-1'},
                                            ts[0],
                                            ts[4] + timedelta(seconds=0.5)
                                            )
        self.assertEqual(2, df.count())
        self.assertEqualRows(
            SensorMeasurement(
                'Company-1',
                'Site-1',
                'Station-1',
                'Sensor-1',
                ts[0],
                'Event-1',
                'Meas-1',
                'double',
                45.7,
                'degree C',
                'PASS',
                40.0,
                90.0,
                'Description'),
            df.head())
        self.assertEqualRows(
            SensorMeasurement(
                'Company-1',
                'Site-1',
                'Station-1',
                'Sensor-1',
                ts[1],
                'Event-1',
                'Meas-1',
                'double',
                49.1,
                'degree C',
                'PASS',
                40.0,
                90.0,
                'Description'),
            df.retrieve()[1])

    def test_query_int(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'station': 'Station-1',
                                             'sensor': 'Sensor-1',
                                             'meas_name': 'Meas-3'},
                                            ts[0],
                                            ts[4] + timedelta(seconds=0.5)
                                            )
        self.assertEqual(1, df.count())
        self.assertEqualRows(
            SensorMeasurement(
                'Company-1',
                'Site-1',
                'Station-1',
                'Sensor-1',
                ts[2],
                'Event-1',
                'Meas-3',
                'long',
                51,
                'degree C',
                'PASS',
                42,
                93,
                'Description'),
            df.head())

    def test_query_string(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'station': 'Station-1',
                                             'sensor': 'Sensor-1',
                                             'meas_name': 'Meas-4'},
                                            ts[0],
                                            ts[4] + timedelta(seconds=0.5)
                                            )
        self.assertEqual(1, df.count())
        self.assertEqualRows(
            SensorMeasurement(
                'Company-1',
                'Site-1',
                'Station-1',
                'Sensor-1',
                ts[3],
                'Event-1',
                'Meas-4',
                'string',
                'POWER ON',
                None,
                'PASS',
                None,
                None,
                'Description'),
            df.head())

    def test_query_binary(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'station': 'Station-1',
                                             'sensor': 'Sensor-1',
                                             'meas_name': 'Meas-5'},
                                            ts[0],
                                            ts[4] + timedelta(seconds=0.5)
                                            )
        self.assertEqual(1, df.count())
        head = df.head()
        self.assertEqual('Company-1', head.company)
        self.assertEqual('Site-1', head.site)
        self.assertEqual('Station-1', head.station)
        self.assertEqual('Sensor-1', head.sensor)
        self.assertEqual(ts[4], head.ts)
        self.assertEqual('Event-1', head.event)
        self.assertEqual('Meas-5', head.meas_name)
        self.assertTrue(numpy.array_equal(numpy.array(
            [0.5555, 0.6666, 0.7777, 0.8888, 0.9999]), head.meas_value))
        self.assertEqual('V', head.meas_unit)
        self.assertEqual('PASS', head.meas_status)
        self.assertIsNone(head.meas_lower_limit)
        self.assertIsNone(head.meas_upper_limit)
        self.assertEqual('Description', head.meas_description)

    def test_list_keys(self):
        df = ec.list_keys()
        self.assertEqual(
            'company    site    station    sensor\n'
            'Company-1  Site-1  Station-1  Sensor-1',
            df.toPandas().sort(
                ['company', 'site', 'station', 'sensor'])
            .to_string(index=False))


if __name__ == "__main__":
    # NOTE Fixtures are added externally, by IPythonSpec.scala.
    unittest.main()
