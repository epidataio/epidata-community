#
# Copyright (c) 2015-2017 EpiData, Inc.
#

from datetime import datetime, timedelta
from epidata.EpidataLiteContext import EpidataLiteContext
from epidata_common.data_types import Waveform
import numpy
import pandas
import pyspark.sql
from pyspark.sql import Row
from pyspark.sql import Column
import unittest
import sqlite3
from sqlite3 import Error


AutomatedTest = Row(
    'company',
    'site',
    'device_group',
    'tester',
    'ts',
    'device_name',
    'test_name',
    'meas_name',
    'meas_datatype',
    'meas_value',
    'meas_unit',
    'meas_status',
    'meas_lower_limit',
    'meas_upper_limit',
    'meas_description',
    'device_status',
    'test_status')


def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    except Error as e:
        print(e)

    return conn

ts = [datetime.fromtimestamp(1428004316.123 + x) for x in range(6)]

database = "/Users/lujiajun/Documents/epidata-community/data/epidata_test.db"

con = create_connection(database)

ec = EpidataLiteContext()



class Base(unittest.TestCase):

    def assertEqualRows(self, one, two):
        # if not isinstance(one, Column):
        self.assertEqual(one.asDict(), two.asDict())

    def assertEqualDataFrames(self, one, two):
        self.assertEqual(one.count(), two.count())
        for i, j in zip(one, two):
            self.assertEqualRows(i, j)





class EpidataContextTests(Base):
    def test_simple_query_test(self):
        # create a database connection
        cur = con.cursor()
        cur.execute("select * from measurements_original where key2 = 'Test-5'")
        rows = cur.fetchall()

        for row in rows:
            print(row)

    def test_query_double(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'device_group': '1000',
                                             'tester': 'Station-1',
                                             'test_name': 'Test-1'},
                                            ts[0],
                                            ts[0] + timedelta(seconds=0.5)
                                            )
        self.assertEqual(1, df.count())
        self.assertEqualRows(
            AutomatedTest(
                'Company-1',
                'Site-1',
                '1000',
                'Station-1',
                ts[0],
                '100001',
                'Test-1',
                'Meas-1',
                None,
                45.7,
                'degree C',
                'PASS',
                40.0,
                90.0,
                'Description',
                'PASS',
                'PASS'),
            df.head())

    def test_query_two_results(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'device_group': '1000',
                                             'tester': 'Station-1',
                                             'test_name': 'Test-1'},
                                            ts[0],
                                            ts[5] + timedelta(seconds=0.5)
                                            )
        self.assertEqual(2, df.count())
        self.assertEqualRows(
            AutomatedTest(
                'Company-1',
                'Site-1',
                '1000',
                'Station-1',
                ts[0],
                '100001',
                'Test-1',
                'Meas-1',
                None,
                45.7,
                'degree C',
                'PASS',
                40.0,
                90.0,
                'Description',
                'PASS',
                'PASS'),
            df.head())
        self.assertEqualRows(
            AutomatedTest(
                'Company-1',
                'Site-1',
                '1000',
                'Station-1',
                ts[1],
                '101001',
                'Test-1',
                'Meas-2',
                None,
                49.1,
                'degree C',
                'PASS',
                40.0,
                90.0,
                'Description',
                'PASS',
                'PASS'),
            df.retrieve()[1])


    def test_query_multiple_partitions(self):
        df = ec.query_measurements_original({'company': ['Company-1'],
                                             'site': ['Site-1'],
                                             'device_group': ['1000'],
                                             'tester': ['Station-1'],
                                             'test_name': ['Test-1']},
                                            ts[0],
                                            ts[5] + timedelta(seconds=0.5)
                                            )
        self.assertEqual(2, df.count())

        df = ec.query_measurements_original(
            {
                'company': [
                    'Company-1',
                    'Company-2'],
                'site': ['Site-1'],
                'device_group': ['1000'],
                'tester': ['Station-1'],
                'test_name': ['Test-1']},
            ts[0],
            ts[5] +
            timedelta(
                seconds=0.5))
        self.assertEqual(3, df.count())

        df = ec.query_measurements_original(
            {
                'company': ['Company-1'],
                'site': ['Site-1'],
                'device_group': ['1000'],
                'tester': [
                    'Station-1',
                    'Station-3'],
                'test_name': ['Test-1']},
            ts[0],
            ts[5] +
            timedelta(
                seconds=0.5))
        self.assertEqual(3, df.count())

        df = ec.query_measurements_original(
            {
                'company': [
                    'Company-1',
                    'Company-2'],
                'site': ['Site-1'],
                'device_group': ['1000'],
                'tester': [
                    'Station-1',
                    'Station-3'],
                'test_name': ['Test-1']},
            ts[0],
            ts[5] +
            timedelta(
                seconds=0.5))
        self.assertEqual(4, df.count())

        df = ec.query_measurements_original(
            {
                'company': [
                    'Company-1',
                    'Company-2'],
                'site': ['Site-1'],
                'device_group': ['1000'],
                'tester': [
                    'Station-1',
                    'Station-3'],
                'test_name': [
                    'Test-1',
                    'Test-3']},
            ts[0],
            ts[5] +
            timedelta(
                seconds=0.5))
        self.assertEqual(5, df.count())


    def test_query_int(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'device_group': '1000',
                                             'tester': 'Station-1',
                                             'test_name': 'Test-3'},
                                            ts[0],
                                            ts[5] + timedelta(seconds=0.5)
                                            )
        self.assertEqual(1, df.count())
        large = 3448388841
        self.assertEqualRows(
            AutomatedTest(
                'Company-1',
                'Site-1',
                '1000',
                'Station-1',
                ts[2],
                '101001',
                'Test-3',
                'Meas-2',
                None,
                large,
                'ns',
                'PASS',
                large - 1,
                large + 1,
                'Description',
                'PASS',
                'PASS'),
            df.head())

    def test_query_string(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'device_group': '1000',
                                             'tester': 'Station-1',
                                             'test_name': 'Test-4'},
                                            ts[0],
                                            ts[5] + timedelta(seconds=0.5)
                                            )
        self.assertEqual(1, df.count())
        self.assertEqualRows(
            AutomatedTest(
                'Company-1',
                'Site-1',
                '1000',
                'Station-1',
                ts[3],
                '101001',
                'Test-4',
                'Meas-2',
                None,
                'POWER ON',
                None,
                'PASS',
                None,
                None,
                'Description',
                'PASS',
                'PASS'),
            df.head())

    def test_query_array(self):
        return

    def test_query_waveform(self):
        return

    def test_list_keys(self):
        df = ec.list_keys()
        self.assertEqual(
            'company    site device_group     tester\n'
            'Company-1  Site-1         1000  Station-1\n'
            'Company-1  Site-1         1000  Station-3\n'
            'Company-2  Site-1         1000  Station-1',
            df.sort(
                ['company', 'site', 'device_group', 'tester'])
                .to_string(index=False))



class DataFrameTests(Base):
    start_time = datetime.fromtimestamp(1428004316.123)
    end_time = datetime.fromtimestamp(1428005326.163)
    df = ec.query_measurements_original({'company': 'Company-1',
                                         'site': 'Site-1',
                                         'device_group': '1000',
                                         'tester': 'Station-1'},
                                        start_time,
                                        end_time)
    # df = ec.query_measurements_cleansed({'company': 'Company-1',
    #                                      'site': 'Site-1',
    #                                      'device_group': '1000',
    #                                      'tester': 'Station-1'},
    #                                     start_time,
    #                                     end_time)

    def simpleTest(self):
        return

    def test_count(self):
        self.assertEqual(6, self.df.size())

    def test_describe(self):
        self.assertEqual(
            '       meas_value  meas_lower_limit  meas_upper_limit\n'
            'count         6.0      3.000000e+00      3.000000e+00\n'
            'mean          NaN      1.149463e+09      1.149463e+09\n'
            'std           NaN      1.990928e+09      1.990928e+09\n'
            'min           NaN      4.000000e+01      9.000000e+01\n'
            'max           NaN      3.448389e+09      3.448389e+09',
            self.df.describe().to_string())
        self.assertEqual(
            '       meas_value  meas_lower_limit  meas_upper_limit\n'
            'count    2.000000               2.0               2.0\n'
            'mean    47.400000              40.0              90.0\n'
            'std      2.404163               0.0               0.0\n'
            'min     45.700000              40.0              90.0\n'
            'max     49.100000              40.0              90.0', self.df.filter(
                self.df.test_name == 'Test-1').describe().to_string())

    def test_describe_empty(self):
        empty_df = self.df.filter(self.df.meas_name == 'MISSING_NAME')
        self.assertEqual(
            '       meas_value  meas_lower_limit  meas_upper_limit\n'
            'count         0.0               0.0               0.0\n'
            'mean          NaN               NaN               NaN\n'
            'std           NaN               NaN               NaN\n'
            'min           NaN               NaN               NaN\n'
            'max           NaN               NaN               NaN',
            empty_df.describe().to_string())

    def test_filter(self):
        self.assertEqual(
            2, len(self.df.filter(
                self.df.test_name == 'Test-1').index))
        self.assertEqualDataFrames(
            self.df.filter(
                self.df.test_name == 'Test-1'),
            self.df[
                self.df.test_name == 'Test-1'])
        self.assertEqualDataFrames(
            self.df.filter(
                self.df.test_name == 'Test-1'),
            self.df.filter("test_name = 'Test-1'"))
        self.assertEqual(
            1, len(self.df.filter(
                self.df.test_name == 'Test-5').index))
        self.assertEqual(
            5, len(self.df.filter(
                self.df.test_name != 'Test-5').index))

    def test_head(self):
        self.assertEqualRows(
            AutomatedTest(
                'Company-1',
                'Site-1',
                '1000',
                'Station-1',
                ts[0],
                '100001',
                'Test-1',
                'Meas-1',
                None,
                45.7,
                'degree C',
                'PASS',
                40.0,
                90.0,
                'Description',
                'PASS',
                'PASS'),
            self.df.head())
        self.assertEqualRows(
            AutomatedTest(
                'Company-1',
                'Site-1',
                '1000',
                'Station-1',
                ts[1],
                '101001',
                'Test-1',
                'Meas-2',
                None,
                49.1,
                'degree C',
                'PASS',
                40.0,
                90.0,
                'Description',
                'PASS',
                'PASS'),
            self.df.head(2)[1])
        self.assertEqual(3, len(self.df.head(3)))

    def test_sort(self):
        self.assertEqual(ts[5], self.df.sort_values(by=['ts'], ascending=False).head().ts)






if __name__ == "__main__":
    # NOTE Fixtures are added externally, by IPythonSpec.scala.
    unittest.main()
