#
# Copyright (c) 2015-2017 EpiData, Inc.
#

from datetime import datetime, timedelta
from epidata.context import ec
from epidata_common.data_types import Waveform
import numpy
import pandas
from pyspark.sql import Row
from pyspark.sql import Column
import unittest


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


ts = [datetime.fromtimestamp(1428004316.123 + x) for x in range(6)]


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
                'double',
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
                'double',
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
                'double',
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
                'long',
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
                'string',
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
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'device_group': '1000',
                                             'tester': 'Station-1',
                                             'test_name': 'Test-5'},
                                            ts[0],
                                            ts[5] + timedelta(seconds=0.5)
                                            )
        self.assertEqual(1, df.count())
        head = df.head()
        self.assertEqual('Company-1', head.company)
        self.assertEqual('Site-1', head.site)
        self.assertEqual('1000', head.device_group)
        self.assertEqual('Station-1', head.tester)
        self.assertEqual(ts[4], head.ts)
        self.assertEqual('101001', head.device_name)
        self.assertEqual('Test-5', head.test_name)
        self.assertEqual('Meas-2', head.meas_name)
        self.assertTrue(numpy.array_equal(
            numpy.array([0.1111, 0.2222, 0.3333, 0.4444]), head.meas_value))
        self.assertEqual('V', head.meas_unit)
        self.assertEqual('PASS', head.meas_status, )
        self.assertIsNone(head.meas_lower_limit)
        self.assertIsNone(head.meas_upper_limit)
        self.assertEqual('Description', head.meas_description)
        self.assertEqual('PASS', head.device_status)
        self.assertEqual('PASS', head.test_status)

    def test_query_waveform(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'device_group': '1000',
                                             'tester': 'Station-1',
                                             'test_name': 'Test-6'},
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
                ts[5],
                '101001',
                'Test-6',
                'Meas-2',
                'waveform',
                Waveform(ts[5], 0.1234, numpy.array([0.5678, 0.9012, 0.3456])),
                'V',
                'PASS',
                None,
                None,
                'Description',
                'PASS',
                'PASS'),
            df.head())

    def test_list_keys(self):
        df = ec.list_keys()
        self.assertEqual(
            'company    site device_group     tester\n'
            'Company-1  Site-1         1000  Station-1\n'
            'Company-1  Site-1         1000  Station-3\n'
            'Company-2  Site-1         1000  Station-1',
            df.toPandas().sort(
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

    def test_cache(self):
        # Not testing caching, just the return value.
        self.assertEqualDataFrames(self.df, self.df.cache())

    def test_count(self):
        self.assertEqual(6, self.df.count())

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

    def test_describe_by_group(self):
        self.assertEqual(
            '       Test-1.meas_value  Test-1.meas_lower_limit  Test-1.meas_upper_limit Test-3.meas_value Test-3.meas_lower_limit Test-3.meas_upper_limit  Test-4.meas_value  Test-4.meas_lower_limit  Test-4.meas_upper_limit  Test-5.meas_value  Test-5.meas_lower_limit  Test-5.meas_upper_limit  Test-6.meas_value  Test-6.meas_lower_limit  Test-6.meas_upper_limit\n'
            'count           2.000000                      2.0                      2.0                 1                       1                       1                1.0                      0.0                      0.0                1.0                      0.0                      0.0                1.0                      0.0                      0.0\n'
            'mean           47.400000                     40.0                     90.0       3.44839e+09             3.44839e+09             3.44839e+09                NaN                      NaN                      NaN                NaN                      NaN                      NaN                NaN                      NaN                      NaN\n'
            'std             2.404163                      0.0                      0.0               NaN                     NaN                     NaN                NaN                      NaN                      NaN                NaN                      NaN                      NaN                NaN                      NaN                      NaN\n'
            'min            45.700000                     40.0                     90.0        3448388841              3448388840              3448388842                NaN                      NaN                      NaN                NaN                      NaN                      NaN                NaN                      NaN                      NaN\n'
            'max            49.100000                     40.0                     90.0        3448388841              3448388840              3448388842                NaN                      NaN                      NaN                NaN                      NaN                      NaN                NaN                      NaN                      NaN',
            self.df.describe_by_group(
                'test_name', [
                    'meas_value', 'meas_lower_limit', 'meas_upper_limit']).to_string())

    def test_describe_by_group_empty(self):
        empty_df = self.df.filter(self.df.meas_name == 'MISSING_NAME')
        self.assertEqual(
            'Empty DataFrame\nColumns: []\nIndex: [count, mean, std, min, max]',
            empty_df.describe_by_group(
                'test_name', [
                    'meas_value', 'meas_lower_limit', 'meas_upper_limit']).to_string())

    def test_distinct(self):
        self.assertEqual(set(['Test-1', 'Test-3', 'Test-4', 'Test-5', 'Test-6']), set(
            self.df.select('test_name').distinct().toPandas().ix[:, 0].tolist()))

    def test_filter(self):
        self.assertEqual(
            2, self.df.filter(
                self.df.test_name == 'Test-1').count())
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
            1, self.df.filter(
                self.df.test_name == 'Test-5').count())
        self.assertEqual(
            5, self.df.filter(
                self.df.test_name != 'Test-5').count())

    def test_get_device_data(self):
        self.assertEqual(
            self.df.filter(
                self.df.device_name == '101001').toPandas().sort('ts').to_string(),
            self.df.get_device_data('101001').to_string())

    def test_get_device_data_without_ts(self):
        # Test without a 'ts' timestamp field to sort by.
        df_device = self.df.select('device_name')
        self.assertEqual(
            df_device.filter(
                df_device.device_name == '101001').toPandas().to_string(),
            df_device.get_device_data('101001').to_string())

    def test_get_device_data_missing(self):
        # Test calling get_device_data on a DataFrame missing a device_name
        # field.
        df = self.df.select('meas_value')
        with self.assertRaises(KeyError):
            df.get_device_data('101001')

    def test_get_meas_data(self):
        self.assertEqual(
            self.df.filter(
                self.df.meas_name == 'Meas-2').toPandas().sort('ts').to_string(),
            self.df.get_meas_data('Meas-2').to_string())

    def test_get_meas_data_without_ts(self):
        # Test without a 'ts' timestamp field to sort by.
        df_meas = self.df.select('meas_name')
        self.assertEqual(
            df_meas.filter(
                df_meas.meas_name == 'Meas-2').toPandas().to_string(),
            df_meas.get_meas_data('Meas-2').to_string())

    def test_get_meas_data_missing(self):
        # Test calling get_meas_data on a DataFrame missing a meas_name field.
        df = self.df.select('meas_value')
        with self.assertRaises(KeyError):
            df.get_meas_data('Meas-2')

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
                'double',
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
                'double',
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

    def test_limit(self):
        self.assertEqualRows(self.df.head(), self.df.limit(1).retrieve()[0])
        self.assertEqual(3, self.df.limit(3).count())
        for i, j in zip(self.df.head(3), self.df.limit(3).retrieve()):
            self.assertEqualRows(i, j)

    def test_select(self):
        self.assertEqualRows(Row(meas_description='Description'),
                             self.df.select('meas_description').head())
        self.assertEqualRows(Row(meas_description='Description'),
                             self.df[['meas_description']].head())
        self.assertEqualRows(Row(meas_description='Description',
                                 meas_status='PASS'),
                             self.df.select('meas_description',
                                            'meas_status').head())
        self.assertEqualRows(Row(meas_description='Description', meas_status='PASS'), self.df[
                             ['meas_description', 'meas_status']].head())

    def test_sort(self):
        self.assertEqual(ts[5], self.df.sort(self.df.ts.desc()).head().ts)

    def test_show(self):
        self.df.show()

    def test_toPandas(self):
        self.assertEqual(6, len(self.df.toPandas().index))
        values = self.df.toPandas()['meas_value'].tolist()
        self.assertEqual(45.7, values[0])
        self.assertEqual(49.1, values[1])
        self.assertEqual(3448388841, values[2])
        self.assertEqual('POWER ON', values[3])
        self.assertTrue(numpy.array_equal(
            numpy.array([0.1111, 0.2222, 0.3333, 0.4444]), values[4]))
        self.assertEqual(
            Waveform(ts[5], 0.1234, numpy.array([0.5678, 0.9012, 0.3456])),
            values[5])

    def test_union(self):
        test1 = self.df.filter(self.df.test_name == 'Test-1')
        test5 = self.df.filter(self.df.test_name == 'Test-5')
        union = test1.union(test5)
        union2 = test5.union(test1)
        self.assertEqual(3, union.count())
        self.assertEqual(3, union2.count())
        self.assertEqual(set(['Test-1', 'Test-5']),
                         set(union[['test_name']].distinct().toPandas().ix[:, 0].tolist()))
        self.assertEqual(set(['Test-1', 'Test-5']),
                         set(union2[['test_name']].distinct().toPandas().ix[:, 0].tolist()))


if __name__ == "__main__":
    # NOTE Fixtures are added externally, by IPythonSpec.scala.
    unittest.main()
