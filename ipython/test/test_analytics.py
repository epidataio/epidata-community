#
# Copyright (c) 2015-2017 EpiData, Inc.
#

from datetime import datetime, timedelta
from epidata.analytics import IMR, outliers
from epidata.context import ec
import pandas
from py4j.protocol import Py4JJavaError
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


ts = [datetime.fromtimestamp(1428004316.123 + x) for x in range(15)]


class IMRTest(unittest.TestCase):

    def assertEqualRows(self, one, two):
        if not isinstance(one, Column):
            self.assertEqual(one.asDict(), two.asDict())

    def assertEqualDataFrames(self, one, two):
        self.assertEqual(one.count(), two.count())
        for i, j in zip(one, two):
            self.assertEqualRows(i, j)

    def test_single_meas_dist(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'device_group': '1000',
                                             'tester': 'Station-1',
                                             'meas_name': 'Meas-1'},
                                            ts[0],
                                            ts[4])
        imr = IMR(df)
        self.assertEqual([45.7, 49.1, 48.8], imr.toPandas(
        ).loc[:, 'meas_value'].values.tolist())

        self.assertEqual(
            [45.7, 49.1, 48.8], imr.toPandas().loc[:, 'I'].values.tolist())
        i_mean = (45.7 + 49.1 + 48.8) / 3.0
        self.assertEqual(
            [i_mean] * 3, imr.toPandas().loc[:, 'I_mean'].values.tolist())
        i_lcl = i_mean - 2.66 * i_mean
        self.assertEqual(
            [i_lcl] * 3, imr.toPandas().loc[:, 'I_LCL'].values.tolist())
        i_ucl = i_mean + 2.66 * i_mean
        self.assertEqual(
            [i_ucl] * 3, imr.toPandas().loc[:, 'I_UCL'].values.tolist())

        self.assertEqual([49.1 - 45.7, 49.1 - 48.8],
                         imr.toPandas().loc[:, 'MR'].values.tolist()[1:])
        mr_mean = (49.1 - 45.7 + 49.1 - 48.8) / 2.0
        self.assertEqual(
            [mr_mean] * 3,
            imr.toPandas().loc[
                :,
                'MR_mean'].values.tolist())
        mr_lcl = 0.0
        self.assertEqual(
            [mr_lcl] * 3,
            imr.toPandas().loc[
                :,
                'MR_LCL'].values.tolist())
        mr_ucl = mr_mean + 3.267 * mr_mean
        self.assertEqual(
            [mr_ucl] * 3,
            imr.toPandas().loc[
                :,
                'MR_UCL'].values.tolist())

    def test_all_meas_dist(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'device_group': '1000',
                                             'tester': 'Station-1'},
                                            ts[0],
                                            ts[4] + timedelta(seconds=0.5))
        imr = IMR(df).toPandas().drop('ts', 1)
        # Compare without 'ts' column due to time representation inconsistencies
        # between systems.
        self.assertEqual(
            '     company    site device_group     tester device_name test_name meas_name meas_datatype  meas_value meas_unit meas_status  meas_lower_limit  meas_upper_limit meas_description device_status test_status     I     I_mean       I_LCL    I_UCL    MR  MR_mean  MR_LCL    MR_UCL\n'
            '0  Company-1  Site-1         1000  Station-1      100001    Test-1    Meas-1          None        45.7  degree C        PASS              40.0              90.0      Description          PASS        PASS  45.7  47.866667  -79.458667  175.192   NaN     1.85     0.0   7.89395\n'
            '1  Company-1  Site-1         1000  Station-1      101001    Test-1    Meas-1          None        49.1  degree C        PASS              40.0              90.0      Description          PASS        PASS  49.1  47.866667  -79.458667  175.192   3.4     1.85     0.0   7.89395\n'
            '2  Company-1  Site-1         1000  Station-1      101001    Test-1    Meas-1          None        48.8  degree C        PASS              40.0              90.0      Description          PASS        PASS  48.8  47.866667  -79.458667  175.192   0.3     1.85     0.0   7.89395\n'
            '3  Company-1  Site-1         1000  Station-1      101001    Test-1    Meas-2          None        88.8  degree C        PASS              40.0              90.0      Description          PASS        PASS  88.8  83.200000 -138.112000  304.512   NaN    11.20     0.0  47.79040\n'
            '4  Company-1  Site-1         1000  Station-1      101001    Test-1    Meas-2          None        77.6  degree C        PASS              40.0              90.0      Description          PASS        PASS  77.6  83.200000 -138.112000  304.512  11.2    11.20     0.0  47.79040',
            imr.to_string())

    def test_selected_meas_dist(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'device_group': '1000',
                                             'tester': 'Station-1'},
                                            ts[0],
                                            ts[4] + timedelta(seconds=0.5))
        # Filtering by meas_name before IMR and within IMR are equivalent.
        self.assertEqualDataFrames(IMR(df.filter(df.meas_name == 'Meas-1')),
                                   IMR(df, ['Meas-1']))
        # Filtering that matches all the meas_names is the same as no
        # filtering.
        self.assertEqualDataFrames(IMR(df),
                                   IMR(df, ['Meas-1', 'Meas-2']))
        # Filtering with a single name is also supported.
        self.assertEqualDataFrames(IMR(df.filter(df.meas_name == 'Meas-1')),
                                   IMR(df, 'Meas-1'))

    def test_insufficient_meas_dist(self):
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'device_group': '1000',
                                             'tester': 'Station-1'},
                                            ts[0],
                                            # Omit the second Meas-2
                                            # measurement.
                                            ts[3] + timedelta(seconds=0.5))

        # With the second Meas-2 measurement ommitted there won't be enough
        # measurements to perform IMR.
        with self.assertRaises(Py4JJavaError):
            IMR(df).retrieve()

        # IMR on the first measurement only is fine.
        IMR(df, 'Meas-1').retrieve()


class OutliersTest(unittest.TestCase):

    def test_quartile(self):
        df = ec.query_measurements_original(
            {
                'company': 'Company-1',
                'site': 'Site-1',
                'device_group': '1000',
                'tester': 'Station-2'},
            ts[0],
            ts[14] +
            timedelta(
                seconds=0.5)).toPandas()

        expected = df.ix[10:14, :]
        expected.loc[
            :,
            'meas_flag'] = [
            'mild',
            'extreme',
            'mild',
            'extreme']
        expected.loc[:, 'meas_method'] = ['quartile']

        self.assertEqual(expected.to_string(),
                         outliers(df, 'meas_value', 'quartile').to_string())

    def test_quartile_empty(self):
        # Test calling quartile on an empty DataFrame.
        df = ec.query_measurements_original({'company': 'Company-1',
                                             'site': 'Site-1',
                                             'device_group': '1000',
                                             'tester': 'Station-NONE'},  # No data for Station-NONE
                                            ts[0],
                                            ts[14] + timedelta(seconds=0.5)).toPandas()

        self.assertEqual(0,
                         outliers(df, 'meas_value', 'quartile').shape[0])

    def test_quartile_none(self):
        # Test a data frame containing a missing value.
        df = pandas.DataFrame({'a': [1.0] * 8 + [None, 2.0]})
        expected = df.ix[9:, :]
        expected.loc[:, 'meas_flag'] = ['extreme']
        expected.loc[:, 'meas_method'] = ['quartile']
        self.assertEqual(
            expected.to_string(),
            outliers(df, 'a', 'quartile').to_string())

    def test_quartile_string(self):
        df = pandas.DataFrame({'a': [1.0] * 8 + ['A STRING']})
        self.assertEqual(0,
                         outliers(df, 'a', 'quartile').shape[0])

    def test_quartile_numeric_object(self):
        # Test numeric values within an object typed column.
        df = pandas.DataFrame({'a': [1.0] * 9 + [2.0]}, dtype='object')
        expected = df.ix[9:, :]
        expected.loc[:, 'meas_flag'] = ['extreme']
        expected.loc[:, 'meas_method'] = ['quartile']
        result = outliers(df, 'a', 'quartile')
        self.assertEqual(expected.to_string(), result.to_string())
        self.assertTrue(all(['object', 'object', 'object'] == result.dtypes))

    def test_quartile_invalid_column(self):
        df = pandas.DataFrame({'a': [1.0]})
        with self.assertRaises(KeyError):
            outliers(df, 'INVALID_COLUMN_NAME', 'quartile')

    def test_invalid_method(self):
        df = pandas.DataFrame({'a': [1.0]})
        with self.assertRaises(ValueError):
            outliers(df, 'meas_value', 'INVALID_METHOD')


if __name__ == "__main__":
    # NOTE Fixtures are added externally, by IPythonSpec.scala.
    unittest.main()
