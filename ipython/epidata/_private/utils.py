#
# Copyright (c) 2015-2017 EpiData, Inc.
#

import json
from pyspark.sql import Row
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import numpy as np

import datetime


class ConvertUtils(object):

    @staticmethod
    def convert_meas_value(df, destination):
        if destination == "measurements_cleansed":
            df['meas_value_d'] = np.nan
            df['meas_value_l'] = np.nan
            df['meas_value_s'] = ""
            df['meas_upper_limit_d'] = np.nan
            df['meas_upper_limit_l'] = np.nan
            df['meas_lower_limit_d'] = np.nan
            df['meas_lower_limit_l'] = np.nan

            for index, row in df.iterrows():
                if row['meas_datatype'] == "double":
                    ConvertUtils.try_set_cell_with_float_value_if_not_use_string(
                        df, index, 'meas_value_d', row, 'meas_value', 'meas_value_s')
                elif row['meas_datatype'] == "long":
                    ConvertUtils.try_set_cell_with_long_value_if_not_use_string(
                        df, index, 'meas_value_l', row, 'meas_value', 'meas_value_s')
                elif row['meas_datatype'] == "string":
                    ConvertUtils.try_set_cell_with_string_value(
                        df, index, 'meas_value_s', row, 'meas_value')
                else:
                    ConvertUtils.try_set_cell_with_float_value_if_not_use_string(
                        df, index, 'meas_value_d', row, 'meas_value', 'meas_value_s')


                if row['meas_datatype'] == "long" and ConvertUtils.is_long_number(row['meas_upper_limit']) and ConvertUtils.is_long_number(row['meas_lower_limit']):
                    ConvertUtils.try_set_cell_with_long_value(
                        df, index, 'meas_upper_limit_l', row, 'meas_upper_limit')
                    ConvertUtils.try_set_cell_with_long_value(
                        df, index, 'meas_lower_limit_l', row, 'meas_lower_limit')
                else:
                    ConvertUtils.try_set_cell_with_float_value(
                        df, index, 'meas_upper_limit_d', row, 'meas_upper_limit')
                    ConvertUtils.try_set_cell_with_float_value(
                        df, index, 'meas_lower_limit_d', row, 'meas_lower_limit')

            df = df.drop('meas_value', 1)
            df = df.drop('meas_upper_limit', 1)
            df = df.drop('meas_lower_limit', 1)
            df['meas_value'] = df['meas_value_d']
            df['meas_upper_limit'] = df['meas_upper_limit_d']
            df['meas_lower_limit'] = df['meas_lower_limit_d']
            df = df.drop('meas_upper_limit_d', 1)
            df = df.drop('meas_lower_limit_d', 1)
            df = df.drop('meas_value_d', 1)

            return df.where(df.notnull(), None)
        else:
            return df

    @staticmethod
    def is_long_number(val):
        try:
            return long(float(val)) == float(val)
        except BaseException:
            return False

    @staticmethod
    def try_set_cell_with_float_value_if_not_use_string(
            df, index, column_name, row, column_value_name, column_name_str):
        try:
            value = float(row[column_value_name])
            df.set_value(index, column_name, value)
        except BaseException:
            ConvertUtils.try_set_cell_with_string_value(
                df, index, column_name_str, row, column_value_name)

    @staticmethod
    def try_set_cell_with_float_value(
            df, index, column_name, row, column_value_name):
        try:
            value = float(row[column_value_name])
            df.set_value(index, column_name, value)
        except BaseException:
            df.set_value(index, column_name, np.nan)

    @staticmethod
    def try_set_cell_with_long_value(
            df, index, column_name, row, column_value_name):
        try:
            value = long(float(row[column_value_name]))
            df.set_value(index, column_name, value)
        except BaseException:
            df.set_value(index, column_name, np.nan)

    @staticmethod
    def try_set_cell_with_long_value_if_not_use_string(
            df, index, column_name, row, column_value_name, column_name_str):
        try:
            value = long(float(row[column_value_name]))
            df.set_value(index, column_name, value)
        except BaseException:
            ConvertUtils.try_set_cell_with_string_value(
                df, index, column_name_str, row, column_value_name)

    @staticmethod
    def try_set_cell_with_string_value(
            df, index, column_name, row, column_value_name):
        try:
            value = str(row[column_value_name])
            if value == "":
                df.set_value(index, column_name, np.nan)
            else:
                df.set_value(index, column_name, value)
        except BaseException:
            df.set_value(index, column_name, np.nan)

    @staticmethod
    def convert_to_pandas_dataframe_model(rdd_df, clean_up=True):

        df = rdd_df \
            .toPandas()
        df['meas_value'] = df['meas_value_str']
        df['meas_upper_limit'] = np.nan
        df['meas_lower_limit'] = np.nan

        for index, row in df.iterrows():
            if row['meas_value_datatype'] == "long":
                df.set_value(index, 'meas_value', row['meas_value_l'])
            elif row['meas_value_datatype'] == "double":
                df.set_value(index, 'meas_value', row['meas_value_d'])
            else:
                # since we set double as default
                pass

            if not np.isnan(row['meas_upper_limit_l']):
                df.set_value(
                    index,
                    'meas_upper_limit',
                    row['meas_upper_limit_l'])
            elif not np.isnan(row['meas_upper_limit_d']):
                df.set_value(
                    index,
                    'meas_upper_limit',
                    row['meas_upper_limit_d'])
            else:
                pass

            if not np.isnan(row['meas_lower_limit_l']):
                df.set_value(
                    index,
                    'meas_lower_limit',
                    row['meas_lower_limit_l'])
            elif not np.isnan(row['meas_lower_limit_d']):
                df.set_value(
                    index,
                    'meas_lower_limit',
                    row['meas_lower_limit_d'])
            else:
                pass

            if clean_up and not row['meas_value_datatype'] == "unknown":
                df.set_value(
                    index,
                    'meas_datatype',
                    row['meas_value_datatype'])

                if row['meas_value_datatype'] == "string" and row['meas_value_str'] != "":
                    df.set_value(index,'meas_upper_limit',np.nan)
                    df.set_value(index,'meas_lower_limit',np.nan)
                    df.set_value(index,'meas_unit',"")

        df = df.drop('meas_value_str', 1)
        df = df.drop('meas_value_d', 1)
        df = df.drop('meas_value_l', 1)
        df = df.drop('meas_value_datatype', 1)
        df = df.drop('meas_upper_limit_d', 1)
        df = df.drop('meas_upper_limit_l', 1)
        df = df.drop('meas_lower_limit_d', 1)
        df = df.drop('meas_lower_limit_l', 1)
        return df.where(df.notnull(), None)
