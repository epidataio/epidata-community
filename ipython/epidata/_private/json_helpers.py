#
# Copyright (c) 2015-2017 EpiData, Inc.
#
import json
import numpy as np
import pandas as pd

class JsonHelpers(object):
    @staticmethod
    def convert_json_string_to_dictionary(json_string):
        dict = json.loads(json_string)

        output = {}
        output["meas_flag"] = "" if "meas_flag" not in dict else dict["meas_flag"]
        output["meas_method"] = "" if "meas_method" not in dict else dict["meas_method"]
        output["company"] = "" if "company" not in dict else dict["company"]
        output["event"] = "" if "event" not in dict else dict["event"]
        output["meas_datatype"] = "" if "meas_datatype" not in dict else dict["meas_datatype"]
        output["meas_description"] = "" if "meas_description" not in dict else dict["meas_description"]
        output["meas_lower_limit_str"] = "" if "meas_lower_limit" not in dict else str(
            dict["meas_lower_limit"])
        output["meas_name"] = "" if "meas_name" not in dict else dict["meas_name"]
        output["meas_status"] = "" if "meas_status" not in dict else dict["meas_status"]
        output["meas_unit"] = "" if "meas_unit" not in dict else dict["meas_unit"]
        output["meas_upper_limit_str"] = "" if "meas_upper_limit" not in dict else str(
            dict["meas_upper_limit"])
        output["meas_value_str"] = "" if "meas_value" not in dict and output["meas_datatype"] not in [
            "double_array", "waveform"] else str(dict["meas_value"])
        output["sensor"] = "" if "sensor" not in dict else dict["sensor"]
        output["site"] = "" if "site" not in dict else dict["site"]
        output["station"] = "" if "station" not in dict else dict["station"]
        output["ts"] = 0 if "ts" not in dict else dict["ts"]

        return output

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
    def try_set_cell_with_float_value_if_not_use_string(
            df, index, column_name, row, column_value_name, column_name_str):
        try:
            value = float(row[column_value_name])
            df.set_value(index, column_name, value)
        except BaseException:
            JsonHelpers.try_set_cell_with_string_value(
                df, index, column_name_str, row, column_value_name)

    @staticmethod
    def try_set_cell_with_long_value_if_not_use_string(
            df, index, column_name, row, column_value_name, column_name_str):
        try:
            value = long(float(row[column_value_name]))
            df.set_value(index, column_name, value)
        except BaseException:
            JsonHelpers.try_set_cell_with_string_value(
                df, index, column_name_str, row, column_value_name)

    @staticmethod
    def convert_json_measurements_to_panda_dataframe(json_list):
        dict_list = list(map(
            lambda json_str: JsonHelpers.convert_json_string_to_dictionary(json_str), json_list))
        df = pd.DataFrame(dict_list)
        return JsonHelpers.convert_to_sensor_pandas_dataframe(df)

    @staticmethod
    def convert_to_sensor_pandas_dataframe(df):

        df['meas_value'] = df['meas_value_str']
        df['meas_upper_limit'] = df['meas_upper_limit_str']
        df['meas_lower_limit'] = df['meas_lower_limit_str']
        for index, row in df.iterrows():
            if row['meas_datatype'] == "double":
                JsonHelpers.try_set_cell_with_float_value(
                    df, index, 'meas_value', row, 'meas_value_str')
                JsonHelpers.try_set_cell_with_float_value(
                    df, index, 'meas_upper_limit', row, 'meas_upper_limit_str')
                JsonHelpers.try_set_cell_with_float_value(
                    df, index, 'meas_lower_limit', row, 'meas_lower_limit_str')
            elif row['meas_datatype'] == "long":
                JsonHelpers.try_set_cell_with_long_value(
                    df, index, 'meas_value', row, 'meas_value_str')
                JsonHelpers.try_set_cell_with_long_value(
                    df, index, 'meas_upper_limit', row, 'meas_upper_limit_str')
                JsonHelpers.try_set_cell_with_long_value(
                    df, index, 'meas_lower_limit', row, 'meas_lower_limit_str')
            elif row['meas_datatype'] == "string":
                df.set_value(index, 'meas_value', row['meas_value_str'])
                df.set_value(
                    index,
                    'meas_upper_limit',
                    row['meas_upper_limit_str'])
                df.set_value(
                    index,
                    'meas_lower_limit',
                    row['meas_lower_limit_str'])
            else:
                df.set_value(index, 'meas_value', np.nan)
        df = df.drop('meas_value_str', 1)
        df = df.drop('meas_upper_limit_str', 1)
        df = df.drop('meas_lower_limit_str', 1)
        return df.where(df.notnull(), None)
