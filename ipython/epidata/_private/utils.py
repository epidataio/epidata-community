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
    def convert_string_to_row(x):
        dict = json.loads(x[1])

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

        return Row(**output)

    @staticmethod
    def convert_json_to_row(x):
        dict = json.loads(x[1])
        return Row(**dict)

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
            # print(df.dtypes)
            for index, row in df.iterrows():
                if row['meas_datatype'] == "double":
                    ConvertUtils.try_set_cell_with_float_value(
                        df, index, 'meas_value_d', row, 'meas_value')
                    ConvertUtils.try_set_cell_with_float_value(
                        df, index, 'meas_upper_limit_d', row, 'meas_upper_limit')
                    ConvertUtils.try_set_cell_with_float_value(
                        df, index, 'meas_lower_limit_d', row, 'meas_lower_limit')
                elif row['meas_datatype'] == "long":
                    ConvertUtils.try_set_cell_with_long_value(
                        df, index, 'meas_value_l', row, 'meas_value')
                    ConvertUtils.try_set_cell_with_long_value(
                        df, index, 'meas_upper_limit_l', row, 'meas_upper_limit')
                    ConvertUtils.try_set_cell_with_long_value(
                        df, index, 'meas_lower_limit_l', row, 'meas_lower_limit')
                elif row['meas_datatype'] == "string":
                    ConvertUtils.try_set_cell_with_string_value(
                        df, index, 'meas_value_s', row, 'meas_value')
                else:
                    pass

            df = df.drop('meas_datatype', 1)
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
            df.set_value(index, column_name, value)
        except BaseException:
            pass

    @staticmethod
    def convert_to_sensor_pandas_dataframe(rdd_df):

        df = rdd_df\
            .toPandas()
        df['meas_value'] = df['meas_value_str']
        df['meas_upper_limit'] = df['meas_upper_limit_str']
        df['meas_lower_limit'] = df['meas_lower_limit_str']
        for index, row in df.iterrows():
            if row['meas_datatype'] == "double":
                ConvertUtils.try_set_cell_with_float_value(
                    df, index, 'meas_value', row, 'meas_value_str')
                ConvertUtils.try_set_cell_with_float_value(
                    df, index, 'meas_upper_limit', row, 'meas_upper_limit_str')
                ConvertUtils.try_set_cell_with_float_value(
                    df, index, 'meas_lower_limit', row, 'meas_lower_limit_str')
            elif row['meas_datatype'] == "long":
                ConvertUtils.try_set_cell_with_long_value(
                    df, index, 'meas_value', row, 'meas_value_str')
                ConvertUtils.try_set_cell_with_long_value(
                    df, index, 'meas_upper_limit', row, 'meas_upper_limit_str')
                ConvertUtils.try_set_cell_with_long_value(
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

    @staticmethod
    def convert_to_sensor_dataframe(df):

        def convert_string_to_double(datatype, value):
            v = None
            if datatype == "double":
                try:
                    v = float(value)
                except BaseException:
                    v = None
            return v

        def convert_string_to_long(datatype, value):
            v = None
            if datatype == "long":
                try:
                    v = long(float(value))
                except BaseException:
                    v = None
            return v

        def convert_string_to_tring(datatype, value):
            v = None
            if datatype == "string":
                try:
                    v = value
                except BaseException:
                    v = None
            return v

        to_double = udf(convert_string_to_double, DoubleType())
        to_long = udf(convert_string_to_long, LongType())
        to_string = udf(convert_string_to_tring, StringType())

        output_df = df .withColumn(
            'meas_lower_limit',
            to_double(
                df['meas_datatype'],
                df['meas_lower_limit_str'])) .withColumn(
            'meas_upper_limit',
            to_double(
                df['meas_datatype'],
                df['meas_upper_limit_str'])) .withColumn(
                    'meas_lower_limit_l',
                    to_long(
                        df['meas_datatype'],
                        df['meas_lower_limit_str'])) .withColumn(
                            'meas_upper_limit_l',
                            to_long(
                                df['meas_datatype'],
                                df['meas_upper_limit_str'])) .withColumn(
                                    'meas_value',
                                    to_double(
                                        df['meas_datatype'],
                                        df['meas_value_str'])) .withColumn(
                                            'meas_value_l',
                                            to_long(
                                                df['meas_datatype'],
                                                df['meas_value_str'])) .withColumn(
                                                    'meas_value_s',
                                                    to_string(
                                                        df['meas_datatype'],
                                                        df['meas_value_str'])) .withColumn(
                                                            'meas_value_b',
                                                            lit(None).cast(
                                                                BinaryType()))
        return output_df \
            .drop("meas_lower_limit_str") \
            .drop("meas_upper_limit_str") \
            .drop("meas_value_str")

    @staticmethod
    def get_sensor_measurement_schema():
        return StructType([
            StructField("company", StringType(), True),
            StructField("event", StringType(), True),
            StructField("meas_description", StringType(), True),
            StructField("meas_flag", StringType(), True),
            StructField("meas_method", StringType(), True),
            StructField("meas_name", StringType(), True),
            StructField("meas_status", StringType(), True),
            StructField("meas_unit", StringType(), True),
            StructField("sensor", StringType(), True),
            StructField("site", StringType(), True),
            StructField("station", StringType(), True),
            StructField("ts", LongType(), True),
            StructField("meas_value_l", DoubleType(), True),
            StructField("meas_value_s", StringType(), True),
            StructField("meas_upper_limit_l", DoubleType(), True),
            StructField("meas_lower_limit_l", DoubleType(), True),
            StructField("meas_value", DoubleType(), True),
            StructField("meas_upper_limit", DoubleType(), True),
            StructField("meas_lower_limit", DoubleType(), True)])

    @staticmethod
    def get_stats_schema():
        return StructType([
            StructField("company", StringType(), True),
            StructField("site", StringType(), True),
            StructField("station", StringType(), True),
            StructField("sensor", StringType(), True),
            StructField("start_time", LongType(), True),
            StructField("stop_time", LongType(), True),
            StructField("event", StringType(), True),
            StructField("meas_name", StringType(), True),
            StructField("meas_summary_name", StringType(), True),
            StructField("meas_summary_value", StringType(), True),
            StructField("meas_summary_description", StringType(), True)])

    @staticmethod
    def convert_to_kafka_model(input_df, dest):
        return input_df

    @staticmethod
    def convert_to_db_model(input_df, dest):

        def convert_to_datetime(timestamp):
            ts = datetime.datetime.fromtimestamp(timestamp / 1e3)
            return ts

        def convert_timestamp_to_epoch(timestamp):
            return int(timestamp / (1000 * 1000 * 1000))

        to_ts = udf(convert_to_datetime, TimestampType())
        to_epoch = udf(convert_timestamp_to_epoch, IntegerType())

        df = input_df \
            .withColumnRenamed("company", "customer") \
            .withColumnRenamed("site", "customer_site") \
            .withColumnRenamed("station", "collection") \
            .withColumnRenamed("sensor", "dataset") \
            .withColumnRenamed("event", "key1") \
            .withColumnRenamed("meas_name", "key2") \
            .withColumn("key3", lit(""))

        if dest == "measurements_cleansed":
            output_df = df\
                .withColumn("val1", lit(None).cast(StringType())) \
                .withColumn("val2", lit(None).cast(StringType())) \
                .withColumn('epoch', to_epoch(df['ts'])) \
                .withColumn('meas_value_b', lit(None).cast(BinaryType()))

            return output_df.withColumn('ts', to_ts(df['ts']))

        elif dest == "measurements_summary":
            return df \
                .withColumn('start_time', to_ts(df['start_time'])) \
                .withColumn('stop_time', to_ts(df['stop_time']))
        else:
            return df
