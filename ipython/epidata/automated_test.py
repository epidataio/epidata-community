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


class AutomatedTest(object):

    @staticmethod
    def to_row(x):
        dict = json.loads(x[1])
        output = {}
        output["company"] = dict.get("company", "")
        output["site"] = dict.get("site", "")
        output["device_group"] = dict.get("device_group", "")
        output["tester"] = dict.get("tester", "")
        output["ts"] = dict.get("ts", 0)
        output["device_name"] = dict.get("device_name", "")
        output["test_name"] = dict.get("test_name", "")
        output["meas_name"] = dict.get("meas_name", "")
        output["meas_datatype"] = dict.get("meas_datatype", "")
        output["meas_unit"] = dict.get("meas_unit", "")
        output["meas_status"] = dict.get("meas_status", "")
        output["meas_description"] = dict.get("meas_description", "")
        output["device_status"] = dict.get("device_status", "")
        output["test_status"] = dict.get("test_status", "")
        output["meas_flag"] = dict.get("meas_flag", "")
        output["meas_method"] = dict.get("meas_method", "")

        # Set meas_value and meas_value_datatype
        output["meas_value_datatype"] = "unknown"
        output["meas_value_str"] = ""
        output["meas_value_d"] = np.nan
        output["meas_value_l"] = np.nan

        if "meas_value" in dict and isinstance(dict['meas_value'], unicode):
            output["meas_value_str"] = dict["meas_value"]
            if output["meas_value_str"] != "":
                output["meas_value_datatype"] = 'string'

        if "meas_value" in dict and isinstance(dict['meas_value'], float):
            if output["meas_datatype"] == "long" and long(
                    float(dict['meas_value'])) == float(dict['meas_value']):
                output["meas_value_l"] = float(dict["meas_value"])
                output["meas_value_datatype"] = 'long'
            else:
                output["meas_value_d"] = float(dict["meas_value"])
                output["meas_value_datatype"] = 'double'

        if "meas_value" in dict and (
            isinstance(
                dict['meas_value'],
                int) or isinstance(
                dict['meas_value'],
                long)):
            output["meas_value_l"] = float(dict["meas_value"])
            output["meas_value_datatype"] = 'long'

        # Set meas_lower_limit
        output["meas_lower_limit_d"] = np.nan
        output["meas_lower_limit_l"] = np.nan

        if "meas_lower_limit" in dict and isinstance(
                dict['meas_lower_limit'], float):
            if long(
                    float(
                        dict["meas_lower_limit"])) == float(
                    dict["meas_lower_limit"]):
                output["meas_lower_limit_l"] = float(dict["meas_lower_limit"])
            else:
                output["meas_lower_limit_d"] = float(dict["meas_lower_limit"])

        if "meas_lower_limit" in dict and (
            isinstance(
                dict['meas_lower_limit'],
                int) or isinstance(
                dict['meas_lower_limit'],
                long)):
            output["meas_lower_limit_l"] = float(dict["meas_lower_limit"])

        # Set meas_upper_limit
        output["meas_upper_limit_d"] = np.nan
        output["meas_upper_limit_l"] = np.nan

        if "meas_upper_limit" in dict and isinstance(
                dict['meas_upper_limit'], float):
            if long(
                    float(
                        dict["meas_upper_limit"])) == float(
                    dict["meas_upper_limit"]):
                output["meas_upper_limit_l"] = float(dict["meas_upper_limit"])
            else:
                output["meas_upper_limit_d"] = float(dict["meas_upper_limit"])

        if "meas_upper_limit" in dict and (
            isinstance(
                dict['meas_upper_limit'],
                int) or isinstance(
                dict['meas_upper_limit'],
                long)):
            output["meas_upper_limit_l"] = float(dict["meas_upper_limit"])

        return Row(**output)

    @staticmethod
    def get_schema():
        return StructType([
            StructField("company", StringType(), True),
            StructField("device_group", StringType(), True),
            StructField("device_name", StringType(), True),
            StructField("device_status", StringType(), True),
            StructField("meas_datatype", StringType(), True),
            StructField("meas_description", StringType(), True),
            StructField("meas_flag", StringType(), True),
            StructField("meas_method", StringType(), True),
            StructField("meas_name", StringType(), True),
            StructField("meas_status", StringType(), True),
            StructField("meas_unit", StringType(), True),
            StructField("site", StringType(), True),
            StructField("test_name", StringType(), True),
            StructField("test_status", StringType(), True),
            StructField("tester", StringType(), True),
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
            StructField("device_group", StringType(), True),
            StructField("tester", StringType(), True),
            StructField("start_time", LongType(), True),
            StructField("stop_time", LongType(), True),
            StructField("device_name", StringType(), True),
            StructField("test_name", StringType(), True),
            StructField("meas_name", StringType(), True),
            StructField("meas_summary_name", StringType(), True),
            StructField("meas_summary_value", StringType(), True),
            StructField("meas_summary_description", StringType(), True)])

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
            .withColumnRenamed("device_group", "collection") \
            .withColumnRenamed("tester", "dataset") \
            .withColumnRenamed("device_name", "key1") \
            .withColumnRenamed("test_name", "key2") \
            .withColumnRenamed("meas_name", "key3")

        if dest == "measurements_cleansed":
            output_df = df .withColumnRenamed(
                "device_status",
                "val1") .withColumnRenamed(
                "test_status",
                "val2") .withColumn(
                'epoch',
                to_epoch(
                    df['ts'])) .withColumn(
                    'meas_value_l',
                    df['meas_value_l'].cast(
                        LongType())) .withColumn(
                            'meas_lower_limit_l',
                            df['meas_lower_limit_l'].cast(
                                LongType())) .withColumn(
                                    'meas_upper_limit_l',
                                    df['meas_upper_limit_l'].cast(
                                        LongType())) .withColumn(
                                            'meas_value_b',
                                            lit(None).cast(
                                                BinaryType()))

            return output_df.withColumn('ts', to_ts(df['ts']))

        elif dest == "measurements_summary":
            return df \
                .withColumn('start_time', to_ts(df['start_time'])) \
                .withColumn('stop_time', to_ts(df['stop_time']))
        else:
            return df
