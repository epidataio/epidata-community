#
# Copyright (c) 2015-2017 EpiData, Inc.
#

from data_frame import DataFrame
from datetime import datetime
import epidata._private.py4j_additions
import json
import os
import signal
from py4j.java_collections import ListConverter, MapConverter
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext
from pyspark.java_gateway import java_import
import re
import time
import urllib

from pyspark.streaming import StreamingContext

from threading import Thread
from streaming import EpidataStreamingContext
from epidata._private.transformation import Transformation


class EpidataContext:

    """
    A connection to the epidata server, and all relevant context.
    """

    def __init__(self):

        spark_conf = SparkConf()
        self._sc = SparkContext(
            os.environ['SPARK_MASTER'],
            'epidata',
            conf=spark_conf)

        # get epidata spark conf
        conf = self._sc.getConf()

        cassandra_user = conf.get(
            'spark.cassandra.auth.username', 'cassandra')
        cassandra_pass = conf.get(
            'spark.cassandra.auth.password', 'epidata')
        cassandra_host = conf.get(
            'spark.cassandra.connection.host', '127.0.0.1')
        cassandra_keyspace = conf.get(
            'spark.epidata.cassandraKeyspaceName',
            'epidata_development')
        kafka_brokers = conf.get(
            'spark.epidata.kafka.brokers', 'localhost:9092')
        kafka_batch_duration = int(conf.get(
            'spark.epidata.kafka.duration', '6'))
        self._measurement_class = conf.get(
            'spark.epidata.measurementClass', 'sensor_measurement')

        java_import(self._sc._jvm, "com.epidata.spark.EpidataContext")
        self._jec = self._sc._jvm.EpidataContext(self._sc._jsc)

        self._sql_ctx = SQLContext(self._sc, self._jec.getSQLContext())
        self._sql_ctx_pyspark = SQLContext(self._sc)
        self._cassandra_conf = {
            'keyspace': cassandra_keyspace,
            'user': cassandra_user,
            'password': cassandra_pass}
        self._has_checked_memory = False
        self._kafka_broker = os.environ.get('KAFKA_BROKER', kafka_brokers)
        self._batch_duration = kafka_batch_duration
        self._ssc = StreamingContext(self._sc, self._batch_duration)

    def query_measurements_original(self, field_query, begin_time, end_time):
        """
        Query for epidata measurements.

        Parameters
        ----------
        field_query : dictionary containing either strings or lists of strings
            A dictionary containing field names and the values those fields must
            contain in matching measurements. Some system configurations require
            that values of specific fields be specified. A string field value
            represents an equality match, while a list value represents set
            membership (all values within the set are matched).
        begin_time : datetime
            Beginning of the time interval to query, inclusive.
        end_time : datetime
            End of the time interval to query, exclusive.

        Returns
        -------
        result : epidata DataFrame
            A DataFrame containing measurements matching the query.
        """
        self._check_cluster_memory()

        java_field_query, java_begin_time, java_end_time = self._to_java_params(
            field_query, begin_time, end_time)

        java_data_frame = self._jec.query(
            java_field_query,
            java_begin_time,
            java_end_time)
        return DataFrame(jdf=java_data_frame, sql_ctx=self._sql_ctx)

    def query_measurements_cleansed(self, field_query, begin_time, end_time):

        self._check_cluster_memory()

        java_field_query, java_begin_time, java_end_time = self._to_java_params(
            field_query, begin_time, end_time)

        java_data_frame = self._jec.queryMeasurementCleansed(
            java_field_query,
            java_begin_time,
            java_end_time)
        return DataFrame(jdf=java_data_frame, sql_ctx=self._sql_ctx)

    def query_measurements_summary(self, field_query, begin_time, end_time):

        self._check_cluster_memory()

        java_field_query, java_begin_time, java_end_time = self._to_java_params(
            field_query, begin_time, end_time)

        java_data_frame = self._jec.queryMeasurementSummary(
            java_field_query,
            java_begin_time,
            java_end_time)
        return DataFrame(jdf=java_data_frame, sql_ctx=self._sql_ctx)

    def create_stream(self, ops, original="measurements", clean_up=True):
        esc = EpidataStreamingContext(
            self._sc,
            self._ssc,
            self._sql_ctx_pyspark,
            original,
            self._kafka_broker,
            self._cassandra_conf,
            self._measurement_class)
        esc.run_stream(ops, clean_up)

    def start_streaming(self):
        def _start():
            self._ssc.start()
            self._ssc.awaitTermination()
        thread = Thread(target=_start)
        thread.start()

    def stop_streaming(self):
        self._ssc.stop(False, True)
        self._ssc = StreamingContext(self._sc, self._batch_duration)

    def create_transformation(
            self,
            func,
            args=[],
            destination="measurements_cleansed"):
        cassandra_tables = [
            'measurements',
            'measurements_original',
            'measurements_raw',
            'measurements_cleansed',
            'measurements_processed',
            'measurements_summary',
            'measurements_aggregates']
        datastore = "cassandra" if destination in cassandra_tables else "kafka"
        return Transformation(func, args, destination, datastore)

    def list_keys(self):
        """
        List the epidata measurement keys.

        Returns
        -------
        result : epidata DataFrame
            A DataFrame containing values of the principal fields used for
            classifying measurements.
        """
        self._check_cluster_memory()
        return DataFrame(jdf=self._jec.listKeys(), sql_ctx=self._sql_ctx)

    def _check_cluster_memory(self):
        if self._has_checked_memory:
            return
        try:
            spark_ip = re.match(
                'spark://(.*):\\d+',
                os.environ['SPARK_MASTER']).group(1)
            clusterStatus = json.loads(
                urllib.urlopen(
                    'http://' +
                    spark_ip +
                    ':18080/json').read())
            if clusterStatus['memory'] - clusterStatus['memoryused'] < 3 * 512:
                raise MemoryError('All cluster memory resources are in use.')
        except MemoryError:
            raise
        except Exception as e:
            print e
            pass
        self._has_checked_memory = True

    def _to_java_params(self, field_query, begin_time, end_time):

        gc = self._sc._gateway._gateway_client

        def to_java_list(x):
            if isinstance(x, basestring):
                return ListConverter().convert([x], gc)
            return ListConverter().convert(x, gc)

        java_list_field_query = {k: to_java_list(v)
                                 for k, v in field_query.items()}
        java_field_query = MapConverter().convert(java_list_field_query, gc)
        java_begin_time = self._to_java_timestamp(begin_time)
        java_end_time = self._to_java_timestamp(end_time)

        return java_field_query, java_begin_time, java_end_time

    def _to_java_timestamp(self, dt):
        ts = long(time.mktime(dt.timetuple()) * 1e3 + dt.microsecond / 1e3)
        return self._sc._jvm.java.sql.Timestamp(ts)


if os.environ.get('SPARK_MASTER') and (os.environ.get('EPIDATA_MODE') == r'BIG_DATA'):
    # The global EpidataContext.
    ec = EpidataContext()
