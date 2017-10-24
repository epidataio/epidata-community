#
# Copyright (c) 2015-2017 EpiData, Inc.
#

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
from kafka import KafkaClient
import json

from epidata._private.utils import ConvertUtils
from sensor_measurement import SensorMeasurement
from automated_test import AutomatedTest


class EpidataStreamingContext:

    def __init__(
            self,
            sc=None,
            ssc=None,
            sql_ctx=None,
            topics=None,
            brokers=None,
            cassandra_conf=None,
            measurement_class=None
    ):
        self._sc = sc
        self._sql_ctx = sql_ctx
        self._topics = topics
        self._ssc = ssc
        self._brokers = brokers
        self._cassandra_conf = cassandra_conf
        self._measurement_class = measurement_class

        # set up Schema
        self._sensor_measurement_schema = SensorMeasurement.get_schema()
        self._sensor_measurement_stats_schema = SensorMeasurement.get_stats_schema()
        self._automated_test_schema = AutomatedTest.get_schema()
        self._automated_test_stats_schema = AutomatedTest.get_stats_schema()

        self._kafka_producer = KafkaProducer(bootstrap_servers=self._brokers)
        self._client = KafkaClient(self._brokers)

    def run_stream(self, ops, clean_up=True):

        self._client.ensure_topic_exists(self._topics)
        kvs = KafkaUtils.createDirectStream(
            self._ssc, [
                self._topics], {
                "metadata.broker.list": self._brokers})

        if self._measurement_class == "sensor_measurement":
            rows = kvs.map(SensorMeasurement.to_row)
        elif self._measurement_class == "automated_test":
            rows = kvs.map(AutomatedTest.to_row)

        def process(time, rdd):
            if rdd.isEmpty() == False:

                rdd_df = self._sql_ctx.createDataFrame(rdd)

                # convert to panda dataframe
                panda_df = ConvertUtils.convert_to_pandas_dataframe_model(
                    rdd_df, clean_up)

                # perform all transformation and save it to cassandra
                for op in ops:

                    # try:
                    # apply transformation
                    output_df = op.apply(panda_df, self._sql_ctx)

                    if not output_df.empty:

                        if op.datastore() == "cassandra":

                            # clean up unnecessary column
                            output_df = ConvertUtils.convert_meas_value(
                                output_df, op.destination())


                            # convert it back to spark data frame
                            spark_output_df = self._sql_ctx.createDataFrame(
                                output_df, self._get_schema(op.destination()))

                            # convert to db model to save to cassandra
                            output_df_db = self._convert_to_db_model(
                                spark_output_df, op.destination())

                            # save to cassandra
                            output_df_db.write .format("org.apache.spark.sql.cassandra") .mode('append') .options(
                                table=op.destination(),
                                keyspace=self._cassandra_conf['keyspace'],
                                user=self._cassandra_conf['user'],
                                password=self._cassandra_conf['password']) .save()

                        elif op.datastore() == "kafka":

                            output_df_kafka = output_df

                            for i in output_df_kafka.index:
                                row_json = output_df_kafka.loc[i].to_json()

                                # push to kafka
                                self._kafka_producer.send(
                                    op.destination(), row_json)

                            # Flush kakfa producer
                            self._kafka_producer.flush()

                    # except BaseException:
                    #     print("Failed transformation: " + op.destination())

        rows.foreachRDD(process)

    def _start(self):
        self._ssc.start()
        self._ssc.awaitTermination()

    def _get_schema(self, destination):
        if destination == "measurements_summary":
            if self._measurement_class == "sensor_measurement":
                return self._sensor_measurement_stats_schema
            elif self._measurement_class == "automated_test":
                return self._automated_test_stats_schema
        else:
            if self._measurement_class == "sensor_measurement":
                return self._sensor_measurement_schema
            elif self._measurement_class == "automated_test":
                return self._automated_test_schema

    def _convert_to_db_model(self, input_df, dest):
        if self._measurement_class == "sensor_measurement":
            return SensorMeasurement.convert_to_db_model(input_df, dest)
        elif self._measurement_class == "automated_test":
            return AutomatedTest.convert_to_db_model(input_df, dest)
