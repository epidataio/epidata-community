#
# Copyright (c) 2015-2022 EpiData, Inc.
#

#from streaming import EpidataStreamingContext
from datetime import datetime
import _private.py4j_additions
import json
import os
import signal
from py4j.java_collections import ListConverter, MapConverter
import re
import time
import urllib
from threading import Thread
from _private.transformation import Transformation
from py4j.java_gateway import JavaGateway
import pandas as pd
import py4j


class EpidataLiteStreamingContext:
    '''
    Initializes the Java Gateway and creates an entry into the pre-compiled JAR file with
    EpidataLiteContext.scala
    '''
    def __init__(self,
                elc_classpath="spark/target/scala-2.12/epidata-spark-assembly-1.0-SNAPSHOT.jar",
                topics=None,
                sqlite_conf=None,
                zmq_conf=None,
                measurement_class=None
    ):
        debug_classpath = "/Users/srinibadri/Documents/Repos/epidata/epidata-community-interns/spark/target/scala-2.12/epidata-spark-assembly-1.0-SNAPSHOT.jar"
        self._gateway = JavaGateway()
#        self._gg = self._gateway.launch_gateway(classpath = elc_classpath)
        self._gg = self._gateway.launch_gateway(classpath = debug_classpath)
        self._jelc = self._gg.jvm.com.epidata.spark.EpidataLiteStreamingContext()
        self._topics = topics
        self._sqlite_conf = sqlite_conf
        self._zmq_conf = zmq_conf
        self._measurement_class = measurement_class

        self._jelc.init()


    '''
    Converts a python list of dictionaries to a Pandas dataframe
    '''
    def to_pandas_dataframe(self, list_of_dicts):
        pdf = pd.DataFrame.from_records(list_of_dicts)
        return pdf

    '''
    CreateTransformation method for epidata streaming

    Parameters
    ----------
    OpName : Name of the trasnformation

    meas_names : List of String

    params : Dictionary [String, String]


    Returns
    -------
    A Pandas dataframe containing measurements matching the query
    '''

    def create_transformation(self, opName, meas_names, params):
        java_meas_names = ListConverter().convert(meas_names, self._gg._gateway_client)
        # java_params = {k: self.to_java_list(v) for k, v in params.items()}
        java_params = MapConverter().convert(params, self._gg._gateway_client)
        trans = self._jelc.createTransformation(opName, java_meas_names, java_params)
        return trans

    def create_stream(self, sourceTopic, destinationTopic, transformation):
        self._jelc.createStream(sourceTopic, destinationTopic, transformation)
        print("stream created")

    def start_streaming(self):
        self._jelc.startStream()
        print("streams started")

    def stop_streaming(self):
        self._jelc.stopStream()
        print("streams stopped")

    def to_java_list(self, x):
        if isinstance(x, str): #or str
            return ListConverter().convert([x], self._gg._gateway_client)
        return ListConverter().convert(x, self._gg._gateway_client)

    def printSomething(self, something):
        s = self._jelc.printSomething(something)
        print(s)


if os.environ.get('EPIDATA_MODE') == r'LITE':
    # The global EpidataLiteStreamingContext.
    esc = EpidataLiteStreamingContext()
