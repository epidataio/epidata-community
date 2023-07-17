#
# Copyright (c) 2015-2022 EpiData, Inc.
#

from epidata import ec, esc
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
from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters
import pandas as pd
import py4j


class EpiDataLiteStreamingContext:
    '''
    Initializes the Java Gateway and creates an entry into the pre-compiled JAR file with
    EpiDataLiteContext.scala
    '''
    def __init__(self,
                esc_classpath=os.environ["EPIDATA_STREAM_PROCESSOR"],
                topics=None,
                sqlite_conf=None,
                zmq_conf=None,
                measurement_class=None
    ):
        self._gateway_parameters = GatewayParameters(address='127.0.0.1', port=25550)
        self._callback_server_parameters = CallbackServerParameters(address='127.0.0.1', port=25551)

        # working code
#        self._gateway = JavaGateway(python_proxy_port=25551, start_callback_server=True, gateway_parameters=self._gateway_parameters, callback_server_parameters=self._callback_server_parameters)

        # alternate code
        self._gateway = JavaGateway(python_proxy_port=25551, start_callback_server=False, gateway_parameters=self._gateway_parameters, callback_server_parameters=self._callback_server_parameters)
#        self._gateway = JavaGateway(gateway_parameters=self._gateway_parameters, callback_server_parameters=self._callback_server_parameters)
        self._gateway.java_gateway_server.resetCallbackClient(
            self._gateway.java_gateway_server.getCallbackClient().getAddress(),
            25551)

        python_port = self._gateway.get_callback_server().get_listening_port()
        #print("python_port:", python_port)
        python_address = self._gateway.java_gateway_server.getCallbackClient().getAddress()
        #print("python_address", python_address)


        py4j_path = os.path.join(os.environ["EPIDATA_HOME"], "ipython/epidata/py4j-0.10.9.2/py4j-java")

        self._sqlite_conf = sqlite_conf
        self._measurement_class = measurement_class

        try:
            self._gg = self._gateway
#            self._gg = JavaGateway.launch_gateway(classpath=esc_classpath, jarpath=os.path.join(py4j_path, 'py4j0.10.9.2.jar'), die_on_exit=True)
            self._jesc = self._gg.entry_point.returnEpiDataLiteStreamingContext()
        except Exception as e:
            print(e)

        self._topics = topics
        self._sqlite_conf = sqlite_conf
        self._zmq_conf = zmq_conf
        self._measurement_class = measurement_class


    '''
    Initializes the EpiDataLiteStreamingContext by opening the required network connections
    '''
    def init(self):
        self._jesc.init()


    '''
    Clears (resets) the EpiDataLiteStreamingContext by closing its network connections
    '''
    def clear(self):
        self._jesc.clear()

        # working code
#        self._gg.close(keep_callback_server=False, close_callback_server_connections=True)

        # alternate code
        self._gg.close(keep_callback_server=True, close_callback_server_connections=False)


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

    def create_transformation(self, opName, meas_names=[], params={}):
        if isinstance(opName, str):
            java_meas_names = ListConverter().convert(meas_names, self._gg._gateway_client)
            # java_params = {k: self.to_java_list(v) for k, v in params.items()}
            java_params = MapConverter().convert(params, self._gg._gateway_client)
            trans = self._jesc.createTransformation(opName, java_meas_names, java_params)
            print("Transformation created:", trans)
            return trans
        else:
            #print("opName is an Object. opName's name:", opName.__name__)
            trans = Transformation(opName, meas_names, params, name=str(opName.__name__), gateway=self._gateway)
            print("Transformation created:", trans)
            return trans

    def create_stream(self, sourceTopic, destinationTopic, transformation):
        self._jesc.createStream(sourceTopic, destinationTopic, transformation)
        print("Data processing stream created")

    def start_streaming(self):
        self._jesc.startStream()
        print("Data processing streams started")

    def stop_streaming(self):
        self._jesc.stopStream()
        print("Data processing streams stopped")

    def to_java_list(self, x):
        if isinstance(x, str): #or str
            return ListConverter().convert([x], self._gg._gateway_client)
        return ListConverter().convert(x, self._gg._gateway_client)


if os.environ.get('EPIDATA_MODE') == r'LITE':
    # The global EpiDataLiteStreamingContext.
    esc = EpiDataLiteStreamingContext()

if __name__ == "__main__":
    pass
