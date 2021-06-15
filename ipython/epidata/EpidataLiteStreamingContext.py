#from data_frame import DataFrame
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
    def __init__(self):
        self.gateway = JavaGateway()
        #other confs and connections if needed

        #works with an absolute path as well
        self.gg = self.gateway.launch_gateway(classpath="../../spark/target/scala-2.12/epidata-spark-assembly-1.0-SNAPSHOT.jar")
        self.java_entry = self.gg.jvm.com.epidata.spark.EpidataLiteStreamingContext()
        self.java_entry.init()

    '''
    Converts a python list of dictionaries to a Pandas dataframe
    '''
    def to_pandas_dataframe(self, list_of_dicts):
        #print(type(list_of_dicts))
        pdf = pd.DataFrame.from_records(list_of_dicts)
        return pdf

    '''
    CreateTransformations method for epidata streaming

    Parameters
    ----------
    OpName : ?
    
    meas_names : List of String
        
    params : Dictionary [String, String]
       

    Returns
    -------
    A Pandas dataframe containing measurements matching the query
    '''

    def create_transformations(self, opName, meas_names, params):
        java_meas_names = ListConverter().convert(meas_names, self.gg._gateway_client)
        # java_params = {k: self.to_java_list(v) for k, v in params.items()}
        java_params = MapConverter().convert(params, self.gg._gateway_client)
        trans = self.java_entry.createTransformations(opName, java_meas_names, java_params)
        return trans

    def create_stream(self, sourceTopic, destinationTopic, transformation):
        print("start creating")
        # self.java_entry.testUnit()
        self.java_entry.createStream(sourceTopic, destinationTopic, transformation)


    def start_stream(self):
        self.java_entry.startStream()

    def stop_stream(self):
        self.java_entry.stopStream()

    def to_java_list(self, x):
        if isinstance(x, basestring): #or str
            return ListConverter().convert([x], self.gg._gateway_client)
        return ListConverter().convert(x, self.gg._gateway_client)

    def printSomething(self, something):
        s = self.java_entry.printSomething(something)
        print(s)



    #streaming, transformation methods as needed















