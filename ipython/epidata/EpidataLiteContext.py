#from data_frame import DataFrame
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
#from streaming import EpidataStreamingContext
from _private.transformation import Transformation
from py4j.java_gateway import JavaGateway
import pandas as pd

'''
from py4j.java_gateway import JavaGateway 

gate = JavaGateway() 
gg = gate.launch_gateway(classpath="./gatewayclass/JarExample.jar")

myclass_instance = gg.jvm.gatewayclass.MyClass()
result = myclass_instance.my_method()
'''

class EpidataLiteContext:
    def __init__(self):
        gateway = JavaGateway()
        #jvm = gateway.jvm
        #java_import(self._sc._jvm, "com.epidata.spark.EpidataContext")
        #self._jec = self._sc._jvm.EpidataContext(self._sc._jsc)
        #other confs and connections
        gg = gateway.launch_gateway(classpath="../../spark/target/scala-2.12/epidata-spark-assembly-1.0-SNAPSHOT.jar")
        java_entry = gg.jvm.com.epidata.spark.EpidataLiteContext() 


        
    def to_pandas_dataframe(self, list_of_dicts):
        pdf = pd.DataFrame.from_records(list_of_dicts) 
        return pdf
   
    """
    Query methods for epidata measurements

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
    A dataframe (pandas? Epidata?) containing measurements matching the query
    """
    
    def query_measurements_original(self, field_query, begin_time, end_time):
        java_field_query, java_begin_time, java_end_time = self._to_java_params(field_query, begin_time, end_time)
        java_df = self.java_entry.query(java_field_query, java_begin_time, java_end_time)
        pdf = self.to_pandas_dataframe(java_df)
        return pdf
        

    def query_measurements_cleansed(self, field_query, begin_time, end_time):
        java_field_query, java_begin_time, java_end_time = self._to_java_params(field_query, begin_time, end_time)
        java_df = self.java_entry.queryMeasurementCleansed(java_field_query, java_begin_time, java_end_time)
        pdf = self.to_pandas_dataframe(java_df)
        return pdf
 
    def query_measurements_summary(self, field_query, begin_time, end_time):
        java_field_query, java_begin_time, java_end_time = self._to_java_params(field_query, begin_time, end_time)
        java_df = self.java_entry.queryMeasurementSummary(java_field_query, java_begin_time, java_end_time)
        pdf = self.to_pandas_dataframe(java_df)
        return pdf

    def list_keys(self):
        """
        List the epidata measurement keys.

        Returns
        -------
        result : epidata DataFrame
            A DataFrame containing values of the principal fields used for
            classifying measurements.
        """
        java_df = self.java_entry.listKeys() 
        return self.to_pandas_dataframe(java_df) #does/should this return pandas dataframe or epidata dataframe? 

    def _to_java_params(self, field_query, begin_time, end_time):
        #gc = gateway.gateway_client #????
 
        def to_java_list(x):
            if isinstance(x, basestring):
                return ListConverter().convert([x])
            return ListConverter().convert(x)
        
        java_list_field_query = {k: to_java_list(v) for k, v in field_query.items()}
        java_field_query = MapConverter().convert(java_list_field_query)
        java_begin_time = self._to_java_timestamp(begin_time)
        java_end_time = self._to_java_timestamp(end_time)
        return java_field_query, java_begin_time, java_end_time


    def _to_java_timestamp(self, dt):
        stamp = time.mktime(dt.timetuple()) * 1e3 + dt.microsecond / 1e3
        timestamp = long(stamp)
        return self.gg.jvm.java.sql.Timestamp(timestamp)

    def _check_cluster_memory(self):
        pass  #not needed with the lite version?
        
    #streaming, transformation methods as needed 


