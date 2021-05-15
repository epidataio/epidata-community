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


class EpidataLiteContext:

    '''
    Initializes the Java Gateway and creates an entry into the pre-compiled JAR file with 
    EpidataLiteContext.scala
    ''' 
    def __init__(self):
        self.gateway = JavaGateway()
        #other confs and connections if needed

        #works with an absolute path as well
        self.gg = self.gateway.launch_gateway(classpath="spark/target/scala-2.12/epidata-spark-assembly-1.0-SNAPSHOT.jar")
        self.java_entry = self.gg.jvm.com.epidata.spark.EpidataLiteContext() 

    '''
    Converts a python list of dictionaries to a Pandas dataframe
    '''   
    def to_pandas_dataframe(self, list_of_dicts):
        #print(type(list_of_dicts))
        pdf = pd.DataFrame.from_records(list_of_dicts) 
        return pdf
   
    '''
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
    A Pandas dataframe containing measurements matching the query
    '''
    
    def query_measurements_original(self, field_query, begin_time, end_time):
        java_field_query, java_begin_time, java_end_time = self._to_java_params(field_query, begin_time, end_time)
        #calling the scala code's query method
        java_df = self.java_entry.query(java_field_query, java_begin_time, java_end_time)
        #manual workaround for the case in which scala code returns an empty list to ensure no errors are thrown
        if isinstance(java_df, py4j.java_collections.JavaList):
            if java_df.size() == 0:
                java_df = []
            else:
                java_df = list(java_df)
        #conversion to Pandas dataframe
        pdf = self.to_pandas_dataframe(java_df)
        return pdf
        

    def query_measurements_cleansed(self, field_query, begin_time, end_time):
        java_field_query, java_begin_time, java_end_time = self._to_java_params(field_query, begin_time, end_time)
        java_df = self.java_entry.queryMeasurementCleansed(java_field_query, java_begin_time, java_end_time)
        #manual workaround for the case in which scala code returns an empty list to ensure no errors are thrown
        if isinstance(java_df, py4j.java_collections.JavaList):
            if java_df.size() == 0:
                java_df = []
            else:
                java_df = list(java_df)
        #conversion to Pandas dataframe
        pdf = self.to_pandas_dataframe(java_df)
        return pdf
 
    def query_measurements_summary(self, field_query, begin_time, end_time):
    
        java_field_query, java_begin_time, java_end_time = self._to_java_params(field_query, begin_time, end_time)
        java_df = self.java_entry.queryMeasurementSummary(java_field_query, java_begin_time, java_end_time)
        #manual workaround for the case in which scala code returns an empty list to ensure no errors are thrown
        if isinstance(java_df, py4j.java_collections.JavaList):
            if java_df.size() == 0:
                java_df = []
            else:
                java_df = list(java_df)
        #conversion to Pandas dataframe
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
        #print(type(java_df))
        if isinstance(java_df, py4j.java_collections.JavaList):
            if java_df.size() == 0:
                java_df = []
            else:
                java_df = list(java_df)
        return self.to_pandas_dataframe(java_df) 

    def _to_java_params(self, field_query, begin_time, end_time):
        #creates a gateway client for the Java Gateway
        self.gc = self.gg._gateway_client

        #converts to a Java List
        def to_java_list(x):
            if isinstance(x, basestring): #or str
                return ListConverter().convert([x], self.gc)
            return ListConverter().convert(x, self.gc)
        
        #converts to appropriate Java parameters
        java_list_field_query = {k: to_java_list(v) for k, v in field_query.items()}
        java_field_query = MapConverter().convert(java_list_field_query, self.gc)
        java_begin_time = self._to_java_timestamp(begin_time)
        java_end_time = self._to_java_timestamp(end_time)
        return java_field_query, java_begin_time, java_end_time


    def _to_java_timestamp(self, dt):
        stamp = time.mktime(dt.timetuple()) * 1e3 + dt.microsecond / 1e3
        timestamp = int(float(stamp))
        return self.gg.jvm.java.sql.Timestamp(timestamp)

    def _check_cluster_memory(self):
        pass  #not needed with the lite version
        
    #streaming, transformation methods as needed 



#some extra code for the purposes of testing/debugging each query method

'''
from datetime import datetime, timedelta
ec = EpidataLiteContext() 
#print(ec.to_pandas_dataframe([ {"hi": "hi"}, {"two": "three"}]))
#print(ec.list_keys())
print

#ts = [datetime.fromtimestamp(1428004316.123 + x) for x in range(6)]
#result = ec.query_measurements_original({'company': 'Company-1', 'site': 'Site-1','device_group': '1000','tester': 'Station-1','test_name': 'Test-1'}, ts[0], ts[0] + timedelta(seconds=0.5))
#df2 = ec.query_measurements_original({'company': 'Company-1','site': 'Site-1','device_group': '1000','tester': 'Station-1','test_name': 'Test-1'},ts[0],ts[3] + timedelta(seconds=0.5))

#2015
#ts3 = [datetime.fromtimestamp(1428004316.123 + x) for x in range(6)]
#df3 = ec.query_measurements_original({'company': 'Company-1','site': 'Site-1', 'device_group': '1000', 'tester': 'Station-1', 'test_name': 'Test-3'}, ts3[0], ts3[5] + timedelta(seconds=0.5) )

#2016
ts4 = [datetime.fromtimestamp(1463336206.123 + x) for x in range(6)]
df4 = ec.query_measurements_original({'company': 'Company-1','site': 'Site-1', 'device_group': '1000', 'tester': 'Station-1', 'test_name': 'Test-3'}, ts4[0], ts4[5] + timedelta(seconds=0.5) )

#2017
ts5 = [datetime.fromtimestamp(1494872206.123 + x) for x in range(6)]
df5 = ec.query_measurements_original({'company': 'Company-1','site': 'Site-1', 'device_group': '1000', 'tester': 'Station-1', 'test_name': 'Test-3'}, ts5[0], ts5[5] + timedelta(seconds=0.5) )


#print(result)
#print
#print(df2)
#print
#print(df3)
#print
print(df4)
print
print(df5)
print
'''















