#
# Copyright (c) 2015-2017 EpiData, Inc.
#
import pandas as pd
from py4j.java_collections import SetConverter, MapConverter, ListConverter

class Transformation(object):

    def __init__(self, func, meas_names=[], args=[], name="Default", destination="", datastore="sqlite", gateway = None):
        self._func = func
        self._args = args
        self.name = name
        self.destination = destination
        self._datastore = datastore
        self._meas_names = meas_names
        self._gateway = gateway

    def apply(self, df, sqlCtx=None):
        if sqlCtx == None:
            df = list(df)
            df = pd.DataFrame.from_records(df)
            df = self._func(df, self._meas_names, **self._args)
            df = df.to_dict('records')
            for i in range(len(df)):
                df[i] = MapConverter().convert(df[i], self._gateway._gateway_client)
            df = ListConverter().convert(df, self._gateway._gateway_client)
            return df
        return self._func(df, self._meas_names, *self._args)


    def destination(self):
        return self.destination

    def datastore(self):
        return self._datastore

    def __str__(self):
        return self._func.__name__

    def toString(self):
        return self._func.__name__

    class Java:
        implements = ['com.epidata.spark.ops.Transformation']
