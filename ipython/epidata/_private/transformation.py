#
# Copyright (c) 2015-2017 EpiData, Inc.
#


class Transformation(object):

    def __init__(self, func, meas_names=[], args=[], name="Default", destination="", datastore="sqlite"):
        self._func = func
        self._args = args
        self._name = name
        self._destination = destination
        self._datastore = datastore
        self._meas_names = meas_names

    def apply(self, df, sqlCtx=None):
        return self._func(df, self._meas_names, *self._args)

    def destination(self):
        return self._destination

    def datastore(self):
        return self._datastore

    def __str__(self):
        return self._func.__name__

    def toString(self):
        return self._func.__name__
    
    class Java:
        implements = ['com.epidata.spark.ops.Transformation']
