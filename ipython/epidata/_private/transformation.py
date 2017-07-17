#
# Copyright (c) 2015-2017 EpiData, Inc.
#

class Transformation(object):

    def __init__(
            self,
            func,
            args=[],
            destination="measurements_cleansed",
            datastore="cassandra"):
        self._func = func
        self._destination = destination
        self._args = args
        self._datastore = datastore

    def apply(self, df, sqlCtx):
        return self._func(df, *self._args)

    def destination(self):
        return self._destination

    def datastore(self):
        return self._datastore
