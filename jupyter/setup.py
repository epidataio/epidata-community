#
# Copyright (c) 2015-2017 EpiData, Inc.
#

import os
import sys

spark_home = os.environ['SPARK_HOME']
sys.path.insert(0, os.path.join(spark_home, 'python'))
sys.path.insert(0, os.path.join(spark_home, 'python/lib/py4j-0.10.4-src.zip'))

epidata_python_home = os.environ['EPIDATA_PYTHON_HOME']
sys.path.insert(0, os.environ['EPIDATA_PYTHON_HOME'])
sys.path.insert(0, os.environ['EPIDATA_IPYTHON_HOME'])

try:
    # Stop existing SparkContext, if present.
    sc.stop()
except BaseException:
    pass

# Automatically import the epidata module.
import epidata

# Make the global EpidataContext ec available.
from epidata.context import ec

print
print '------------------'
print 'Welcome to Epidata'
print '------------------'
print
print 'EpidataContext available as ec'

def run_magics():
    from IPython import get_ipython
    ipython = get_ipython()
    ipython.magic("matplotlib inline")

run_magics()
