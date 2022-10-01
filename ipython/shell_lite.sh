#
# Copyright (c) 2015-2017 EpiData, Inc.
#

# Set measurement class to 'automated_test' or 'sensor_measurement'
MEASUREMENT_CLASS='automated_test'
SQLITE_DB='../data/epidata_development.db'

EPIDATA_PYTHON_HOME=../python \
EPIDATA_IPYTHON_HOME=. \
jupyter console --config=jupyter_console_config.py

#ipython --pylab --ipython-dir=config --profile=default
