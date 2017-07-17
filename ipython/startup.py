#
# Copyright (c) 2015-2017 EpiData, Inc.
#

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
print 'EpidataContext available as ec.'
print 'Type help(epidata) for help.'
