import os
import sys

sys.path.insert(0, os.environ['EPIDATA_PYTHON_HOME'])
sys.path.insert(0, os.environ['EPIDATA_IPYTHON_HOME'])
from epidata.context import ec

print
print '------------------'
print 'Welcome to Epidata'
print '------------------'
print
print 'EpidataContext available as ec'
