import sys
import os
py4j_src = os.path.join(os.path.dirname(__file__), 'py4j-0.10.9.2/src')
py4j_jar = os.path.join(os.path.dirname(__file__), 'py4j-0.10.9.2/py4j-java/py4j0.10.9.2.jar')
sys.path.insert(0, py4j_src)
sys.path.insert(0, py4j_jar)

import EpidataLiteContext

# import context
# from data_frame import DataFrame

__doc__ = """
epidata - Tools for querying and analyzing measurements stored on a remote cluster.
===================================================================================

Additional documentation is available for the following:

epidata.context
epidata.DataFrame

For example, help(epidata.DataFrame) provides information on DataFrame.
"""
