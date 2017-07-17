from py4j.protocol import Py4JJavaError
import re

# Simplify Py4JJavaError reporting.


def _newPy4jErrorStr(self):
    java_error = self.java_exception.toString()
    match = re.match('^java.lang.[^:]*: (.*)$', java_error)
    return match.group(1) if match else java_error


Py4JJavaError.__str__ = _newPy4jErrorStr
