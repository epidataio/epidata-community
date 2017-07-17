# Set up the parameters.
PYSPARK=`which pyspark`
SPARK_HOME="$(cd "`dirname "$PYSPARK"`"/..; pwd)"
PYTHONPATH=..:../../python:$SPARK_HOME/python:$SPARK_HOME/python/build

# Build the documentation.
SPARK_HOME=$SPARK_HOME \
PYTHONPATH=$PYTHONPATH \
make html

# Copy the documentation to the play static assets directory.
mkdir ../../play/public/doc
cp -r _build/html/* ../../play/public/doc
