#!/bin/sh
#
# Copyright (c) 2015-2022 EpiData, Inc.
#

export EPIDATA_HOME="C:/Users/Raj Sunku/OneDrive - UC San Diego/Epidata/epidata-community"
export EPIDATA_PYTHON_HOME=$EPIDATA_HOME/python
export EPIDATA_IPYTHON_HOME=$EPIDATA_HOME/ipython
export PYTHONPATH="$PYTHONPATH;$EPIDATA_IPYTHON_HOME;$EPIDATA_IPYTHON_HOME/epidata;$EPIDATA_IPYTHON_HOME/epidata/_private;$EPIDATA_IPYTHON_HOME/epidata/py4j-0.10.9.2/src/py4j"
export EPIDATA_MODE=LITE
export EPIDATA_LITE_JAR="$EPIDATA_HOME/spark/target/scala-2.12/epidata-spark-assembly-1.0-SNAPSHOT.jar"

#ipython notebook --ipython-dir=config --profile=default
java -jar "$EPIDATA_LITE_JAR"
jupyter notebook --config config.py
