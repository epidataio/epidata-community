#
# Copyright (c) 2015-2017 EpiData, Inc.
#

PYSPARK=`which pyspark`
SPARK_HOME="$(cd "`dirname "$PYSPARK"`"/..; pwd)"

DOCKER_HOST_IP=`ifconfig docker0 | awk 'NR==2 {print $2}' | sed s/addr://`
CASSANDRA_USER='cassandra'
CASSANDRA_PASSWORD='epidata'
KAFKA_BROKERS='localhost:9092'
KAFKA_DURATION='6'

SPARK_HOME=$SPARK_HOME \
EPIDATA_PYTHON_HOME=../python \
EPIDATA_IPYTHON_HOME=. \
SPARK_CONF_DIR=../spark/conf/ \
PYSPARK_SUBMIT_ARGS="--jars ../spark/target/scala-2.11/epidata-spark-assembly-1.0-SNAPSHOT.jar "\
"--driver-class-path ../spark/target/scala-2.11/epidata-spark-assembly-1.0-SNAPSHOT.jar "\
"--driver-java-options -Dspark.cassandra.connection.host=$DOCKER_HOST_IP " \
"-Dspark.cassandra.auth.username=$CASSANDRA_USER "\
"-Dspark.cassandra.auth.password=$CASSANDRA_PASSWORD "\
"-Dspark.epidata.kafka.brokers=$KAFKA_BROKERS "\
"-Dspark.epidata.kafka.duration=$KAFKA_DURATION "\
SPARK_MASTER=spark://$DOCKER_HOST_IP:7077 \
ipython notebook --ipython-dir=config --profile=default
