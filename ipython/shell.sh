#
# Copyright (c) 2015-2017 EpiData, Inc.
#

PYSPARK=`which pyspark`
SPARK_HOME="$(cd "`dirname "$PYSPARK"`"/..; pwd)"

DOCKER_HOST_IP=`ifconfig docker0 | awk 'NR==2 {print $2}' | sed s/addr://`
SPARK_DRIVER_HOST='127.0.0.1'
LOCAL_HOST_IP='127.0.0.1'
CASSANDRA_HOST='127.0.0.1'
CASSANDRA_USER='cassandra'
CASSANDRA_PASSWORD='epidata'
CASSANDRA_KEYSPACE_NAME='epidata_development'
MEASUREMENT_CLASS='sensor_measurement'
KAFKA_BROKERS='localhost:9092'
KAFKA_DURATION='6'
SQLITE_DB = '../data/epidata_development.db'

SPARK_HOME=$SPARK_HOME \
EPIDATA_PYTHON_HOME=../python \
EPIDATA_IPYTHON_HOME=. \
SPARK_CONF_DIR=../spark/conf/ \
PYSPARK_SUBMIT_ARGS="--jars ../spark/target/scala-2.11/epidata-spark-assembly-1.0-SNAPSHOT.jar "\
"--driver-class-path ../spark/target/scala-2.11/epidata-spark-assembly-1.0-SNAPSHOT.jar "\
"--driver-java-options "\
"'-Dspark.driver.host=$SPARK_DRIVER_HOST "\
"-Dspark.cassandra.connection.host=$CASSANDRA_HOST "\
"-Dspark.cassandra.auth.username=$CASSANDRA_USER "\
"-Dspark.cassandra.auth.password=$CASSANDRA_PASSWORD "\
"-Dspark.epidata.cassandraKeyspaceName=$CASSANDRA_KEYSPACE_NAME "\
"-Dspark.epidata.measurementClass=$MEASUREMENT_CLASS "\
"-Dspark.epidata.kafka.brokers=$KAFKA_BROKERS "\
"-Dspark.epidata.kafka.duration=$KAFKA_DURATION "\
"-Dspark.cores.max=3' pyspark-shell" \
SPARK_MASTER=spark://$LOCAL_HOST_IP:7077 \
ipython --pylab --ipython-dir=config --profile=default
