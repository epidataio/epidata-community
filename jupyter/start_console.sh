#
# Copyright (c) 2015-2017 EpiData, Inc.
#

SPARK_MASTER="spark://127.0.0.1:7077"
SPARK_DRIVER_HOST='127.0.0.1'
CASSANDRA_HOST='127.0.0.1'
CASSANDRA_USER='cassandra'
CASSANDRA_PASSWORD='epidata'
CASSANDRA_KEYSPACE_NAME='epidata_development'
MEASUREMENT_CLASS='sensor_measurement'
KAFKA_BROKERS='localhost:9092'
KAFKA_DURATION='6'

PYSPARK_SUBMIT_ARGS="--jars /home/ubuntu/epidata/spark/target/scala-2.11/epidata-spark-assembly-1.0-SNAPSHOT.jar "\
"--driver-class-path /home/ubuntu/epidata/spark/target/scala-2.11/epidata-spark-assembly-1.0-SNAPSHOT.jar "\
"--driver-java-options '-Dspark.driver.host=$SPARK_DRIVER_HOST -Dspark.cassandra.connection.host=$CASSANDRA_HOST "\
"-Dspark.cassandra.auth.username=$CASSANDRA_USER "\
"-Dspark.cassandra.auth.password=$CASSANDRA_PASSWORD "\
"-Dspark.epidata.kafka.brokers=$KAFKA_BROKERS "\
"-Dspark.epidata.kafka.duration=$KAFKA_DURATION "\
"-Dspark.epidata.cassandraKeyspaceName=$CASSANDRA_KEYSPACE_NAME -Dspark.epidata.measurementClass=$MEASUREMENT_CLASS "\
"-Dspark.cores.max=3' --master $SPARK_MASTER pyspark-shell" \
jupyter console --kernel pyspark
