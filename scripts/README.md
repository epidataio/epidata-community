**Epidata Scripts**

1. Build the jar file
sbt "project scripts" assembly

2. Submit the script to Spark with this command
$SPARK_HOME/bin/spark-submit --class "com.epidata.spark.MeasStatisticsStream" scripts/target/scala-2.11/epidata-scripts-assembly-1.0-SNAPSHOT.jar

3. Kill the app with this command
ps -ef | grep spark |  grep FillMissingValueStream | awk '{print $2}' | xargs kill  -SIGTERM


other example: 

a. $SPARK_HOME/bin/spark-submit --class "com.epidata.spark.FillMissingValueStream" scripts/target/scala-2.11/epidata-scripts-assembly-1.0-SNAPSHOT.jar

b. $SPARK_HOME/bin/spark-submit --class "com.epidata.spark.OutlierDetectorStream" scripts/target/scala-2.11/epidata-scripts-assembly-1.0-SNAPSHOT.jar
