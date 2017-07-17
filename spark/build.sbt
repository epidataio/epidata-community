name := "epidata-spark"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.datastax.spark" % "spark-cassandra-connector-unshaded_2.11" % "2.0.0-M3",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.2",
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.1.0" % "provided",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.1.0",
  "com.datastax.spark" % "spark-cassandra-connector-embedded_2.11" % "2.0.1" % "test",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "junit" % "junit" % "4.12" % "test",
  "org.apache.cassandra" % "cassandra-all" % "3.2" % "test"
).map(_.exclude("org.slf4j", "log4j-over-slf4j"))  // Excluded to allow for Cassandra to run embedded

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

test in assembly := {}

Keys.fork in Test := true

scalariformSettings

testOptions in Test += Tests.Argument("-oF")
