import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

name := "epidata-spark"

resolvers += Resolver.jcenterRepo

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % "2.7.4",
  "org.xerial" % "sqlite-jdbc" % "3.32.3.3",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.9.0",
  "org.apache.spark" %% "spark-core" % "2.4.6",
  "org.apache.spark" %% "spark-sql" % "2.4.6",
  "org.apache.spark" %% "spark-streaming" % "2.4.6" % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.6",
  "org.apache.kafka" % "kafka-streams" % "2.4.1",
  "com.datastax.spark" %% "spark-cassandra-connector-embedded" % "2.4.3" % Test,
  "org.scalatest" %% "scalatest" % "3.1.4" % Test,
  "org.scalatestplus" %% "junit-4-12" % "3.1.2.0" % "test",
  "junit" % "junit" % "4.12" % Test,
  "org.apache.cassandra" % "cassandra-all" % "3.11.6"
).map(_.exclude("org.slf4j", "log4j-over-slf4j"))
.map(_.exclude("org.slf4j", "org.slf4j.impl.Log4jLoggerFactory"));  // Excluded for Cassandra embedded

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.10.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.5"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.10.5"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


test in assembly := {}

Keys.fork in Test := true

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(AlignParameters, false)

libraryDependencies += "ru.dgis" %% "reactive-zmq" % "0.4.0"

testOptions in Test += Tests.Argument("-oF")
