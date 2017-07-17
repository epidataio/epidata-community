name := "epidata-models"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.2",
  "com.googlecode.json-simple" % "json-simple" % "1.1.1"
)

scalariformSettings

scalaVersion := "2.11.8"

scalaVersion in ThisBuild := "2.11.8"

