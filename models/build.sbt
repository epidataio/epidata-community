import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

name := "epidata-models"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.9.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.1",
  "org.apache.spark" %% "spark-streaming" % "2.4.6" % "provided",
  "org.apache.spark" %% "spark-core" % "2.4.6",
  "org.apache.spark" %% "spark-sql" % "2.4.6",
  "com.googlecode.json-simple" % "json-simple" % "1.1.1"
)

SbtScalariform.scalariformSettings
//scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(AlignParameters, false)
