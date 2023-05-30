import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._
import scala.sys.process._

name := "epidata-stream-processor"
maintainer := "EpiData, Inc."

scalaVersion := "2.12.11"

resolvers += Resolver.jcenterRepo
resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"
resolvers ++= Seq(
  "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
  Resolver.jcenterRepo
)

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
  "org.apache.cassandra" % "cassandra-all" % "3.11.6",
  "com.google.guava" % "guava" % "31.1-jre",
  "net.sf.py4j" % "py4j" % "0.10.9.2"
).map(_.exclude("org.slf4j", "log4j-over-slf4j"));  // Excluded for Cassandra embedded

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.10.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.10.5"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.10.5"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case manifest if manifest.contains("MANIFEST.MF") =>
    // We don't need manifest files since sbt-assembly will create
    // one with the given settings
    MergeStrategy.discard
  case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") =>
    // Keep the content for all reference-overrides.conf files
    MergeStrategy.concat
  case x => MergeStrategy.first
}

test in assembly := {}

mappings in Universal += {
  val jar = (packageBin in Compile).value
  jar -> ("lib/" + jar.getName)
}

Keys.fork in Test := true

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(AlignParameters, false)

libraryDependencies += "ru.dgis" %% "reactive-zmq" % "0.4.0"

testOptions in Test += Tests.Argument("-oF")

assembly / mainClass := Some("com.epidata.spark.OpenGateway")
