import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

name := "epidata-play"

resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

routesGenerator := InjectedRoutesGenerator

libraryDependencies ++= Seq(
  guice,
  ehcache,
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.9.0",
  "de.kaufhof" %% "pillar" % "4.1.2",
  "com.typesafe.play" %% "play-mailer" % "6.0.1",
  "com.typesafe.play" %% "twirl-api" % "1.5.0",
  "com.typesafe.play" %% "filters-helpers" % "2.6.25",
  "org.scalacheck" %% "scalacheck" % "1.14.3" % Test,
  "org.scalatest" %% "scalatest" % "3.2.0" % Test,
  "org.scalatestplus" %% "scalacheck-1-14" % "3.2.0.0" % "test",
  "org.mockito" % "mockito-core" % "3.3.3" % Test,
  "org.apache.kafka" %% "kafka" % "2.4.1",
  "org.apache.kafka" % "kafka-streams" % "2.4.1",
  "org.apache.kafka" % "kafka-clients" % "2.4.1"
)

routesImport ++= List("java.util.Date",
                      "util.Ordering",
                      "util.QueryStringBinders._")

lazy val autopep8 = taskKey[Unit]("autopep8")

// TODO - update
//autopep8 := {
//  "find play -name *.py" #| "xargs autopep8 -i -aa" !
//}

// TODO - update
//(test in Test) <<= (test in Test)
//  .dependsOn(autopep8)


//TwirlKeys.templateImports += "org.example._"

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(AlignParameters, false)

scalaVersion := "2.12.11"
