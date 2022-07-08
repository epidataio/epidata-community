import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._
import scala.sys.process._

name := "epidata-play"

resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"
resolvers ++= Seq(
  "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
  Resolver.jcenterRepo
)


routesGenerator := InjectedRoutesGenerator

libraryDependencies ++= Seq(
  guice,
  ehcache,
  "org.xerial" % "sqlite-jdbc" % "3.32.3.3",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.9.0",
  "de.kaufhof" %% "pillar" % "4.1.2",
  "com.typesafe.play" %% "play-mailer" % "6.0.1",
  "com.typesafe.play" %% "twirl-api" % "1.5.0",
  "com.typesafe.play" %% "filters-helpers" % "2.6.25",
  "org.scalacheck" %% "scalacheck" % "1.14.3" % Test,
  "org.scalactic" %% "scalactic" % "3.2.5",
  "org.scalatest" %% "scalatest" % "3.2.5" % "test",
  "org.scalatestplus" %% "scalacheck-1-14" % "3.2.0.0" % "test",
  "org.specs2" %% "specs2-core" % "3.9.5" % "test",
  "org.mockito" % "mockito-core" % "3.3.3" % Test,
  "org.apache.kafka" %% "kafka" % "2.4.1",
  "org.apache.kafka" % "kafka-streams" % "2.4.1",
  "org.apache.kafka" % "kafka-clients" % "2.4.1",
  "com.jason-goodwin" %% "authentikat-jwt" % "0.4.5"

)

scalacOptions in Test ++= Seq("-Yrangepos")

routesImport ++= List("java.util.Date",
  "util.Ordering",
  "util.QueryStringBinders._")

lazy val autopep8 = taskKey[Unit]("autopep8")

autopep8 := {
  "find play -name *.py" #| "xargs autopep8 -i -aa" !
}

(test in Test) := ((test in Test)
  .dependsOn(autopep8)).value

//TwirlKeys.templateImports += "org.example._"

//ScalariformKeys.preferences := ScalariformKeys.preferences.value
//  .setPreference(DoubleIndentConstructorArguments, true)
//  .setPreference(AlignParameters, false)

scalaVersion := "2.12.11"

libraryDependencies += "ru.dgis" %% "reactive-zmq" % "0.4.0"