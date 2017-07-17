name := "epidata-play"

libraryDependencies ++= Seq(
  cache,
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.2",
  "com.chrisomeara" % "pillar_2.10" % "2.3.0",
  "com.typesafe.play" %% "play-mailer" % "2.4.1",
  "javax.inject" % "javax.inject" % "1",
  "ws.securesocial" %% "securesocial" % "2.1.4",
  "org.scalacheck" %% "scalacheck" % "1.12.2" % "test",
  "org.scalatestplus" % "play_2.10" % "1.0.0" % "test",
  "org.mockito" % "mockito-all" % "1.9.5" % "test",
  "org.apache.kafka" % "kafka_2.10" % "0.10.2.0",
  "org.apache.kafka" % "kafka-clients" % "0.10.2.0"
)

play.Project.playScalaSettings

routesImport ++= List("java.util.Date",
                      "util.Ordering",
                      "util.QueryStringBinders._")

scalariformSettings

lazy val autopep8 = taskKey[Unit]("autopep8")

autopep8 := {
  "find play -name *.py" #| "xargs autopep8 -i -aa" !
}

(test in Test) <<= (test in Test)
  .dependsOn(autopep8)

scalaVersion := "2.10.6"
