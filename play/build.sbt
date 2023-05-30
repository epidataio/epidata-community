import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._
import scala.sys.process._

name := "epidata-https-server"
maintainer := "EpiData, Inc."

scalaVersion := "2.12.11"

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
  "org.scalactic" %% "scalactic" % "3.0.0",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.scalatestplus" %% "junit-4-12" % "3.2.0.0",
  "org.mockito" % "mockito-core" % "3.11.2" % Test,
  "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.3" % Test,
  "org.apache.kafka" %% "kafka" % "2.4.1",
  "org.apache.kafka" % "kafka-streams" % "2.4.1",
  "org.apache.kafka" % "kafka-clients" % "2.4.1",
  "com.jason-goodwin" %% "authentikat-jwt" % "0.4.5"

)

libraryDependencies += specs2 % Test

libraryDependencies += ws

libraryDependencies += ehcache

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

//  case x =>
    // For all the other files, use the default sbt-assembly merge strategy
//    val oldStrategy = (assemblyMergeStrategy in assembly).value
//    oldStrategy(x)
}

mainClass in assembly := Some("play.core.server.ProdServerStart")
fullClasspath in assembly += Attributed.blank(PlayKeys.playPackageAssets.value)

doc in Compile := (target.value / "none")

test in assembly := {}

//TwirlKeys.templateImports += "org.example._"

//ScalariformKeys.preferences := ScalariformKeys.preferences.value
//  .setPreference(DoubleIndentConstructorArguments, true)
//  .setPreference(AlignParameters, false)

libraryDependencies += "ru.dgis" %% "reactive-zmq" % "0.4.0"
