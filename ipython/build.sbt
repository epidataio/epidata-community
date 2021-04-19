import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._
import scala.sys.process._

name := "epidata-ipython-tests"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.xerial" % "sqlite-jdbc" % "3.32.3.3",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.9.0",
  "org.apache.spark" %% "spark-sql" % "2.4.6",
  "org.scalactic" %% "scalactic" % "3.2.5",
  "org.scalatest" %% "scalatest" % "3.2.5" % "test"

)

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(AlignParameters, false)

lazy val sparkAssembly = TaskKey[File]("spark-assembly")

lazy val autopep8 = taskKey[Unit]("autopep8")

autopep8 := {
  "find ipython python -name *.py" #| "xargs autopep8 -i -aa" !
}

// Build python package for distribution to workers.
lazy val bdistEgg = taskKey[Unit]("bdist_egg")

bdistEgg := {
  sys.process.Process(Seq("python", "setup.py", "bdist_egg"),
    new java.io.File("ipython")).!!
}

bdistEgg := (bdistEgg
  .dependsOn(autopep8)).value

// Overall build target
lazy val build = taskKey[Unit]("build")

build := {
}

build := (build
  .dependsOn(sparkAssembly)
  .dependsOn(autopep8)
  .dependsOn(bdistEgg)).value

(test in Test) := ((test in Test)
  .dependsOn(build)).value
