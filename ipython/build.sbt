import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

name := "epidata-ipython-tests"

//scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.9.0",
  "org.apache.spark" %% "spark-sql" % "2.4.6",
  "org.scalatest" %% "scalatest" % "3.2.0" % Test
)

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(AlignParameters, false)

lazy val sparkAssembly = TaskKey[File]("spark-assembly")

lazy val autopep8 = taskKey[Unit]("autopep8")

//TODO - Update
//autopep8 := {
//  "find ipython python -name *.py" #| "xargs autopep8 -i -aa" !
//}

// Build python package for distribution to workers.
lazy val bdistEgg = taskKey[Unit]("bdist_egg")

bdistEgg := {
  sys.process.Process(Seq("python", "setup.py", "bdist_egg"),
    new java.io.File("ipython")).!!
}

// TODO - Update
//bdistEgg := bdistEgg
//  .dependsOn(autopep8)

// Overall build target
lazy val build = taskKey[Unit]("build")

build := {
}

//TODO - Update
//build := build
//  .dependsOn(sparkAssembly)
//  .dependsOn(autopep8)
//  .dependsOn(bdistEgg)

//build := build
//  .dependsOn(sparkAssembly)
//  .dependsOn(autopep8)
//  .dependsOn(bdistEgg)

(test in Test) := (test in Test)
  .dependsOn(build)
