import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._

name := "epidata"

version in ThisBuild := "1.0-SNAPSHOT"


lazy val commonSettings = Seq(
  organization := "com.epidata",
  version := "1.0-SNAPSHOT",
  scalaVersion := "2.12.11"
)

scalaVersion := "2.12.11"

// Prevent concurrent execution of tests from different subprojects.
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

lazy val root = project.in(file(".")).aggregate(
  jupyter,
  ipython,
  models,
  play,
  spark,
  scripts)

lazy val scripts = project
  .dependsOn(spark)

lazy val ipython = project
  .dependsOn(spark)

lazy val jupyter = project
    .dependsOn(spark)

lazy val models = (project in file("models"))
  .settings(
  crossScalaVersions := Seq("2.11.12", "2.12.11")
)

//lazy val securesocial = ProjectRef(uri("git://github.com/jaliss/securesocial#0f9710325724da34a46c5ecefb439121fce837b7"), "core")

lazy val play = project
  .dependsOn(models)
  .enablePlugins(PlayScala)

lazy val spark = project
  .dependsOn(models)
  .enablePlugins(UniversalPlugin)

lazy val playAssembly = TaskKey[File]("play-assembly")
playAssembly in Global := (assembly in play).value

lazy val sparkAssembly = TaskKey[File]("spark-assembly")
sparkAssembly in Global := (assembly in spark).value

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(DoubleIndentConstructorArguments, true)
  .setPreference(AlignParameters, false)
