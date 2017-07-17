name := "epidata"

version in ThisBuild := "1.0-SNAPSHOT"


lazy val commonSettings = Seq(
  organization := "com.epidata",
  version := "1.0-SNAPSHOT",
  scalaVersion := "2.11.8"
)

scalaVersion := "2.11.8"

// Prevent concurrent execution of tests from different subprojects. 
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

lazy val root = project.in(file(".")).aggregate(
  ipython,
  models,
  play,
  spark,
  scripts
)

lazy val scripts = project
  .dependsOn(spark).settings(scalaVersion := "2.11.8")

lazy val ipython = project
  .dependsOn(spark).settings(scalaVersion := "2.11.8")

lazy val models = project.settings(
  crossScalaVersions := Seq("2.10.6", "2.11.8")
)

lazy val play = project
  .dependsOn(models).settings(scalaVersion := "2.10.6")

lazy val spark = project
  .dependsOn(models).settings(scalaVersion := "2.11.8")

lazy val sparkAssembly = TaskKey[File]("spark-assembly")

sparkAssembly in Global <<= (assembly in spark)

scalariformSettings


