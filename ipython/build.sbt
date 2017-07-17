name := "epidata-ipython-tests"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.2",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "io.netty" % "netty-transport-native-epoll" % "4.0.37.Final" classifier "linux-x86_64",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test"
)

scalariformSettings

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

bdistEgg <<= bdistEgg
  .dependsOn(autopep8)

// Overall build target
lazy val build = taskKey[Unit]("build")

build := {
}

build <<= build
  .dependsOn(sparkAssembly)
  .dependsOn(autopep8)
  .dependsOn(bdistEgg)  

(test in Test) <<= (test in Test)
  .dependsOn(build)
