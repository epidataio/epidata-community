// Comment to get more information during initialization
logLevel := Level.Warn


lazy val root = (project in file("."))
  .enablePlugins(SbtPlugin)


// The Typesafe repository
resolvers += "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"

// Use the Play sbt plugin for Play projects
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.6.25")

// Use the Assembly plugin to produce fat jars.
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")

// The Scalastyle plugin, a Scala style checker.
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

// The Sonatype repository, for Scalastyle.
resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

// The Scalariform plugin, an automatic Scala sourcecode formatter.
addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.3")

// The sbt dependency graph plugin
//addSbtPlugin("io.get-coursier" % "sbt-coursier" % "2.0.0-RC3-3")

// The Artima SuperSafe plugin for ScalaTest/Scalactic
//addSbtPlugin("com.artima.supersafe" % "sbtplugin" % "1.1.12")
