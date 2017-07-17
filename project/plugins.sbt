// Comment to get more information during initialization
logLevel := Level.Warn

// The Typesafe repository 
resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

// Use the Play sbt plugin for Play projects
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.2.6")

// Use the Assembly plugin to produce fat jars.
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")

// The Scalastyle plugin, a Scala style checker.
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.6.0")

// The Sonatype repository, for Scalastyle.
resolvers += "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/"

// The Scalariform plugin, an automatic Scala sourcecode formatter.
addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.3.0")
