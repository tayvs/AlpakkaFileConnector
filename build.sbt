name := "AlpakkaFileConnector"

version := "0.1"

scalaVersion := "2.12.8"

resolvers += "dnvriend" at "http://dl.bintray.com/dnvriend/maven"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence" % "2.5.21"
libraryDependencies += "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.21"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-file" % "1.0-M2"

// akka 2.5.x
libraryDependencies += "com.github.dnvriend" %% "akka-persistence-inmemory" % "2.5.15.1" % Test