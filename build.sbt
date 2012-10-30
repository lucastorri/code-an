organization := "co.torri"

name := "code-an"

version := "1.0"

scalaVersion := "2.9.2"

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"

libraryDependencies ++= Seq(
  "org.spark-project" %% "spark-core" % "0.6.0",
  "org.reflections" % "reflections" % "0.9.8",
  "com.mosesn" %% "pirate" % "0.1.0"
)

resolvers ++= Seq(
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype Repository" at "https://oss.sonatype.org/content/repositories/releases/",
  "Spray Repository" at "http://repo.spray.cc/"
)