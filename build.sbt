name := """ecco-estc-search"""

version := "1.1-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  cache,
  ws,
  filters,
  "org.apache.lucene" % "lucene-analyzers-common" % "6.2.1",
  "org.apache.lucene" % "lucene-codecs" % "6.2.1",
  "org.apache.lucene" % "lucene-queryparser" % "6.2.1",
  "com.koloboke" % "koloboke-api-jdk8" % "1.0.0",
  "com.koloboke" % "koloboke-impl-jdk8" % "1.0.0",
  "com.bizo" %% "mighty-csv" % "0.2",
  "mdsj" % "mdsj" % "0.2"
)

