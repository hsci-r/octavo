name := """ecco-estc-search"""

version := "1.1-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  cache,
  ws,
  filters,
  "org.apache.lucene" % "lucene-analyzers-common" % "6.2.1",
  "org.apache.lucene" % "lucene-queryparser" % "6.2.1",
  "com.bizo" %% "mighty-csv" % "0.2"
)

