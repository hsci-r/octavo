import com.typesafe.config._

name := """octavo"""

val conf = ConfigFactory.parseFile(new File("conf/application.conf")).resolve()

version := conf.getString("app.version")

enablePlugins(SystemdPlugin)

maintainer := "Eetu Mäkelä <eetu.makela@aalto.fi>"

packageSummary := "octavo"

packageDescription := "Octavo - Open API for Text and Metadata, built using the Play framework"

lazy val root = (project in file(".")).enablePlugins(
  PlayScala,
  DockerPlugin,
  AshScriptPlugin)

scalaVersion := "2.12.2"

resolvers += Resolver.mavenLocal

sources in (Compile, doc) := Seq.empty

publishArtifact in (Compile, packageDoc) := false

dockerBaseImage := "openjdk:alpine"

dockerExposedPorts in Docker := Seq(9000, 9443)

libraryDependencies ++= Seq(
  guice,
  "org.apache.lucene" % "lucene-core" % "6.6.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "6.6.0",
  "fi.seco" %% "lucene-morphologicalanalyzer" % "1.1.2",
  "fi.seco" %% "lucene-fstordtermvectorscodec" % "1.4.0",
  "org.apache.lucene" % "lucene-queryparser" % "6.6.0",
  "org.apache.lucene" % "lucene-highlighter" % "6.6.0",
  "com.koloboke" % "koloboke-api-jdk8" % "1.0.0",
  "com.koloboke" % "koloboke-impl-jdk8" % "1.0.0",
  "com.beachape" %% "enumeratum" % "1.5.12",
  "com.bizo" %% "mighty-csv" % "0.2",
  "com.tdunning" % "t-digest" % "3.1",
  "org.codehaus.groovy" % "groovy" % "2.4.11",
  "mdsj" % "mdsj" % "0.2",
  "org.nd4j" % "nd4j-native-platform" % "0.8.0", 
  "org.deeplearning4j" % "deeplearning4j-core" % "0.7.2",
  "com.jujutsu.tsne" % "tsne" % "2.3.0"
)

