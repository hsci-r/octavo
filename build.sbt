import com.typesafe.config._

scalaVersion := "2.12.4"

resolvers += Resolver.mavenLocal

name := """octavo"""

val conf = ConfigFactory.parseFile(new File("conf/application.conf")).resolve()

version := conf.getString("app.version")

lazy val root = (project in file(".")).enablePlugins(
  PlayScala,
  SystemdPlugin,
  DockerPlugin,
  AshScriptPlugin)

maintainer := "Eetu Mäkelä <eetu.makela@helsinki.fi>"

packageSummary := "octavo"

packageDescription := "Octavo - Open API for Text and Metadata, built using the Play framework"

sources in (Compile, doc) := Seq.empty

publishArtifact in (Compile, packageDoc) := false

dockerBaseImage := "openjdk:alpine"

dockerExposedPorts in Docker := Seq(9000, 9443)

resolvers ++= Seq("ImageJ" at "http://maven.imagej.net/content/repositories/releases/", "betadriven" at "https://nexus.bedatadriven.com/content/groups/public/")

libraryDependencies ++= Seq(
  guice,
  "org.apache.lucene" % "lucene-core" % "7.1.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "7.1.0",
  "fi.seco" %% "lucene-morphologicalanalyzer" % "1.2.0",
  "fi.seco" %% "lucene-perfieldpostingsformatordtermvectorscodec" % "1.1.1",
  "fi.seco" % "lexicalanalysis-resources-fi-core" % "1.5.16",
  "org.apache.lucene" % "lucene-queryparser" % "7.1.0",
  "org.apache.lucene" % "lucene-highlighter" % "7.1.0",
  "com.koloboke" % "koloboke-api-jdk8" % "1.0.0",
  "com.koloboke" % "koloboke-impl-jdk8" % "1.0.0",
  "com.beachape" %% "enumeratum" % "1.5.12",
  "com.bizo" %% "mighty-csv" % "0.2",
  "com.tdunning" % "t-digest" % "3.1",
  "org.codehaus.groovy" % "groovy-jsr223" % "2.4.11",
//  "org.scijava" % "scripting-scala" % "0.2.1",
//  "org.scijava" % "scripting-renjin" % "0.2.2",
  "org.scijava" % "scripting-jython" % "0.4.1",
//  "org.scijava" % "scripting-kotlin" % "0.1.0",
  "mdsj" % "mdsj" % "0.2",
  "org.nd4j" % "nd4j-native-platform" % "0.8.0", 
  "org.deeplearning4j" % "deeplearning4j-core" % "0.7.2",
  "com.jujutsu.tsne" % "tsne" % "2.3.0"
)

