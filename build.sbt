import com.typesafe.config._

lazy val conf = ConfigFactory.parseFile(new File("conf/application.conf")).resolve()

lazy val commonSettings = Seq(
  scalaVersion := "2.12.9",
  resolvers += Resolver.mavenLocal,
  developers := List(Developer(id="jiemakel",name="Eetu Mäkelä",email="eetu.makela@iki.fi",url=url("http://iki.fi/eetu.makela"))),
  organization := "io.github.jiemakel",
  description := "Octavo - Open API for Text and Metadata, built using the Play framework",
  licenses := List("MIT" -> new URL("https://opensource.org/licenses/MIT")),
  homepage := Some(url("https://github.com/jiemakel/octavo/")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/jiemakel/octavo"),
      "scm:git@github.com:jiemakel/octavo.git"
    )
  ),
  version := conf.getString("app.version"),
  resolvers ++= Seq("ImageJ" at "http://maven.imagej.net/content/repositories/releases/", "betadriven" at "https://nexus.bedatadriven.com/content/groups/public/")
)

lazy val mainSettings = Seq(
  name := "octavo",
  libraryDependencies ++= Seq(
    guice,
    "commons-codec" % "commons-codec" % "1.11",
    "org.apache.lucene" % "lucene-core" % "8.2.0",
    "org.apache.lucene" % "lucene-codecs" % "8.2.0",
    "org.apache.lucene" % "lucene-analyzers-common" % "8.2.0",
    "fi.seco" %% "lucene-morphologicalanalyzer" % "1.2.1",
    "fi.seco" %% "lucene-perfieldpostingsformatordtermvectorscodec" % "1.1.5",
    "fi.seco" % "lexicalanalysis-resources-fi-core" % "1.5.16",
    "fi.hsci" %% "lucene-normalisinganalyzer" % "1.0.0",
    "org.apache.lucene" % "lucene-queryparser" % "8.2.0",
    "org.apache.lucene" % "lucene-highlighter" % "8.2.0",
    "com.koloboke" % "koloboke-api-jdk8" % "1.0.0",
    "com.koloboke" % "koloboke-impl-jdk8" % "1.0.0",
    "com.beachape" %% "enumeratum" % "1.5.12",
    "com.bizo" %% "mighty-csv" % "0.2",
    "com.tdunning" % "t-digest" % "3.1",
    "org.codehaus.groovy" % "groovy-jsr223" % "2.4.11",
    "org.scijava" % "scripting-jython" % "0.4.1" % "runtime",
    "org.jetbrains.kotlin" % "kotlin-compiler-embeddable" % "1.2.60" % "runtime" exclude("org.jetbrains","annotations"),
    "org.jetbrains.kotlin" % "kotlin-script-runtime" % "1.2.60" % "runtime" exclude("org.jetbrains","annotations"),
    "org.jetbrains.kotlin" % "kotlin-script-util" % "1.2.60" % "runtime" exclude("org.jetbrains.kotlin","kotlin-daemon-client"),
    "org.jetbrains.kotlin" % "kotlin-stdlib" % "1.2.60" % "runtime" exclude("org.jetbrains","annotations"),
    "mdsj" % "mdsj" % "0.2",
    "org.nd4j" % "nd4j-native-platform" % "0.8.0",
    "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-beta3",
    "com.jujutsu.tsne" % "tsne" % "2.3.0",
    "org.jetbrains.xodus" % "xodus-environment" % "1.2.3" exclude("org.jetbrains","annotations")
  )
)

import com.typesafe.sbt.packager.docker._

lazy val dockerSettings = Seq(
  maintainer := "Eetu Mäkelä <eetu.makela@iki.fi>",
  packageSummary := "octavo",
  packageDescription := "Octavo - Open API for Text and Metadata, built using the Play framework",
  dockerBaseImage := "openjdk:13-alpine",
  dockerExposedPorts := Seq(9000, 9443),
  dockerEnvVars := Map("JAVA_OPTS"->"-Dindices.index=/opt/docker/index"),
  dockerExposedVolumes := Seq("/opt/docker/logs","/opt/docker/index","/opt/docker/tmp"),
  dockerUsername := Some("jiemakel"),
  daemonUserUid in Docker := None,
  daemonUser in Docker := "daemon",
  dockerUpdateLatest := true,
  dockerCommands := {
    val splitIndex = dockerCommands.value.lastIndexWhere{case Cmd("FROM",_) => true; case _ => false} + 1
    val (head,tail) = dockerCommands.value.splitAt(splitIndex)
    head ++ Seq(Cmd("RUN","apk","update"),Cmd("RUN","apk","add","rsync","openssh")) ++ tail
  }
)

lazy val assemblySettings = Seq(
  mainClass in assembly := Some("play.core.server.ProdServerStart"),
  fullClasspath in assembly += Attributed.blank(PlayKeys.playPackageAssets.value),
  artifact in (Compile, assembly) := {
    val art = (artifact in (Compile, assembly)).value
    art.withClassifier(Some("assembly"))
  },
  assemblyMergeStrategy in assembly := {
    case PathList("org","nd4j","serde","base64","Nd4jBase64.class") => MergeStrategy.first
    case PathList("org", "jetbrains", "annotations", xs @ _*) => MergeStrategy.first // these appear in multiple places
    case manifest if manifest.contains("MANIFEST.MF") =>
      MergeStrategy.discard
    case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") =>
      MergeStrategy.concat
    case PathList("META-INF", xs @ _*) if xs.exists(_.endsWith("kotlin_module")) => MergeStrategy.first
    case PathList("META-INF", "json", "org.scijava.plugin.Plugin") => MergeStrategy.first
    case PathList("META-INF", "native", xs @ _*) => MergeStrategy.first
    case PathList("org", "junit", xs @ _*) => MergeStrategy.first // tsne is borked
    case PathList("org", "hamcrest", xs @ _*) => MergeStrategy.first
    case PathList("junit", xs @ _*) => MergeStrategy.first
    case "overview.html" => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
) // ++ addArtifact(artifact in (Compile, assembly), assembly).settings 

lazy val publishSettings = Seq(
  pomIncludeRepository := { _ => false },
  publishMavenStyle := true,
  publishTo := sonatypePublishTo.value
)

lazy val octavo = (project in file("."))
  .settings(commonSettings:_*)
  .settings(mainSettings:_*)
  .settings(dockerSettings:_*)
  .settings(assemblySettings:_*)
  .settings(publishSettings:_*)
//  .settings(publishArtifact in (Compile, assembly) := false)
  .enablePlugins(
    PlayScala,
    SystemdPlugin,
    DockerPlugin,
    AshScriptPlugin)

lazy val octavoAssembly = (project in file("build/assembly"))
  .settings(commonSettings:_*)
  .settings(name := "octavo-assembly")
  .settings(publishSettings:_*)
  .disablePlugins(AssemblyPlugin)
  .settings(packageBin in Compile := (assembly in (octavo, Compile)).value)

lazy val rootSettings = Seq(
  publishArtifact := false,
  publishArtifact in Test := false
)

lazy val root = (project in file("build/root"))
  .settings(commonSettings:_*)
  .settings(rootSettings:_*)
  .disablePlugins(AssemblyPlugin)
  .aggregate(octavo,octavoAssembly)

