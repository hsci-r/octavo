import com.typesafe.config._
import com.typesafe.sbt.packager.docker.Cmd

lazy val conf = ConfigFactory.parseFile(new File("conf/application.conf")).resolve()

lazy val commonSettings = Seq(
  turbo := true,
  useCoursier := true,
  scalaVersion := "2.13.6",
//  scalacOptions += "-target:9",
//  javacOptions ++= Seq("-source","9","-target","9"),
  resolvers += Resolver.mavenLocal,
  developers := List(Developer(id="jiemakel",name="Eetu Mäkelä",email="eetu.makela@iki.fi",url=url("http://iki.fi/eetu.makela"))),
  organization := "io.github.hsci-r",
  description := "Octavo - Open API for Text and Metadata, built using the Play framework",
  licenses := List("MIT" -> new URL("https://opensource.org/licenses/MIT")),
  homepage := Some(url("https://github.com/hsci-r/octavo/")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/hsci-r/octavo"),
      "scm:git@github.com:hsci-r/octavo.git"
    )
  ),
  version := conf.getString("app.version"),
  resolvers ++= Seq("ImageJ" at "https://maven.imagej.net/content/repositories/releases/", "betadriven" at "https://nexus.bedatadriven.com/content/groups/public/")
)

lazy val mainSettings = Seq(
  name := "octavo",
  libraryDependencies ++= Seq(
    guice,
    "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.3",
    "commons-codec" % "commons-codec" % "1.15",
    "fi.hsci" %% "lucene-perfieldpostingsformatordtermvectorscodec" % "1.2.5",
    "org.apache.lucene" % "lucene-core" % "8.9.0",
    "org.apache.lucene" % "lucene-codecs" % "8.9.0",
    "org.apache.lucene" % "lucene-backward-codecs" % "8.9.0",
    "org.apache.lucene" % "lucene-analyzers-common" % "8.9.0",
    "fi.seco" %% "lucene-morphologicalanalyzer" % "1.2.1",
    "fi.seco" % "lexicalanalysis-resources-fi-core" % "1.5.16",
    "fi.hsci" %% "lucene-normalisinganalyzer" % "1.1.0",
    "org.apache.lucene" % "lucene-queryparser" % "8.9.0",
    "org.apache.lucene" % "lucene-highlighter" % "8.9.0",
    "com.koloboke" % "koloboke-api-jdk8" % "1.0.0",
    "com.koloboke" % "koloboke-impl-jdk8" % "1.0.0",
    "com.beachape" %% "enumeratum" % "1.6.1",
//    "com.bizo" %% "mighty-csv" % "0.2",
    "com.tdunning" % "t-digest" % "3.3",
    "org.codehaus.groovy" % "groovy-jsr223" % "3.0.8",
    "org.scijava" % "scripting-jython" % "1.0.0",
    "org.jetbrains.kotlin" % "kotlin-compiler-embeddable" % "1.5.10",
    "org.jetbrains.kotlin" % "kotlin-script-runtime" % "1.5.10",
    "org.jetbrains.kotlin" % "kotlin-script-util" % "1.5.10",
    "org.jetbrains.kotlin" % "kotlin-stdlib" % "1.5.10",
    "mdsj" % "mdsj" % "0.2",
//    "org.nd4j" % "nd4j-native-platform" % "1.0.0-beta4",
    "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-beta4",
    "com.jujutsu.tsne" % "tsne" % "2.5.0",
    "org.jetbrains.xodus" % "xodus-environment" % "1.3.232" exclude("org.jetbrains","annotations"),
    "com.github.tototoshi" %% "scala-csv" % "1.3.8",
  "ch.qos.logback" % "logback-classic" % "1.2.3"

  )
)

lazy val dockerSettings = Seq(
  maintainer := "Eetu Mäkelä <eetu.makela@iki.fi>",
  packageSummary := "octavo",
  packageDescription := "Octavo - Open API for Text and Metadata, built using the Play framework",
  dockerBaseImage := "adoptopenjdk/openjdk15-openj9",
  dockerExposedPorts := Seq(9000, 9443),
  dockerEnvVars := Map("JAVA_OPTS"->"-Dindices.index=/opt/docker/index"),
  dockerExposedVolumes := Seq("/opt/docker/logs","/opt/docker/index","/opt/docker/tmp"),
  dockerRepository := Some("quay.io"),
  dockerUsername := Some("hsci"),
  daemonUserUid in Docker := None,
  daemonUser in Docker := "daemon",
  dockerUpdateLatest := true,
  dockerCommands := dockerCommands.value ++ Seq(Cmd("USER","root"),Cmd("RUN","apt-get","-y","update"),Cmd("RUN","apt-get","-y","install","rsync"),Cmd("RUN","chmod","a+rwx","/opt/docker/tmp","/opt/docker/logs"),Cmd("USER","65536"))
  /*,
  dockerCommands := {
    val s1 = dockerCommands.value.indexWhere{case Cmd("COPY",_) => true; case _ => false} + 1
    val (h1,t1) = dockerCommands.value.splitAt(s1)
    val s2 = t1.indexWhere{case Cmd("FROM",_) => true; case _ => false} + 1
    val (h2,t2) = t1.splitAt(s2)
    val s3 = t2.indexWhere{case Cmd("COPY",_) => true; case _ => false} + 1
    val (h3,t3) = t2.splitAt(s3)
    h1 ++
      Seq(ExecCmd("RUN","rm","/opt/docker/conf/application.conf","/opt/docker/lib/"+organization.value+"."+name.value+"-"+version.value+"-sans-externalized.jar","/opt/docker/lib/"+organization.value+"."+name.value+"-"+version.value+"-assets.jar")) ++
      h2 ++
      Seq(Cmd("RUN","apt","update"),Cmd("RUN","apt","install","rsync","openssh")) ++
      h3 ++
      Seq(
        Cmd("COPY","opt/docker/conf/application.conf","/opt/docker/conf/application.conf"),
        Cmd("COPY","opt/docker/lib/"+organization.value+"."+name.value+"-"+version.value+"-sans-externalized.jar","opt/docker/lib/"+organization.value+"."+name.value+"-"+version.value+"-assets.jar","/opt/docker/lib/")
      ) ++
      t3
  }*/
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

