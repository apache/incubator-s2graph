name := "s2graph"

lazy val commonSettings = Seq(
  organization := "com.kakao.s2graph",
  version := "0.11.0-SNAPSHOT",
  crossScalaVersions := Seq("2.11.7"),
  scalacOptions := Seq("-language:postfixOps", "-unchecked", "-deprecation", "-feature", "-Xexperimental"),
  javaOptions ++= collection.JavaConversions.propertiesAsScalaMap(System.getProperties).map{ case (key, value) => "-D" + key + "=" + value }.toSeq,
  testOptions in Test += Tests.Argument("-oDF"),
  parallelExecution in Test := false,
  resolvers ++= Seq(
    Resolver.mavenLocal,
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
    "Twitter Maven" at "http://maven.twttr.com",
    "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
  )
)

lazy val root = project.in(file(".")).enablePlugins(PlayScala).dependsOn(s2core)
  .settings(commonSettings: _*)

lazy val s2core = project.settings(commonSettings: _*)

lazy val spark = project.settings(commonSettings: _*)

lazy val loader = project.dependsOn(s2core, spark).settings(commonSettings: _*)

libraryDependencies ++= Seq(
  "com.github.danielwegener" % "logback-kafka-appender" % "0.0.3",
  ws,
  filters,
  "org.json4s" %% "json4s-native" % "3.2.11" % Test
)
