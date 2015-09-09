name := "s2graph"

lazy val commonSettings = Seq(
  organization := "com.daumkakao.s2graph",
  version := "0.10.0-SNAPSHOT",
  crossScalaVersions := Seq("2.11.7", "2.10.5"),
  resolvers ++= Seq(
    Resolver.mavenLocal,
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
    "Twitter Maven" at "http://maven.twttr.com",
    "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"
  )
)

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

javaOptions ++= collection.JavaConversions.propertiesAsScalaMap(System.getProperties).map{ case (key, value) => "-D" + key + "=" + value }.toSeq

// I - show reminder of failed and canceled tests without stack traces
// T - show reminder of failed and canceled tests with short stack traces
// G - show reminder of failed and canceled tests with full stack traces
testOptions in Test += Tests.Argument("-oDF")

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
