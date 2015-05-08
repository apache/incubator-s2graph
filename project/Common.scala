import sbt._

object Common {
  lazy val resolvers = Seq(
    Resolver.mavenLocal, 
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
    "Twitter Maven" at "http://maven.twttr.com",
    "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  )
  lazy val scalaVersion = "2.10.4"
  lazy val version = "0.0.4-SNAPSHOT"
  lazy val hbaseVersion = "1.0.1"
  lazy val hadoopVersion = "2.7.0"
}
