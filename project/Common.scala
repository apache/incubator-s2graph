import sbt._

object Common {
  lazy val resolvers = Seq(
    Resolver.mavenLocal,
    "DaumKakao AA Snapshot" at "http://maven.daumcorp.com/content/repositories/dk-aa-snapshots/",
    "DaumKakao AA" at "http://maven.daumcorp.com/content/groups/dk-aa-group/",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
    "Twitter Maven" at "http://maven.twttr.com",
    "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  )

  lazy val organization = "com.daumkakao.s2graph"
  lazy val version = "0.0.9-SNAPSHOT"

  lazy val scalaVersion = "2.11.7"
  lazy val playVersion = "2.3.10"
//  lazy val sparkVersion = "1.4.1"
  lazy val sparkVersion = "1.4.1-cdh5.3.3"
  //  lazy val hbaseVersion = "1.0.1"
  lazy val hbaseVersion = "1.0.0-cdh5.4.2"
  //  lazy val hadoopVersion = "2.7.0"
  lazy val hadoopVersion = "2.6.0-cdh5.4.2"
}
