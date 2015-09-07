import sbt._

object Common {
  lazy val resolvers = Seq(
    Resolver.mavenLocal,
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
    "Twitter Maven" at "http://maven.twttr.com",
    "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  )

//  lazy val scalaVersion = "2.11.7"
  lazy val scalaVersion = "2.10.5"
  lazy val version = "0.1.0-SNAPSHOT"
  lazy val organization = "com.daumkakao.s2graph"
  lazy val sparkVersion = "1.4.1"

  //  lazy val hbaseVersion = "1.0.1"
  lazy val hbaseVersion = "1.0.0-cdh5.4.2"
  //  lazy val hadoopVersion = "2.7.0"
  lazy val hadoopVersion = "2.6.0-cdh5.4.2"
}
