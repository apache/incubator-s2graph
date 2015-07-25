import sbt._

object Common {
  lazy val resolvers = Seq(
    Resolver.mavenLocal,
    "kakao Maven Snapshot" at "http://artifactory.iwilab.com:8088/artifactory/libs-snapshot-local",
    "kakao Maven Release" at "http://artifactory.iwilab.com:8088/artifactory/libs-release-local",
    "daum" at "http://maven.daumcorp.com/content/groups/daum-public/",
    "AA Release" at "http://maven.daumcorp.com/content/repositories/dk-aa-release",
    "AA Snapshots" at "http://maven.daumcorp.com/content/repositories/dk-aa-snapshots",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
    "Twitter Maven" at "http://maven.twttr.com",
    "sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  )
  lazy val scalaVersion = "2.10.4"
  lazy val version = "0.0.8-SNAPSHOT"
  lazy val organization = "com.daumkakao.s2graph"
//  lazy val hbaseVersion = "1.0.1"
  lazy val hbaseVersion = "1.0.0-cdh5.4.2"
//  lazy val hadoopVersion = "2.7.0"
  lazy val hadoopVersion = "2.6.0-cdh5.4.2"
}
