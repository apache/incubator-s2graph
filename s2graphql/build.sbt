name := "s2graphql"

version := "0.1"

description := "GraphQL server with akka-http and sangria and s2graph"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= Seq(
  "org.sangria-graphql" %% "sangria" % "1.3.3",
  "org.sangria-graphql" %% "sangria-spray-json" % "1.0.0",

  "com.typesafe.akka" %% "akka-http" % "10.0.10",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.10",

  "com.typesafe.akka" %% "akka-slf4j" % "2.4.6",

  "org.scalatest" %% "scalatest" % "3.0.4" % Test
)

Revolver.settings
