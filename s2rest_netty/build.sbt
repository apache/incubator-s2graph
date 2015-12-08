name := "s2rest_netty"

enablePlugins(JavaAppPackaging)

libraryDependencies ++= Seq(
  "io.netty" % "netty-all" % "4.0.33.Final",
  "com.typesafe.play" %% "play-ws" % Common.playVersion
)
