name := "s2rest_finagle"

enablePlugins(JavaAppPackaging)

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-http" % "6.31.0"
)
