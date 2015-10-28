package com.kakao.ml.launcher

import org.scalatest.{Matchers, FunSuite}

class LauncherTest extends FunSuite with Matchers with LocalSparkContext {

  val sampleJson =
    """{
      |  "name": "s2ml_job",
      |  "env": {
      |    "dir": "/path/to/dir"
      |  },
      |  "processors": [
      |    {
      |      "class": "com.kakao.ml.example.Tuple2RandomNumberGenerator",
      |      "params": {
      |        "num": 1000
      |      }
      |    },
      |    {
      |      "class": "com.kakao.ml.example.PiEstimator"
      |    }
      |  ]
      |}
    """.stripMargin

  test("working") {
    Launcher.launch(sampleJson, "run", Some(sc))
  }

}
