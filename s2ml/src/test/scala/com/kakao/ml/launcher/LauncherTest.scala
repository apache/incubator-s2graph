package com.kakao.ml.launcher

import org.scalatest.{Matchers, FunSuite}

class LauncherTest extends FunSuite with Matchers with LocalSparkContext {

  val sampleJson =
    """
      |{
      |  "name": "pi",
      |  "processors": [
      |    {
      |      "class": "com.kakao.ml.example.Tuple2RandomNumberGenerator",
      |      "params": {
      |        "num": 1000
      |      }
      |    },
      |    {
      |      "class": "com.kakao.ml.Inspector"
      |    },
      |    {
      |      "class": "com.kakao.ml.example.PiEstimator"
      |    }
      |  ]
      |}
      |
    """.stripMargin

  test("working") {
    Launcher.launch(sampleJson, "run", Some(sc))
  }

}
