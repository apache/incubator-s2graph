package org.apache.s2graph.lambda

import org.scalatest.{FunSuite, Matchers}

class SpeedLayerTest extends FunSuite with Matchers with LocalSparkContext {

  test("streaming word count") {
    Launcher.launch(Json.get("stream_wc.json"), "run", Some(sc))
  }

}
