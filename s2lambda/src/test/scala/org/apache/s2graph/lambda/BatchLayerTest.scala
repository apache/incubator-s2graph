package org.apache.s2graph.lambda

import org.scalatest.{FunSuite, Matchers}

class BatchLayerTest extends FunSuite with Matchers with LocalSparkContext {

  test("pi estimating") {
    Launcher.launch(Json.get("pi.json"), "run", Some(sc))
  }

  test("word count") {
    Launcher.launch(Json.get("wc.json"), "run", Some(sc))
  }

}
