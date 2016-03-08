package org.apache.s2graph.spark

import org.apache.s2graph.spark.spark.SparkApp
import org.scalatest.{FunSuite, Matchers}

object TestApp extends SparkApp {
  override def run(): Unit = {
    validateArgument("topic", "phase")
  }
}

class SparkAppTest extends FunSuite with Matchers {
  test("parse argument") {
    TestApp.main(Array("s2graphInreal", "real"))
    TestApp.getArgs(0) shouldEqual "s2graphInreal"
  }
}
