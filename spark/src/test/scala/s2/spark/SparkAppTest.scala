package s2.spark

import org.scalatest.{FunSuite, Matchers}

/**
 * Created by alec.k on 14. 12. 26..
 */

object TestApp extends SparkApp {
  // 상속받은 클래스에서 구현해줘야 하는 함수
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
