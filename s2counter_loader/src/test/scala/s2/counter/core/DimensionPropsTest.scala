package s2.counter.core

import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ListBuffer

/**
 * Created by hsleep(honeysleep@gmail.com) on 2015. 10. 7..
 */
class DimensionPropsTest extends FunSuite with Matchers {
  test("makeRequestBody with Seq") {
    val requestBody =
      """
        |{
        |  "_from" => [[_from]]
        |}
      """.stripMargin
    val requestBodyExpected =
      """
        |{
        |  "_from" => 1
        |}
      """.stripMargin
    val requestBodyResult = DimensionProps.makeRequestBody(requestBody, Seq(("[[_from]]", "1")).toList)

    requestBodyResult shouldEqual requestBodyExpected
  }

  test("makeRequestBody with ListBuffer") {
    val requestBody =
      """
        |{
        |  "_from" => [[_from]]
        |}
      """.stripMargin
    val requestBodyExpected =
      """
        |{
        |  "_from" => 1
        |}
      """.stripMargin
    val requestBodyResult = DimensionProps.makeRequestBody(requestBody, ListBuffer(("[[_from]]", "1")).toList)

    requestBodyResult shouldEqual requestBodyExpected
  }
}
