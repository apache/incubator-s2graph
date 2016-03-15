package org.apache.s2graph.counter.loader.core

import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}
import s2.models.DBModel

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 7. 3..
 */
class CounterEtlFunctionsSpec extends FlatSpec with BeforeAndAfterAll with Matchers {
  override def beforeAll: Unit = {
    DBModel.initialize(ConfigFactory.load())
  }

  "CounterEtlFunctions" should "parsing log" in {
    val data =
      """
        |1435107139287	insert	e	aaPHfITGUU0B_150212123559509	abcd	test_case	{"cateid":"100110102","shopid":"1","brandid":""}
        |1435106916136	insert	e	Tgc00-wtjp2B_140918153515441	efgh	test_case	{"cateid":"101104107","shopid":"2","brandid":""}
      """.stripMargin.trim.split('\n')
    val items = {
      for {
        line <- data
        item <- CounterEtlFunctions.parseEdgeFormat(line)
      } yield {
        item.action should equal("test_case")
        item
      }
    }

    items should have size 2
  }
}
