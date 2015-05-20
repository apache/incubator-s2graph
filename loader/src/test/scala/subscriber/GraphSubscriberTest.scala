package subscriber

import com.daumkakao.s2graph.core.{Label, Service, Management}
import org.scalatest.{ FunSuite, Matchers }
import play.api.libs.json.{JsBoolean, JsNumber}
import s2.spark.WithKafka

class GraphSubscriberTest extends FunSuite with Matchers with WithKafka {
  val phase = "alpha"
  val dbUrl = "jdbc:mysql://nuk151.kr2.iwilab.com:13306/graph_alpha"
  val zkQuorum = "tokyo062.kr2.iwilab.com"
  val kafkaBrokerList = "rabat176.kr2.iwilab.com:9099"
  val currentTs = System.currentTimeMillis()
  val op = "insertBulk"
  val testLabelName = "talk_friend_long_term_agg"
  val labelToReplace = "talk_friend_long_term_agg_2015-10-10"
  val serviceName = "s2graph"
  val columnName = "account_id"
  val columnType = "long"
  val indexProps = Seq("time" -> JsNumber(0), "weight" -> JsNumber(0))
  val props = Seq("is_hidden" -> JsBoolean(false), "is_blocked" -> JsBoolean(false))
  val hTableName = "graph_test_tc"
  val ttl = 86000
  val testStrings = List("1431788400000\tinsertBulk\te\t147229417\t99240432\ttalk_friend_long_term_agg\t{\"interests\":{},\"age_band\":67,\"account_id\":10099240432,\"gift_score\":0,\"service_user_id\":0,\"profile_id\":0,\"is_favorite\":\"false\",\"is_story_friend\":\"false\",\"talk_score\":1000,\"gender\":\"F\",\"interest_score\":0,\"score\":1096,\"birth_date\":\"\",\"birth_year\":1948,\"agedist_score\":966}")

  GraphSubscriberHelper.apply(phase, dbUrl, zkQuorum, kafkaBrokerList)

  test("GraphSubscriberHelper.store") {
    // actually we need to delete labelToReplace first for each test.
    Management.copyLabel(testLabelName, labelToReplace, Some(hTableName))

//
//    val msgs = (for {
//      i <- (1 until 10)
//      j <- (100 until 110)
//    } yield {
//        s"$currentTs\t$op\tedge\t$i\t$j\t$testLabelName"
//      }).toSeq
    val msgs = testStrings

    val stat = GraphSubscriberHelper.store(msgs, Some(labelToReplace))(None)
    println(stat)
  }
}