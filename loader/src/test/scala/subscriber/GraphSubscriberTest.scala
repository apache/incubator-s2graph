package subscriber

import com.daumkakao.s2graph.core.{Label, Service, Management}
import org.scalatest.{ FunSuite, Matchers }
import play.api.libs.json.{JsBoolean, JsNumber}
import s2.spark.WithKafka

class GraphSubscriberTest extends FunSuite with Matchers with WithKafka {
  val phase = "dev"
  val dbUrl = "jdbc:mysql://localhost:3306/graph_dev"
  val zkQuorum = "localhost"
  val kafkaBrokerList = "localhost:9099"
  val currentTs = System.currentTimeMillis()
  val op = "insertBulk"
  val testLabelName = "s2graph_label_test"
  val labelToReplace = "s2graph_label_test_new"
  val serviceName = "s2graph"
  val columnName = "user_id"
  val columnType = "long"
  val indexProps = Seq("time" -> JsNumber(0), "weight" -> JsNumber(0))
  val props = Seq("is_hidden" -> JsBoolean(false), "is_blocked" -> JsBoolean(false))
  val hTableName = "s2graph-dev_new"
  val ttl = 86000
  val testStrings = List("1431788400000\tinsertBulk\te\t147229417\t99240432\ts2graph_label_test\t{\"is_hidden\": true}")

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