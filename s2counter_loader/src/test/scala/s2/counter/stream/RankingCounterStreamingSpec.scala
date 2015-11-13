package s2.counter.stream

import com.kakao.s2graph.core.mysqls.Label
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.libs.json.Json
import s2.config.{S2ConfigFactory, S2CounterConfig}
import s2.counter.core.CounterFunctions.HashMapAccumulable
import s2.counter.core._
import s2.counter.core.v2.{GraphOperation, RankingStorageGraph}
import s2.helper.CounterAdmin
import s2.models.{Counter, DBModel, DefaultCounterModel}
import s2.spark.HashMapParam

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.util.{Failure, Success}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 17..
 */
class RankingCounterStreamingSpec extends FlatSpec with BeforeAndAfterAll with Matchers {
  private val master = "local[2]"
  private val appName = "ranking_counter_streaming"
  private val batchDuration = Seconds(1)

  private var sc: SparkContext = _
  private var ssc: StreamingContext = _

  val admin = new CounterAdmin(S2ConfigFactory.config)
  val graphOp = new GraphOperation(S2ConfigFactory.config)
  val s2config = new S2CounterConfig(S2ConfigFactory.config)
  val rankingCounter = new RankingCounter(S2ConfigFactory.config, new RankingStorageGraph(S2ConfigFactory.config))

  val service = "test"
  val action = "test_case"

  override def beforeAll(): Unit = {
    DBModel.initialize(S2ConfigFactory.config)

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    ssc = new StreamingContext(conf, batchDuration)

    sc = ssc.sparkContext

    admin.setupCounterOnGraph

    // create test_case label
    com.kakao.s2graph.core.Management.createService(service, s2config.HBASE_ZOOKEEPER_QUORUM, s"${service}_dev", 1, None, "gz")
    if (Label.findByName(action, useCache = false).isEmpty) {
      val strJs =
        s"""
           |{
           |  "label": "$action",
           |  "srcServiceName": "$service",
           |  "srcColumnName": "src",
           |  "srcColumnType": "string",
           |  "tgtServiceName": "$service",
           |  "tgtColumnName": "$action",
           |  "tgtColumnType": "string",
           |  "indices": [
           |  ],
           |  "props": [
           |  ]
           |}
       """.stripMargin
      graphOp.createLabel(Json.parse(strJs))
    }

    admin.deleteCounter(service, action).foreach {
      case Success(v) =>
      case Failure(ex) =>
        println(s"$ex")
        throw ex
    }
    admin.createCounter(Counter(useFlag = true, 2, service, action, Counter.ItemType.STRING, autoComb = true, "", useRank = true))
  }

  override def afterAll(): Unit = {
    admin.deleteCounter(service, action)
    if (ssc != null) {
      ssc.stop()
    }
  }

  "RankingCounterStreaming" should "update" in {
    val policy = DefaultCounterModel.findByServiceAction(service, action, useCache = false).get

    rankingCounter.ready(policy) should equal(true)
    val data =
      s"""
         |{"success":true,"policyId":${policy.id},"item":"1","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":3}]}
         |{"success":false,"policyId":${policy.id},"item":"2","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":2}]}
         |{"success":true,"policyId":${policy.id},"item":"3","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":1}]}
         |{"success":true,"policyId":${policy.id},"item":"3","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":2}]}
         |{"success":true,"policyId":${policy.id},"item":"4","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":1}]}
      """.stripMargin.trim
    //    println(data)
    val rdd = sc.parallelize(Seq(("", data)))

    //    rdd.foreachPartition { part =>
    //      part.foreach(println)
    //    }

    val result = CounterFunctions.makeRankingRdd(rdd, 2).collect().toMap

    //    result.foreachPartition { part =>
    //      part.foreach(println)
    //    }

    val acc: HashMapAccumulable = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    result should not be empty
    val rankKey = RankingKey(policy.id, policy.version, ExactQualifier(TimedQualifier("M", 1433084400000L), ""))
    result should contain (rankKey -> Map(
      "1" -> RankingValue(3, 1),
      "3" -> RankingValue(2, 2),
      "4" -> RankingValue(1, 1)
    ))

    val key = RankingKey(policy.id, policy.version, ExactQualifier(TimedQualifier("M", 1433084400000L), ""))
    val value = result.get(key)

    value should not be empty
    value.get.get("1").get should equal (RankingValue(3, 1))
    value.get.get("2") shouldBe empty
    value.get.get("3").get should equal (RankingValue(2, 2))

    rankingCounter.ready(policy) should equal(true)

    // delete, update and get
    rankingCounter.delete(key)
    Thread.sleep(1000)
    CounterFunctions.updateRankingCounter(Seq((key, value.get)), acc)
    Thread.sleep(1000)
    val rst = rankingCounter.getTopK(key)

    rst should not be empty
//    rst.get.totalScore should equal(4f)
    rst.get.values should contain allOf(("3", 2d), ("4", 1d), ("1", 3d))
  }

//  "rate by base" >> {
//    val data =
//      """
//        |{"success":true,"policyId":42,"item":"2","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":2,"result":4}]}
//      """.stripMargin.trim
//    val rdd = sc.parallelize(Seq(("", data)))
//
//    val trxLogRdd = CounterFunctions.makeTrxLogRdd(rdd, 2).collect()
//    trxLogRdd.foreach { log =>
//      CounterFunctions.rateBaseRankingMapper(log) must not be empty
//    }
//
//    true must_== true
//  }

//  it should "update rate ranking counter" in {
//    S2ConfigFactory.config.getString("db.default.url") must_== "jdbc:mysql://nuk151.kr2.iwilab.com:13306/graph_alpha"
//
//    val data =
//      """
//        |{"success":true,"policyId":7,"item":"2","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":1}]}
//        |{"success":true,"policyId":7,"item":"2","results":[{"interval":"M","dimension":"gender.M","ts":1433084400000,"value":1,"result":1}]}
//        |{"success":true,"policyId":42,"item":"2","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":2,"result":4}]}
//        |{"success":true,"policyId":42,"item":"2","results":[{"interval":"M","dimension":"gender.M","ts":1433084400000,"value":2,"result":4}]}
//        |{"success":true,"policyId":7,"item":"2","results":[{"interval":"M","dimension":"p1.1","ts":1433084400000,"value":1,"result":1}]}
//        |{"success":true,"policyId":7,"item":"1","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":1}]}
//        |{"success":true,"policyId":42,"item":"2","results":[{"interval":"M","dimension":"p1.1","ts":1433084400000,"value":2,"result":4}]}
//        |{"success":true,"policyId":7,"item":"1","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":2}]}
//      """.stripMargin.trim
//    //    println(data)
//    val rdd = sc.parallelize(Seq(("", data)))
//
//    //    rdd.foreachPartition { part =>
//    //      part.foreach(println)
//    //    }
//
//    val trxLogRdd = CounterFunctions.makeTrxLogRdd(rdd, 2)
//    trxLogRdd.count() must_== data.trim.split('\n').length
//
//    val itemRankingRdd = CounterFunctions.makeItemRankingRdd(trxLogRdd, 2)
//    itemRankingRdd.foreach(println)
//
//    val result = CounterFunctions.rateRankingCount(itemRankingRdd, 2).collect().toMap
//    result.foreach(println)
//    result must have size 4
//
//    val acc: HashMapAccumulable = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))
//
//    // rate ranking
//    val key = RankingKey(43, 2, ExactQualifier(TimedQualifier("M", 1433084400000L), ""))
//    val value = result.get(key)
//
////    println(key, value)
//
//    value must beSome
//    value.get.get("1") must beSome
//    value.get.get("1").get must_== RankingValue(1, 0)
//    value.get.get("2").get must_== RankingValue(0.25, 0)
//
//    val key2 = RankingKey(43, 2, ExactQualifier(TimedQualifier("M", 1433084400000L), "p1.1"))
//    val value2 = result.get(key2)
//
////    println(key2, value2)
//
//    val values = value.map(v => (key, v)).toSeq ++ value2.map(v => (key2, v)).toSeq
//    println(s"values: $values")
//
//    // delete, update and get
//    CounterFunctions.deleteRankingCounter(key)
//    CounterFunctions.deleteRankingCounter(key2)
//
//    CounterFunctions.updateRankingCounter(values, acc)
//
//    // for update graph
//    Thread.sleep(1000)
//
//    val rst = CounterFunctions.getRankingCounter(key)
//    rst must beSome
//    rst.get.totalScore must_== 0d
//    rst.get.values must containTheSameElementsAs(Seq(("2", 0.25d), ("1", 1d)))
//
//    val rst2 = CounterFunctions.getRankingCounter(key2)
//    rst2 must beSome
//    rst2.get.totalScore must_== 0d
//    rst2.get.values must containTheSameElementsAs(Seq(("2", 0.25d)))
//  }
//
//  it should "update rate ranking counter with threshold" in {
//    S2ConfigFactory.config.getString("db.default.url") must_== "jdbc:mysql://nuk151.kr2.iwilab.com:13306/graph_alpha"
//
//    val data =
//      """
//        |{"success":true,"policyId":7,"item":"1","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":1}]}
//        |{"success":true,"policyId":7,"item":"1","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":2}]}
//        |{"success":true,"policyId":7,"item":"2","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":1}]}
//        |{"success":true,"policyId":7,"item":"2","results":[{"interval":"M","dimension":"gender.M","ts":1433084400000,"value":1,"result":1}]}
//        |{"success":true,"policyId":42,"item":"2","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":2,"result":4}]}
//        |{"success":true,"policyId":42,"item":"2","results":[{"interval":"M","dimension":"gender.M","ts":1433084400000,"value":2,"result":4}]}
//      """.stripMargin.trim
//    //    println(data)
//    val rdd = sc.parallelize(Seq(("", data)))
//
//    //    rdd.foreachPartition { part =>
//    //      part.foreach(println)
//    //    }
//
//    val trxLogRdd = CounterFunctions.makeTrxLogRdd(rdd, 2)
//    trxLogRdd.count() must_== data.trim.split('\n').length
//
//    val itemRankingRdd = CounterFunctions.makeItemRankingRdd(trxLogRdd, 2)
//    itemRankingRdd.foreach(println)
//
//    val result = CounterFunctions.rateRankingCount(itemRankingRdd, 2).collect().toMap
//    result.foreach(println)
//    result must have size 2
//
//    val acc: HashMapAccumulable = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))
//
//    // rate ranking
//    val key = RankingKey(52, 2, ExactQualifier(TimedQualifier("M", 1433084400000L), ""))
//    val value = result.get(key)
//
//    value must beSome
//    value.get.get("1") must beNone
//    value.get.get("2").get must_== RankingValue(0.25, 0)
//    //
//    //    // delete, update and get
//    //    CounterFunctions.deleteRankingCounter(key)
//    //    CounterFunctions.updateRankingCounter(Seq((key, value.get)), acc)
//    //    val rst = CounterFunctions.getRankingCounter(key)
//    //
//    //    rst must beSome
//    //    rst.get.totalScore must_== 4d
//    //    rst.get.values must containTheSameElementsAs(Seq(("3", 2d), ("4", 1d), ("1", 1d)))
//  }
//
//  it should "update trend ranking counter" in {
//    S2ConfigFactory.config.getString("db.default.url") must_== "jdbc:mysql://nuk151.kr2.iwilab.com:13306/graph_alpha"
//
//    val data =
//      """
//        |{"success":true,"policyId":7,"item":"1","results":[{"interval":"M","dimension":"","ts":1435676400000,"value":1,"result":1}]}
//        |{"success":true,"policyId":7,"item":"1","results":[{"interval":"M","dimension":"","ts":1435676400000,"value":1,"result":2}]}
//        |{"success":true,"policyId":7,"item":"2","results":[{"interval":"M","dimension":"","ts":1435676400000,"value":1,"result":1}]}
//        |{"success":true,"policyId":7,"item":"2","results":[{"interval":"M","dimension":"gender.M","ts":1435676400000,"value":1,"result":1}]}
//        |{"success":true,"policyId":7,"item":"1","results":[{"interval":"M","dimension":"","ts":1427814000000,"value":1,"result":92}]}
//      """.stripMargin.trim
//    //    println(data)
//    val rdd = sc.parallelize(Seq(("", data)))
//
//    //    rdd.foreachPartition { part =>
//    //      part.foreach(println)
//    //    }
//
//    val trxLogRdd = CounterFunctions.makeTrxLogRdd(rdd, 2)
//    trxLogRdd.count() must_== data.trim.split('\n').length
//
//    val itemRankingRdd = CounterFunctions.makeItemRankingRdd(trxLogRdd, 2)
//    itemRankingRdd.foreach(println)
//
//    val result = CounterFunctions.trendRankingCount(itemRankingRdd, 2).collect().toMap
//    result.foreach(println)
//    result must have size 2
//
//    val acc: HashMapAccumulable = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))
//
//    // trend ranking
//    val key = RankingKey(55, 2, ExactQualifier(TimedQualifier("M", 1435676400000L), ""))
//    val value = result.get(key)
//
//    value must beSome
//    value.get.get("1").get must_== RankingValue(2, 0)
//    value.get.get("2").get must_== RankingValue(1, 0)
//
//    val key2 = RankingKey(55, 2, ExactQualifier(TimedQualifier("M", 1427814000000L), ""))
//    val value2 = result.get(key2)
//
//    value2 must beSome
//    value2.get.get("1").get must_== RankingValue(1, 0)
//    //
//    //    // delete, update and get
//    //    CounterFunctions.deleteRankingCounter(key)
//    //    CounterFunctions.updateRankingCounter(Seq((key, value.get)), acc)
//    //    val rst = CounterFunctions.getRankingCounter(key)
//    //
//    //    rst must beSome
//    //    rst.get.totalScore must_== 4d
//    //    rst.get.values must containTheSameElementsAs(Seq(("3", 2d), ("4", 1d), ("1", 1d)))
//  }
}
