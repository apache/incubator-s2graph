package s2.counter.stream

import com.kakao.s2graph.core.mysqls.Label
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import play.api.libs.json.Json
import s2.config.{S2ConfigFactory, S2CounterConfig}
import s2.counter.core.CounterFunctions.HashMapAccumulable
import s2.counter.core.TimedQualifier.IntervalUnit
import s2.counter.core._
import s2.counter.core.v2.{ExactStorageGraph, GraphOperation, RankingStorageGraph}
import s2.helper.CounterAdmin
import s2.models.{Counter, DBModel, DefaultCounterModel}
import s2.spark.HashMapParam

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.concurrent.ExecutionContext.Implicits.global
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

  val exactCounter = new ExactCounter(S2ConfigFactory.config, new ExactStorageGraph(S2ConfigFactory.config))
  val rankingCounter = new RankingCounter(S2ConfigFactory.config, new RankingStorageGraph(S2ConfigFactory.config))

  val service = "test"
  val action = "test_case"
  val action_base = "test_case_base"
  val action_rate = "test_case_rate"
  val action_rate_threshold = "test_case_rate_threshold"
  val action_trend = "test_case_trend"

  override def beforeAll(): Unit = {
    DBModel.initialize(S2ConfigFactory.config)

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    ssc = new StreamingContext(conf, batchDuration)

    sc = ssc.sparkContext

    admin.setupCounterOnGraph()

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
    if (Label.findByName(action_base, useCache = false).isEmpty) {
      val strJs =
        s"""
           |{
           |  "label": "$action_base",
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

    // action
    admin.deleteCounter(service, action).foreach {
      case Success(v) =>
      case Failure(ex) =>
        println(s"$ex")
    }
    admin.createCounter(Counter(useFlag = true, 2, service, action, Counter.ItemType.STRING, autoComb = true, "", useRank = true))
    val policy = DefaultCounterModel.findByServiceAction(service, action).get

    // action_base
    admin.deleteCounter(service, action_base).foreach {
      case Success(v) =>
      case Failure(ex) =>
        println(s"$ex")
    }
    admin.createCounter(Counter(useFlag = true, 2, service, action_base, Counter.ItemType.STRING, autoComb = true, "", useRank = true))
    val basePolicy = DefaultCounterModel.findByServiceAction(service, action_base).get

    // action_rate
    admin.deleteCounter(service, action_rate).foreach {
      case Success(v) =>
      case Failure(ex) =>
        println(s"$ex")
    }
    admin.createCounter(Counter(useFlag = true, 2, service, action_rate, Counter.ItemType.STRING, autoComb = true, "gender,p1", useRank = true,
      rateActionId = Some(policy.id), rateBaseId = Some(basePolicy.id)))

    // action_rate_threshold
    admin.deleteCounter(service, action_rate_threshold).foreach {
      case Success(v) =>
      case Failure(ex) =>
        println(s"$ex")
    }
    admin.createCounter(Counter(useFlag = true, 2, service, action_rate_threshold, Counter.ItemType.STRING, autoComb = true, "gender,p1", useRank = true,
      rateActionId = Some(policy.id), rateBaseId = Some(basePolicy.id), rateThreshold = Some(3)))

    // action_trend
    admin.deleteCounter(service, action_trend).foreach {
      case Success(v) =>
      case Failure(ex) =>
        println(s"$ex")
    }
    admin.createCounter(Counter(useFlag = true, 2, service, action_trend, Counter.ItemType.STRING, autoComb = true, "p1", useRank = true,
      rateActionId = Some(policy.id), rateBaseId = Some(policy.id)))
 }

  override def afterAll(): Unit = {
    admin.deleteCounter(service, action)
    admin.deleteCounter(service, action_base)
    admin.deleteCounter(service, action_rate)
    admin.deleteCounter(service, action_rate_threshold)
    admin.deleteCounter(service, action_trend)
    if (ssc != null) {
      ssc.stop()
    }
  }

  "RankingCounterStreaming" should "update" in {
    val policy = DefaultCounterModel.findByServiceAction(service, action).get
//    val basePolicy = DefaultCounterModel.findByServiceAction(service, action_base).get

    rankingCounter.ready(policy) should equal (true)
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

    rankingCounter.ready(policy) should equal (true)

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

  it should "update rate ranking counter" in {
    val policy = DefaultCounterModel.findByServiceAction(service, action).get
    val basePolicy = DefaultCounterModel.findByServiceAction(service, action_base).get
    val ratePolicy = DefaultCounterModel.findByServiceAction(service, action_rate).get

    // update base policy
    val eq = ExactQualifier(TimedQualifier("M", 1433084400000l), "")
    val exactKey = ExactKey(basePolicy, "1", checkItemType = true)

    // check base item count
    exactCounter.updateCount(basePolicy, Seq(
      (exactKey, Map(eq -> 2l))
    ))
    Thread.sleep(1000)

    // direct get
    val baseCount = exactCounter.getCount(basePolicy, "1", Seq(IntervalUnit.MONTHLY), 1433084400000l, 1433084400000l, Map.empty[String, Set[String]])
    baseCount should not be empty
    baseCount.get should equal (FetchedCountsGrouped(exactKey, Map(
      (eq.tq.q, Map.empty[String, String]) -> Map(eq-> 2l)
    )))

    // related get
    val relatedCount = exactCounter.getRelatedCounts(basePolicy, Seq("1" -> Seq(eq)))
    relatedCount should not be empty
    relatedCount.get("1") should not be empty
    relatedCount.get("1").get should equal (Map(eq -> 2l))

    val data =
      s"""
        |{"success":true,"policyId":${policy.id},"item":"2","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":1}]}
        |{"success":true,"policyId":${policy.id},"item":"2","results":[{"interval":"M","dimension":"gender.M","ts":1433084400000,"value":1,"result":1}]}
        |{"success":true,"policyId":${basePolicy.id},"item":"2","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":2,"result":4}]}
        |{"success":true,"policyId":${basePolicy.id},"item":"2","results":[{"interval":"M","dimension":"gender.M","ts":1433084400000,"value":2,"result":4}]}
        |{"success":true,"policyId":${policy.id},"item":"2","results":[{"interval":"M","dimension":"p1.1","ts":1433084400000,"value":1,"result":1}]}
        |{"success":true,"policyId":${policy.id},"item":"1","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":1}]}
        |{"success":true,"policyId":${basePolicy.id},"item":"2","results":[{"interval":"M","dimension":"p1.1","ts":1433084400000,"value":2,"result":4}]}
        |{"success":true,"policyId":${policy.id},"item":"1","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":2}]}
      """.stripMargin.trim
    //    println(data)
    val rdd = sc.parallelize(Seq(("", data)))

    //    rdd.foreachPartition { part =>
    //      part.foreach(println)
    //    }

    val trxLogRdd = CounterFunctions.makeTrxLogRdd(rdd, 2)
    trxLogRdd.count() should equal (data.trim.split('\n').length)

    val itemRankingRdd = CounterFunctions.makeItemRankingRdd(trxLogRdd, 2)
    itemRankingRdd.foreach(println)

    val result = CounterFunctions.rateRankingCount(itemRankingRdd, 2).collect().toMap.filterKeys(key => key.policyId == ratePolicy.id)
    result.foreach(println)
    result should have size 3

    val acc: HashMapAccumulable = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    // rate ranking
    val key = RankingKey(ratePolicy.id, 2, ExactQualifier(TimedQualifier("M", 1433084400000L), ""))
    val value = result.get(key)

//    println(key, value)

    value should not be empty
    value.get.get("1") should not be empty
    value.get.get("1").get should equal (RankingValue(1, 0))
    value.get.get("2").get should equal (RankingValue(0.25, 0))

    val key2 = RankingKey(ratePolicy.id, 2, ExactQualifier(TimedQualifier("M", 1433084400000L), "p1.1"))
    val value2 = result.get(key2)

//    println(key2, value2)

    val values = value.map(v => (key, v)).toSeq ++ value2.map(v => (key2, v)).toSeq
    println(s"values: $values")

    // delete, update and get
    rankingCounter.delete(key)
    rankingCounter.delete(key2)
    Thread.sleep(1000)
    CounterFunctions.updateRankingCounter(values, acc)
    // for update graph
    Thread.sleep(1000)

    val rst = rankingCounter.getTopK(key)
    rst should not be empty
    rst.get.values should equal (Seq(("1", 1d), ("2", 0.25d)))

    val rst2 = rankingCounter.getTopK(key2)
    rst2 should not be empty
    rst2.get.values should equal (Seq(("2", 0.25d)))
  }

  it should "update rate ranking counter with threshold" in {
    val policy = DefaultCounterModel.findByServiceAction(service, action).get
    val basePolicy = DefaultCounterModel.findByServiceAction(service, action_base).get
    val ratePolicy = DefaultCounterModel.findByServiceAction(service, action_rate_threshold).get

    val data =
      s"""
        |{"success":true,"policyId":${policy.id},"item":"1","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":1}]}
        |{"success":true,"policyId":${policy.id},"item":"1","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":2}]}
        |{"success":true,"policyId":${policy.id},"item":"2","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":1,"result":1}]}
        |{"success":true,"policyId":${policy.id},"item":"2","results":[{"interval":"M","dimension":"gender.M","ts":1433084400000,"value":1,"result":1}]}
        |{"success":true,"policyId":${basePolicy.id},"item":"2","results":[{"interval":"M","dimension":"","ts":1433084400000,"value":2,"result":4}]}
        |{"success":true,"policyId":${basePolicy.id},"item":"2","results":[{"interval":"M","dimension":"gender.M","ts":1433084400000,"value":2,"result":4}]}
      """.stripMargin.trim
    //    println(data)
    val rdd = sc.parallelize(Seq(("", data)))

    //    rdd.foreachPartition { part =>
    //      part.foreach(println)
    //    }

    val trxLogRdd = CounterFunctions.makeTrxLogRdd(rdd, 2)
    trxLogRdd.count() should equal (data.trim.split('\n').length)

    val itemRankingRdd = CounterFunctions.makeItemRankingRdd(trxLogRdd, 2)
    itemRankingRdd.foreach(println)

    val result = CounterFunctions.rateRankingCount(itemRankingRdd, 2).collect().toMap.filterKeys(key => key.policyId == ratePolicy.id)
    result.foreach(println)
    result should have size 2

    val acc: HashMapAccumulable = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    // rate ranking
    val key = RankingKey(ratePolicy.id, 2, ExactQualifier(TimedQualifier("M", 1433084400000L), ""))
    val value = result.get(key)

    value should not be empty
    value.get.get("1") should be (None)
    value.get.get("2").get should equal (RankingValue(0.25, 0))

    // delete, update and get
    rankingCounter.delete(key)
    Thread.sleep(1000)
    CounterFunctions.updateRankingCounter(Seq((key, value.get)), acc)
    Thread.sleep(1000)
    val rst = rankingCounter.getTopK(key)

    rst should not be empty
    rst.get.values should equal (Seq(("2", 0.25d)))
  }

  it should "update trend ranking counter" in {
    val policy = DefaultCounterModel.findByServiceAction(service, action).get
    val trendPolicy = DefaultCounterModel.findByServiceAction(service, action_trend).get

    val exactKey1 = ExactKey(policy, "1", checkItemType = true)
    val exactKey2 = ExactKey(policy, "2", checkItemType = true)
    // update old key value
    val tq1 = TimedQualifier("M", 1435676400000l)
    val tq2 = TimedQualifier("M", 1427814000000l)
    exactCounter.updateCount(policy, Seq(
      exactKey1 -> Map(ExactQualifier(tq1.add(-1), "") -> 1l, ExactQualifier(tq2.add(-1), "") -> 92l)
    ))
    val eq1 = ExactQualifier(tq1, "")
    val eq2 = ExactQualifier(tq2, "")

    val oldCount = exactCounter.getPastCounts(policy, Seq("1" -> Seq(eq1, eq2), "2" -> Seq(eq1, eq1.copy(dimension = "gender.M"))))
    oldCount should not be empty
    oldCount.get("1").get should equal(Map(eq1 -> 1l, eq2 -> 92l))
    oldCount.get("2") should be (None)

    val data =
      s"""
        |{"success":true,"policyId":${policy.id},"item":"1","results":[{"interval":"M","dimension":"","ts":1435676400000,"value":1,"result":1}]}
        |{"success":true,"policyId":${policy.id},"item":"1","results":[{"interval":"M","dimension":"","ts":1435676400000,"value":1,"result":2}]}
        |{"success":true,"policyId":${policy.id},"item":"2","results":[{"interval":"M","dimension":"","ts":1435676400000,"value":1,"result":1}]}
        |{"success":true,"policyId":${policy.id},"item":"2","results":[{"interval":"M","dimension":"gender.M","ts":1435676400000,"value":1,"result":1}]}
        |{"success":true,"policyId":${policy.id},"item":"1","results":[{"interval":"M","dimension":"","ts":1427814000000,"value":1,"result":92}]}
      """.stripMargin.trim
    //    println(data)
    val rdd = sc.parallelize(Seq(("", data)))

    //    rdd.foreachPartition { part =>
    //      part.foreach(println)
    //    }

    val trxLogRdd = CounterFunctions.makeTrxLogRdd(rdd, 2)
    trxLogRdd.count() should equal (data.trim.split('\n').length)

    val itemRankingRdd = CounterFunctions.makeItemRankingRdd(trxLogRdd, 2)
    itemRankingRdd.foreach(println)

    val result = CounterFunctions.trendRankingCount(itemRankingRdd, 2).collect().toMap
    result.foreach(println)
    // dimension gender.M is ignored, because gender is not defined dimension in trend policy.
    result should have size 2

    val acc: HashMapAccumulable = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    // trend ranking
    val key = RankingKey(trendPolicy.id, 2, ExactQualifier(TimedQualifier("M", 1435676400000L), ""))
    val value = result.get(key)

    value should not be empty
    value.get.get("1").get should equal (RankingValue(2, 0))
    value.get.get("2").get should equal (RankingValue(1, 0))

    val key2 = RankingKey(trendPolicy.id, 2, ExactQualifier(TimedQualifier("M", 1427814000000L), ""))
    val value2 = result.get(key2)

    value2 should not be empty
    value2.get.get("1").get should equal (RankingValue(1, 0))

    // delete, update and get
    rankingCounter.delete(key)
    Thread.sleep(1000)
    CounterFunctions.updateRankingCounter(Seq((key, value.get)), acc)
    Thread.sleep(1000)
    val rst = rankingCounter.getTopK(key)

    rst should not be empty
    rst.get.values should equal (Seq("1" -> 2, "2" -> 1))
  }
}
