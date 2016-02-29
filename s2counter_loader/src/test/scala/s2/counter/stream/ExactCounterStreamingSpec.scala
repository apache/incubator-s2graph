package s2.counter.stream

import com.kakao.s2graph.core.GraphUtil
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
  * Created by hsleep(honeysleep@gmail.com) on 2015. 11. 19..
  */
class ExactCounterStreamingSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  private val master = "local[2]"
  private val appName = "exact_counter_streaming"
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

  override def beforeAll(): Unit = {
    DBModel.initialize(S2ConfigFactory.config)

    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    ssc = new StreamingContext(conf, batchDuration)

    sc = ssc.sparkContext

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

    // action
    admin.deleteCounter(service, action).foreach {
      case Success(v) =>
      case Failure(ex) =>
        println(s"$ex")
    }
    admin.createCounter(Counter(useFlag = true, 2, service, action, Counter.ItemType.STRING, autoComb = true, "is_shared,relationship", useRank = true))
  }

  override def afterAll(): Unit = {
    admin.deleteCounter(service, action)
    if (ssc != null) {
      ssc.stop()
    }
  }

  "ExactCounter" should "update" in {
    val policy = DefaultCounterModel.findByServiceAction(service, action).get
    val data =
      s"""
        |1434534565675	$service	$action	70362200_94013572857366866	{"is_shared":"false","relationship":"FE"}	{"userId":"48255079","userIdType":"profile_id","value":"1"}
        |1434534565675	$service	$action	46889329_94013502934177075	{"is_shared":"false","relationship":"FE"}	{"userId":"48255079","userIdType":"profile_id","value":"1"}
        |1434534566220	$service	$action	51223360_94013140590929619	{"is_shared":"false","relationship":"FE"}	{"userId":"312383","userIdType":"profile_id","value":"1"}
        |1434534566508	$service	$action	63808459_94013420047377826	{"is_shared":"false","relationship":"FE"}	{"userId":"21968241","userIdType":"profile_id","value":"1"}
        |1434534566210	$service	$action	46889329_94013502934177075	{"is_shared":"false","relationship":"FE"}	{"userId":"6062217","userIdType":"profile_id","value":"1"}
        |1434534566459	$service	$action	49699692_94012186431261763	{"is_shared":"false","relationship":"FE"}	{"userId":"67863471","userIdType":"profile_id","value":"1"}
        |1434534565681	$service	$action	64556827_94012311028641810	{"is_shared":"false","relationship":"FE"}	{"userId":"19381218","userIdType":"profile_id","value":"1"}
        |1434534565865	$service	$action	41814266_94012477588942163	{"is_shared":"false","relationship":"FE"}	{"userId":"19268547","userIdType":"profile_id","value":"1"}
        |1434534565865	$service	$action	66697741_94007840665633458	{"is_shared":"false","relationship":"FE"}	{"userId":"19268547","userIdType":"profile_id","value":"1"}
        |1434534566142	$service	$action	66444074_94012737377133826	{"is_shared":"false","relationship":"FE"}	{"userId":"11917195","userIdType":"profile_id","value":"1"}
        |1434534566077	$service	$action	46889329_94013502934177075	{"is_shared":"false","relationship":"FE"}	{"userId":"37709890","userIdType":"profile_id","value":"1"}
        |1434534565938	$service	$action	40921487_94012905738975266	{"is_shared":"false","relationship":"FE"}	{"userId":"59869223","userIdType":"profile_id","value":"1"}
        |1434534566033	$service	$action	64506628_93994707216829506	{"is_shared":"false","relationship":"FE"}	{"userId":"50375575","userIdType":"profile_id","value":"1"}
        |1434534566451	$service	$action	40748868_94013448321919139	{"is_shared":"false","relationship":"FE"}	{"userId":"12249539","userIdType":"profile_id","value":"1"}
        |1434534566669	$service	$action	64499956_94013227717457106	{"is_shared":"false","relationship":"FE"}	{"userId":"25167419","userIdType":"profile_id","value":"1"}
        |1434534566669	$service	$action	66444074_94012737377133826	{"is_shared":"false","relationship":"FE"}	{"userId":"25167419","userIdType":"profile_id","value":"1"}
        |1434534566318	$service	$action	64774665_94012837889027027	{"is_shared":"true","relationship":"F"}	{"userId":"71557816","userIdType":"profile_id","value":"1"}
        |1434534566274	$service	$action	67075480_94008509166933763	{"is_shared":"false","relationship":"FE"}	{"userId":"57931860","userIdType":"profile_id","value":"1"}
        |1434534566659	$service	$action	46889329_94013502934177075	{"is_shared":"false","relationship":"FE"}	{"userId":"19990823","userIdType":"profile_id","value":"1"}
        |1434534566250	$service	$action	70670053_93719933175630611	{"is_shared":"true","relationship":"F"}	{"userId":"68897412","userIdType":"profile_id","value":"1"}
        |1434534566402	$service	$action	46889329_94013502934177075	{"is_shared":"false","relationship":"FE"}	{"userId":"15541439","userIdType":"profile_id","value":"1"}
        |1434534566122	$service	$action	48890741_94013463616012786	{"is_shared":"false","relationship":"FE"}	{"userId":"48040409","userIdType":"profile_id","value":"1"}
        |1434534566055	$service	$action	64509008_94002318232678546	{"is_shared":"true","relationship":"F"}	{"userId":"46532039","userIdType":"profile_id","value":"1"}
        |1434534565994	$service	$action	66644368_94009163363033795	{"is_shared":"false","relationship":"FE"}	{"userId":"4143147","userIdType":"profile_id","value":"1"}
        |1434534566448	$service	$action	64587644_93938555963733954	{"is_shared":"false","relationship":"FE"}	{"userId":"689042","userIdType":"profile_id","value":"1"}
        |1434534565935	$service	$action	52812511_94012009551561315	{"is_shared":"false","relationship":"FE"}	{"userId":"35509692","userIdType":"profile_id","value":"1"}
        |1434534566544	$service	$action	70452048_94008573197583762	{"is_shared":"false","relationship":"FE"}	{"userId":"5172421","userIdType":"profile_id","value":"1"}
        |1434534565929	$service	$action	54547023_94013384964278435	{"is_shared":"false","relationship":"FE"}	{"userId":"33556498","userIdType":"profile_id","value":"1"}
        |1434534566358	$service	$action	46889329_94013502934177075	{"is_shared":"false","relationship":"FE"}	{"userId":"8987346","userIdType":"profile_id","value":"1"}
        |1434534566057	$service	$action	67075480_94008509166933763	{"is_shared":"false","relationship":"FE"}	{"userId":"35134964","userIdType":"profile_id","value":"1"}
        |1434534566140	$service	$action	54547023_94013384964278435	{"is_shared":"false","relationship":"FE"}	{"userId":"11900315","userIdType":"profile_id","value":"1"}
        |1434534566158	$service	$action	64639374_93888330176053635	{"is_shared":"true","relationship":"F"}	{"userId":"49996643","userIdType":"profile_id","value":"1"}
        |1434534566025	$service	$action	67265128_94009084771192002	{"is_shared":"false","relationship":"FE"}	{"userId":"37801480","userIdType":"profile_id","value":"1"}
      """.stripMargin.trim
    //    println(data)
    val rdd = sc.parallelize(Seq(("", data)))

    //    rdd.foreachPartition { part =>
    //      part.foreach(println)
    //    }
    val resultRdd = CounterFunctions.makeExactRdd(rdd, 2)
    val result = resultRdd.collect().toMap

    //    result.foreachPartition { part =>
    //      part.foreach(println)
    //    }

    val parsed = {
      for {
        line <- GraphUtil.parseString(data)
        item <- CounterEtlItem(line).toSeq
        ev <- CounterFunctions.exactMapper(item).toSeq
      } yield {
        ev
      }
    }
    val parsedResult = parsed.groupBy(_._1).mapValues(values => values.map(_._2).reduce(CounterFunctions.reduceValue[ExactQualifier, Long](_ + _, 0L)))

    //    parsedResult.foreach { case (k, v) =>
    //      println(k, v)
    //    }

    result should not be empty
    result should equal (parsedResult)

    val itemId = "46889329_94013502934177075"
    val key = ExactKey(DefaultCounterModel.findByServiceAction(service, action).get, itemId, checkItemType = true)
    val value = result.get(key)

    value should not be empty
    value.get.get(ExactQualifier(TimedQualifier("t", 0), Map.empty[String, String])) should equal (Some(6L))

    exactCounter.getCount(policy, itemId, Seq(IntervalUnit.TOTAL), 0, 0, Map.empty[String, Set[String]]) should be (None)

    val acc: HashMapAccumulable = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))
    resultRdd.foreachPartition { part =>
      CounterFunctions.updateExactCounter(part.toSeq, acc)
    }

    Option(FetchedCountsGrouped(key, Map(
      (IntervalUnit.TOTAL, Map.empty[String, String]) -> Map(ExactQualifier(TimedQualifier("t", 0), "") -> 6l)
    ))).foreach { expected =>
      exactCounter.getCount(policy, itemId, Seq(IntervalUnit.TOTAL), 0, 0, Map.empty[String, Set[String]]) should be (Some(expected))
    }
    Option(FetchedCountsGrouped(key, Map(
      (IntervalUnit.TOTAL, Map("is_shared" -> "false")) -> Map(ExactQualifier(TimedQualifier("t", 0), "is_shared.false") -> 6l)
    ))).foreach { expected =>
      exactCounter.getCount(policy, itemId, Seq(IntervalUnit.TOTAL), 0, 0, Map("is_shared" -> Set("false"))) should be (Some(expected))
    }
    Option(FetchedCountsGrouped(key, Map(
      (IntervalUnit.TOTAL, Map("relationship" -> "FE")) -> Map(ExactQualifier(TimedQualifier("t", 0), "relationship.FE") -> 6l)
    ))).foreach { expected =>
      exactCounter.getCount(policy, itemId, Seq(IntervalUnit.TOTAL), 0, 0, Map("relationship" -> Set("FE"))) should be (Some(expected))
    }
    Option(FetchedCountsGrouped(key, Map(
      (IntervalUnit.TOTAL, Map("is_shared" -> "false", "relationship" -> "FE")) -> Map(ExactQualifier(TimedQualifier("t", 0), "is_shared.relationship.false.FE") -> 6l)
    ))).foreach { expected =>
      exactCounter.getCount(policy, itemId, Seq(IntervalUnit.TOTAL), 0, 0, Map("is_shared" -> Set("false"), "relationship" -> Set("FE"))) should be (Some(expected))
    }
  }
}
