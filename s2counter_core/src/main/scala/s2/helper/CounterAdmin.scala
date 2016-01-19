package s2.helper

import com.kakao.s2graph.core.Graph
import com.kakao.s2graph.core.mysqls.Label
import com.typesafe.config.Config
import play.api.libs.json.Json
import s2.config.S2CounterConfig
import s2.counter.core.v1.{ExactStorageAsyncHBase, RankingStorageRedis}
import s2.counter.core.v2.{ExactStorageGraph, GraphOperation, RankingStorageGraph}
import s2.counter.core.{ExactCounter, RankingCounter}
import s2.models.{Counter, CounterModel}

import scala.util.Try

/**
  * Created by hsleep(honeysleep@gmail.com) on 2015. 11. 11..
  */
class CounterAdmin(config: Config) {
  val s2config = new S2CounterConfig(config)
  val counterModel = new CounterModel(config)
  val graphOp = new GraphOperation(config)
  val s2graph = new Graph(config)(scala.concurrent.ExecutionContext.global)
  val storageManagement = new com.kakao.s2graph.core.Management(s2graph)

  def setupCounterOnGraph(): Unit = {
    // create s2counter service
    val service = "s2counter"
    storageManagement.createService(service, s2config.HBASE_ZOOKEEPER_QUORUM, s"$service-${config.getString("phase")}", 1, None, "gz")
    // create bucket label
    val label = "s2counter_topK_bucket"
    if (Label.findByName(label, useCache = false).isEmpty) {
      val strJs =
        s"""
           |{
           |  "label": "$label",
           |  "srcServiceName": "s2counter",
           |  "srcColumnName": "dimension",
           |  "srcColumnType": "string",
           |  "tgtServiceName": "s2counter",
           |  "tgtColumnName": "bucket",
           |  "tgtColumnType": "string",
           |  "indices": [
           |    {"name": "time", "propNames": ["time_unit", "date_time"]}
           |	],
           |	"props": [
           |      {"name": "time_unit", "dataType": "string", "defaultValue": ""},
           |      {"name": "date_time", "dataType": "long", "defaultValue": 0}
           |	],
           |  "hTableName": "s2counter_60",
           |  "hTableTTL": 5184000
           |}
        """.stripMargin
      graphOp.createLabel(Json.parse(strJs))
    }
  }

  def createCounter(policy: Counter): Unit = {
    val newPolicy = policy.copy(hbaseTable = Some(makeHTableName(policy)))
    prepareStorage(newPolicy)
    counterModel.createServiceAction(newPolicy)
  }

  def deleteCounter(service: String, action: String): Option[Try[Unit]] = {
    for {
      policy <- counterModel.findByServiceAction(service, action, useCache = false)
    } yield {
      Try {
        exactCounter(policy).destroy(policy)
        if (policy.useRank) {
          rankingCounter(policy).destroy(policy)
        }
        counterModel.deleteServiceAction(policy)
      }
    }
  }

  def prepareStorage(policy: Counter): Unit = {
    if (policy.rateActionId.isEmpty) {
      // if defined rate action, do not use exact counter
      exactCounter(policy).prepare(policy)
    }
    if (policy.useRank) {
      rankingCounter(policy).prepare(policy)
    }
  }

  def prepareStorage(policy: Counter, version: Byte): Unit = {
    // this function to prepare storage by version parameter instead of policy.version
    prepareStorage(policy.copy(version = version))
  }

  private val exactCounterMap = Map(
    s2.counter.VERSION_1 -> new ExactCounter(config, new ExactStorageAsyncHBase(config)),
    s2.counter.VERSION_2 -> new ExactCounter(config, new ExactStorageGraph(config))
  )
  private val rankingCounterMap = Map(
    s2.counter.VERSION_1 -> new RankingCounter(config, new RankingStorageRedis(config)),
    s2.counter.VERSION_2 -> new RankingCounter(config, new RankingStorageGraph(config))
  )

  private val tablePrefixMap = Map (
    s2.counter.VERSION_1 -> "s2counter",
    s2.counter.VERSION_2 -> "s2counter_v2"
  )

  def exactCounter(version: Byte): ExactCounter = exactCounterMap(version)
  def exactCounter(policy: Counter): ExactCounter = exactCounter(policy.version)
  def rankingCounter(version: Byte): RankingCounter = rankingCounterMap(version)
  def rankingCounter(policy: Counter): RankingCounter = rankingCounter(policy.version)

  def makeHTableName(policy: Counter): String = {
    Seq(tablePrefixMap(policy.version), policy.service, policy.ttl) ++ policy.dailyTtl mkString "_"
  }
}
