package s2.counter

import com.kakao.s2graph.core.{Graph, GraphUtil}
import org.apache.spark.SparkContext
import s2.config.{S2ConfigFactory, S2CounterConfig, StreamingConfig}
import s2.counter.core.{BlobExactKey, CounterEtlFunctions, CounterFunctions}
import s2.models.Counter.ItemType
import s2.models.{CounterModel, DBModel}
import s2.spark.{HashMapParam, SparkApp, WithKafka}

import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.concurrent.ExecutionContext

/**
 * Created by rain on 7/1/15.
 */
object CounterBulkLoader extends SparkApp with WithKafka {
  lazy val config = S2ConfigFactory.config
  lazy val s2Config = new S2CounterConfig(config)
  lazy val counterModel = new CounterModel(config)
  lazy val className = getClass.getName.stripSuffix("$")
  lazy val producer = getProducer[String, String](StreamingConfig.KAFKA_BROKERS)

  implicit val graphEx = ExecutionContext.Implicits.global

  val initialize = {
    println("initialize")
//    Graph(config)
    DBModel.initialize(config)
    true
  }

  override def run(): Unit = {
    val hdfsPath = args(0)
    val blockSize = args(1).toInt
    val minPartitions = args(2).toInt
    val conf = sparkConf(s"$hdfsPath: CounterBulkLoader")

    val sc = new SparkContext(conf)
    val acc = sc.accumulable(MutableHashMap.empty[String, Long], "Throughput")(HashMapParam[String, Long](_ + _))

    val msgs = sc.textFile(hdfsPath)

    val etlRdd = msgs.repartition(minPartitions).mapPartitions { part =>
      // parse and etl
      assert(initialize)
      val items = {
        for {
          msg <- part
          line <- GraphUtil.parseString(msg)
          sp = GraphUtil.split(line) if sp.size <= 7 || GraphUtil.split(line)(7) != "in"
          item <- CounterEtlFunctions.parseEdgeFormat(line)
        } yield {
          acc +=("Edges", 1)
          item
        }
      }
      items.grouped(blockSize).flatMap { grouped =>
        grouped.groupBy(e => (e.service, e.action)).flatMap { case ((service, action), v) =>
          CounterEtlFunctions.checkPolicyAndMergeDimension(service, action, v.toList)
        }
      }
    }

    val exactRdd = CounterFunctions.exactCountFromEtl(etlRdd, etlRdd.partitions.length)
    val logRdd = exactRdd.mapPartitions { part =>
      val seq = part.toSeq
      CounterFunctions.insertBlobValue(seq.map(_._1).filter(_.itemType == ItemType.BLOB).map(_.asInstanceOf[BlobExactKey]), acc)
      // update exact counter
      CounterFunctions.updateExactCounter(seq, acc).toIterator
    }

    val rankRdd = CounterFunctions.makeRankingRddFromTrxLog(logRdd, logRdd.partitions.length)
    rankRdd.foreachPartition { part =>
      CounterFunctions.updateRankingCounter(part, acc)
    }
  }
}