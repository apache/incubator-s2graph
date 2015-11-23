package s2.counter

import java.text.SimpleDateFormat

import kafka.producer.KeyedMessage
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.Json
import s2.config.{S2ConfigFactory, StreamingConfig}
import s2.counter.core.ExactCounter.ExactValueMap
import s2.counter.core._
import s2.counter.core.v1.ExactStorageHBase
import s2.counter.core.v2.ExactStorageGraph
import s2.models.{Counter, CounterModel, DBModel}
import s2.spark.{SparkApp, WithKafka}

import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 7. 1..
 */
object EraseDailyCounter extends SparkApp with WithKafka {
  import scala.concurrent.ExecutionContext.Implicits.global

  lazy val producer = getProducer[String, String](StreamingConfig.KAFKA_BROKERS)

  def valueToEtlItem(policy: Counter, key: ExactKeyTrait, values: ExactValueMap): Seq[CounterEtlItem] = {
    if (values.nonEmpty) {
      for {
        (eq, value) <- filter(values.toList)
      } yield {
        CounterEtlItem(eq.tq.ts, policy.service, policy.action, key.itemKey, Json.toJson(eq.dimKeyValues), Json.toJson(Map("value" -> -value)))
      }
    } else {
      Nil
    }
  }

  def filter(values: List[(ExactQualifier, Long)]): List[(ExactQualifier, Long)] = {
    val sorted = values.sortBy(_._1.dimKeyValues.size).reverse
    val (eq, value) = sorted.head
    val dimKeys = eq.dimKeyValues.toSeq
    val flat = {
      for {
        i <- 0 to dimKeys.length
        comb <- dimKeys.combinations(i)
      } yield {
        ExactQualifier(eq.tq, comb.toMap) -> value
      }
    }.toMap

//    println("flat >>>", flat)

    val valuesMap = values.toMap
    val remain = (valuesMap ++ flat.map { case (k, v) =>
      k -> (valuesMap(k) - v)
    }).filter(_._2 > 0).toList

//    println("remain >>>", remain)

    if (remain.isEmpty) {
      List((eq, value))
    } else {
      (eq, value) :: filter(remain)
    }
  }

  def produce(policy: Counter, exactRdd: RDD[(ExactKeyTrait, ExactValueMap)]): Unit = {
    exactRdd.mapPartitions { part =>
      for {
        (key, values) <- part
        item <- valueToEtlItem(policy, key, values)
      } yield {
        item
      }
    }.foreachPartition { part =>
      val m = MutableHashMap.empty[Int, mutable.MutableList[CounterEtlItem]]
      part.foreach { item =>
        val k = getPartKey(item.item, 20)
        val values = m.getOrElse(k, mutable.MutableList.empty[CounterEtlItem])
        values += item
        m.update(k, values)
      }
      m.foreach { case (k, v) =>
        v.map(_.toKafkaMessage).grouped(1000).foreach { grouped =>
//          println(grouped)
          producer.send(new KeyedMessage[String, String](StreamingConfig.KAFKA_TOPIC_COUNTER, null, k, grouped.mkString("\n")))
        }
      }
    }
  }

  def rddToExactRdd(policy: Counter, date: String, rdd: RDD[String]): RDD[(ExactKeyTrait, ExactValueMap)] = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val fromTs = dateFormat.parse(date).getTime
    val toTs = fromTs + 23 * 60 * 60 * 1000

    rdd.mapPartitions { part =>
      val exactCounter = policy.version match {
        case VERSION_1 => new ExactCounter(S2ConfigFactory.config, new ExactStorageHBase(S2ConfigFactory.config))
        case VERSION_2 => new ExactCounter(S2ConfigFactory.config, new ExactStorageGraph(S2ConfigFactory.config))
      }

      for {
        line <- part
        FetchedCounts(exactKey, qualifierWithCountMap) <- exactCounter.getCount(policy, line, Array(TimedQualifier.IntervalUnit.DAILY), fromTs, toTs)
      } yield {
        (exactKey, qualifierWithCountMap)
      }
    }
  }

  lazy val className = getClass.getName.stripSuffix("$")

  override def run(): Unit = {
    validateArgument("service", "action", "date", "file", "op")
    DBModel.initialize(S2ConfigFactory.config)

    val (service, action, date, file, op) = (args(0), args(1), args(2), args(3), args(4))
    val conf = sparkConf(s"$className: $service.$action")

    val ctx = new SparkContext(conf)

    val rdd = ctx.textFile(file, 20)

    val counterModel = new CounterModel(S2ConfigFactory.config)

    val policy = counterModel.findByServiceAction(service, action).get
    val exactRdd = rddToExactRdd(policy, date, rdd)
    produce(policy, exactRdd)
  }
}
