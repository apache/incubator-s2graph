package s2.counter

import java.text.SimpleDateFormat

import kafka.producer.KeyedMessage
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import play.api.libs.json.Json
import s2.config.{S2ConfigFactory, StreamingConfig}
import s2.counter.core.ExactCounter.ExactValueMap
import s2.counter.core._
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

//  def valueToEtlItem2(policy: Counter, key: ExactKeyTrait, values: ExactValue): Seq[CounterETLItem] = {
//    val sorted = values.toSeq.sortBy(_._1.dimKeyValues.size).reverse
//    val (eq, value) = sorted.head
//    val dimKeys = eq.dimKeyValues.toSeq
//    val reduced = {
//      for {
//        i <- dimKeys.indices
//        comb <- dimKeys.combinations(i)
//      } yield {
//        ExactQualifier(eq.tq, comb.toMap) -> value
//      }
//    }.toMap
//    sorted.map { case (eq, value) =>
//      eq -> (value - reduced(eq))
//    }
//  }

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
      CounterFunctions.exactCounterByVersion(policy.version) match {
        case Some(exactCounter) =>
          for {
            line <- part
            FetchedCountsGrouped(exactKey, intervalWithCountMap) <- exactCounter.getCount(policy, line, Array(TimedQualifier.IntervalUnit.DAILY), fromTs, toTs, Map.empty[String, Set[String]])
            count = intervalWithCountMap.values.head
          } yield {
            (exactKey, count)
          }
        case None =>
          throw new Exception(s"unknown version: ${policy.version}")
      }
    }
  }

  lazy val className = getClass.getName.stripSuffix("$")

  // 상속받은 클래스에서 구현해줘야 하는 함수
  override def run(): Unit = {
    validateArgument("service", "action", "date", "file", "op")
    DBModel.initialize(S2ConfigFactory.config)

    val (service, action, date, file, op) = (args(0), args(1), args(2), args(3), args(4))
    val conf = sparkConf(s"$className: $service.$action")

    val ctx = new SparkContext(conf)

    val rdd = ctx.textFile(file)

    val counterModel = new CounterModel(S2ConfigFactory.config)

    val policy = counterModel.findByServiceAction(service, action).get
    val exactRdd = rddToExactRdd(policy, date, rdd)
    produce(policy, exactRdd)
  }
}
