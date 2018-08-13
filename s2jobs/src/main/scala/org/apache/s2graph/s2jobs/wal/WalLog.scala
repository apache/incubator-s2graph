package org.apache.s2graph.s2jobs.wal

import org.apache.s2graph.core.JSONParser
import org.apache.s2graph.s2jobs.wal.process.AggregateParam
import org.apache.s2graph.s2jobs.wal.utils.BoundedPriorityQueue
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import play.api.libs.json.{JsObject, Json}

import scala.util.Try

object WalLogAgg {
  val outputColumns = Seq("from", "logs", "maxTs", "minTs")

  def apply(walLog: WalLog): WalLogAgg = {
    new WalLogAgg(walLog.from, Seq(walLog), walLog.timestamp, walLog.timestamp)
  }

  def merge(iter: Iterator[WalLogAgg],
            param: AggregateParam)(implicit ord: Ordering[WalLog]) = {
    val heap = new BoundedPriorityQueue[WalLog](param.heapSize)
    var minTs = Long.MaxValue
    var maxTs = Long.MinValue

    iter.foreach { walLogAgg =>
      minTs = Math.min(walLogAgg.minTs, minTs)
      maxTs = Math.max(walLogAgg.maxTs, maxTs)

      walLogAgg.logs.foreach { walLog =>
        heap += walLog
      }
    }
    val topItems = if (param.sortTopItems) heap.toArray.sortBy(-_.timestamp) else heap.toArray

    WalLogAgg(topItems.head.from, topItems, maxTs, minTs)
  }
}

case class WalLogAgg(from: String,
                     logs: Seq[WalLog],
                     maxTs: Long,
                     minTs: Long)

case class WalLog(timestamp: Long,
                  operation: String,
                  elem: String,
                  from: String,
                  to: String,
                  service: String,
                  label: String,
                  props: String) {
  val id = from
  val columnName = label
  val serviceName = to

  lazy val propsKeyValues = Json.parse(props).as[JsObject].fields.map { case (key, jsValue) =>
    key -> JSONParser.jsValueToString(jsValue)
  }
}

object WalLog {
  val orderByTsAsc = Ordering.by[WalLog, Long](walLog => walLog.timestamp)

  val WalLogSchema = StructType(Seq(
    StructField("timestamp", LongType, false),
    StructField("operation", StringType, false),
    StructField("elem", StringType, false),
    StructField("from", StringType, false),
    StructField("to", StringType, false),
    StructField("service", StringType, true),
    StructField("label", StringType, false),
    StructField("props", StringType, false)
    //    StructField("direction", StringType, true)
  ))

  def fromRow(row: Row): WalLog = {
    val timestamp = row.getAs[Long]("timestamp")
    val operation = Try(row.getAs[String]("operation")).toOption.getOrElse("insert")
    val elem = Try(row.getAs[String]("elem")).toOption.getOrElse("edge")
    val from = row.getAs[String]("from")
    val to = row.getAs[String]("to")
    val service = row.getAs[String]("service")
    val label = row.getAs[String]("label")
    val props = Try(row.getAs[String]("props")).toOption.getOrElse("{}")

    WalLog(timestamp, operation, elem, from, to, service, label, props)
  }
}
