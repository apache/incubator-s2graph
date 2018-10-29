package org.apache.s2graph.s2jobs.wal

import com.google.common.hash.Hashing
import org.apache.s2graph.core.{GraphUtil, JSONParser}
import org.apache.s2graph.s2jobs.wal.process.params.AggregateParam
import org.apache.s2graph.s2jobs.wal.transformer.Transformer
import org.apache.s2graph.s2jobs.wal.utils.BoundedPriorityQueue
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import play.api.libs.json.{JsObject, Json}

import scala.util.Try

object WalLogAgg {
  val outputColumns = Seq("from", "vertices", "edges")

  def isEdge(walLog: WalLog): Boolean = {
    walLog.elem == "edge" || walLog.elem == "e"
  }

  def apply(walLog: WalLog): WalLogAgg = {
    val (vertices, edges) =
      if (isEdge(walLog)) (Nil, Seq(walLog))
      else (Seq(walLog), Nil)

    new WalLogAgg(walLog.from, vertices, edges)
  }

  def toFeatureHash(dimVal: DimVal): Long = toFeatureHash(dimVal.dim, dimVal.value)

  def toFeatureHash(dim: String, value: String): Long = {
    Hashing.murmur3_128().hashBytes(s"$dim:$value".getBytes("UTF-8")).asLong()
  }

  private def addToHeap(walLog: WalLog,
                        heap: BoundedPriorityQueue[WalLog],
                        now: Long,
                        validTimestampDuration: Option[Long]): Unit = {
    val ts = walLog.timestamp
    val isValid = validTimestampDuration.map(d => now - ts < d).getOrElse(true)

    if (isValid) {
      heap += walLog
    }
  }

  private def addToHeap(iter: Seq[WalLog],
                        heap: BoundedPriorityQueue[WalLog],
                        now: Long,
                        validTimestampDuration: Option[Long]): Unit = {
    iter.foreach(walLog => addToHeap(walLog, heap, now, validTimestampDuration))
  }

  private def toWalLogAgg(edgeHeap: BoundedPriorityQueue[WalLog],
                          vertexHeap: BoundedPriorityQueue[WalLog],
                          sortTopItems: Boolean): Option[WalLogAgg] = {
    val topVertices = if (sortTopItems) vertexHeap.toArray.sortBy(-_.timestamp) else vertexHeap.toArray
    val topEdges = if (sortTopItems) edgeHeap.toArray.sortBy(-_.timestamp) else edgeHeap.toArray

    topEdges.headOption.map(head => WalLogAgg(head.from, topVertices, topEdges))
  }

  def mergeWalLogs(iter: Iterator[WalLog],
                   heapSize: Int,
                   now: Long,
                   validTimestampDuration: Option[Long],
                   sortTopItems: Boolean)(implicit ord: Ordering[WalLog]): Option[WalLogAgg] = {
    val edgeHeap = new BoundedPriorityQueue[WalLog](heapSize)
    val vertexHeap = new BoundedPriorityQueue[WalLog](heapSize)

    iter.foreach { walLog =>
      if (walLog.isVertex) addToHeap(walLog, vertexHeap, now, validTimestampDuration)
      else addToHeap(walLog, edgeHeap, now, validTimestampDuration)
    }

    toWalLogAgg(edgeHeap, vertexHeap, sortTopItems)
  }

  def merge(iter: Iterator[WalLogAgg],
            heapSize: Int,
            now: Long,
            validTimestampDuration: Option[Long],
            sortTopItems: Boolean)(implicit ord: Ordering[WalLog]): Option[WalLogAgg] = {
    val edgeHeap = new BoundedPriorityQueue[WalLog](heapSize)
    val vertexHeap = new BoundedPriorityQueue[WalLog](heapSize)

    iter.foreach { walLogAgg =>
      addToHeap(walLogAgg.vertices, vertexHeap, now, validTimestampDuration)
      addToHeap(walLogAgg.edges, edgeHeap, now, validTimestampDuration)
    }

    toWalLogAgg(edgeHeap, vertexHeap, sortTopItems)
  }

  def mergeWalLogs(iter: Iterator[WalLog],
                   param: AggregateParam)(implicit ord: Ordering[WalLog]): Option[WalLogAgg] = {
    mergeWalLogs(iter, param.heapSize, param.now, param.validTimestampDuration, param.sortTopItems)
  }

  def merge(iter: Iterator[WalLogAgg],
            param: AggregateParam)(implicit ord: Ordering[WalLog]): Option[WalLogAgg] = {
    merge(iter, param.heapSize, param.now, param.validTimestampDuration, param.sortTopItems)
  }


  private def filterPropsInner(walLogs: Seq[WalLog],
                          transformers: Seq[Transformer],
                          validFeatureHashKeys: Set[Long]): Seq[WalLog] = {
    walLogs.map { walLog =>
      val fields = walLog.propsJson.fields.filter { case (propKey, propValue) =>
        val filtered = transformers.flatMap { transformer =>
          transformer.toDimValLs(walLog, propKey, JSONParser.jsValueToString(propValue)).filter(dimVal => validFeatureHashKeys(toFeatureHash(dimVal)))
        }
        filtered.nonEmpty
      }

      walLog.copy(props = Json.toJson(fields.toMap).as[JsObject].toString)
    }
  }

  def filterProps(walLogAgg: WalLogAgg,
                        transformers: Seq[Transformer],
                        validFeatureHashKeys: Set[Long]) = {
    val filteredVertices = filterPropsInner(walLogAgg.vertices, transformers, validFeatureHashKeys)
    val filteredEdges = filterPropsInner(walLogAgg.edges, transformers, validFeatureHashKeys)

    walLogAgg.copy(vertices = filteredVertices, edges = filteredEdges)
  }
}

object DimValCountRank {
  def fromRow(row: Row): DimValCountRank = {
    val dim = row.getAs[String]("dim")
    val value = row.getAs[String]("value")
    val count = row.getAs[Long]("count")
    val rank = row.getAs[Long]("rank")

    new DimValCountRank(DimVal(dim, value), count, rank)
  }
}

case class DimValCountRank(dimVal: DimVal, count: Long, rank: Long)

case class DimValCount(dimVal: DimVal, count: Long)

object DimVal {
  def fromRow(row: Row): DimVal = {
    val dim = row.getAs[String]("dim")
    val value = row.getAs[String]("value")

    new DimVal(dim, value)
  }
}

case class DimVal(dim: String, value: String)

case class WalLogAgg(from: String,
                     vertices: Seq[WalLog],
                     edges: Seq[WalLog])

case class WalLog(timestamp: Long,
                  operation: String,
                  elem: String,
                  from: String,
                  to: String,
                  service: String,
                  label: String,
                  props: String) {
  val isVertex = elem == "v" || elem == "vertex"
  val id = from
  val columnName = label
  val serviceName = to
  lazy val propsJson = Json.parse(props).as[JsObject]
  lazy val propsKeyValues = propsJson.fields.map { case (key, jsValue) =>
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
