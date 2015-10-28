package com.kakao.s2graph.core

import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.types2.{HGKeyValue, VertexGraphStorageDes, VertexGraphStorageSer}

//import com.kakao.s2graph.core.models._

import com.kakao.s2graph.core.types._
import org.apache.hadoop.hbase.client.{Delete, Put}
import org.hbase.async.{DeleteRequest, GetRequest, HBaseRpc, PutRequest}
import play.api.libs.json.Json

import scala.collection.mutable.ListBuffer

/**
  */
case class Vertex(id: VertexId,
                  ts: Long = System.currentTimeMillis(),
                  props: Map[Int, InnerValLike] = Map.empty[Int, InnerValLike],
                  op: Byte = 0,
                  belongLabelIds: Seq[Int] = Seq.empty) extends GraphElement {

  import Graph.vertexCf
  import Vertex._

  val innerId = id.innerId

  def schemaVer = serviceColumn.schemaVersion

  def serviceColumn = ServiceColumn.findById(id.colId)

  def service = Service.findById(serviceColumn.serviceId)

  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, service.hTableName)

  def defaultProps = Map(ColumnMeta.lastModifiedAtColumnSeq.toInt -> InnerVal.withLong(ts, schemaVer))

  lazy val kvs = VertexGraphStorageSer(this).toKeyValues


  /** TODO: make this as configurable */
  override def serviceName = service.serviceName

  override def isAsync = false

  override def queueKey = Seq(ts.toString, serviceName).mkString("|")

  override def queuePartitionKey = id.innerId.toString

  def propsWithName = for {
    (seq, v) <- props
    meta <- ColumnMeta.findByIdAndSeq(id.colId, seq.toByte)
  } yield (meta.name -> v.toString)

  def buildPuts(): List[Put] = {
    //    logger.error(s"put: $this => $rowKey")
//    val put = new Put(rowKey.bytes)
//    for ((q, v) <- qualifiersWithValues) {
//      put.addColumn(vertexCf, q, ts, v)
//    }
//    List(put)
    val kv = kvs.head
    val put = new Put(kv.row)
    kvs.map { kv =>
      put.addColumn(kv.cf, kv.qualifier, kv.timestamp, kv.value)
    }
    List(put)
  }

  def buildPutsAsync(): List[HBaseRpc] = {
    Graph.storageFactory.put(kvs).toList
  }

  def buildPutsAll(): List[HBaseRpc] = {
    op match {
      case d: Byte if d == GraphUtil.operations("delete") => buildDeleteAsync()
      case _ => buildPutsAsync()
    }
  }

  def buildDelete(): List[Delete] = {
    List(new Delete(kvs.head.row, ts))
  }

  def buildDeleteAsync(): List[HBaseRpc] = {
    val kv = kvs.head
    Graph.storageFactory.delete(Seq(kv.copy(_qualifier = null))).toList
  }

  def buildDeleteBelongsToId(): List[HBaseRpc] = {
    val kv = kvs.head
    import org.apache.hadoop.hbase.util.Bytes
    val newKVs = belongLabelIds.map { id => kv.copy(_qualifier = Bytes.toBytes(Vertex.toPropKey(id)) )}
    Graph.storageFactory.delete(newKVs).toList
  }
  def buildGet() = {
    new GetRequest(hbaseTableName.getBytes, kvs.head.row, vertexCf)
  }

  def toEdgeVertex() = Vertex(SourceVertexId(id.colId, innerId), ts, props, op)


  override def hashCode() = {
    val hash = id.hashCode()
//    logger.debug(s"Vertex.hashCode: $this -> $hash")
    hash
  }

  override def equals(obj: Any) = {
    obj match {
      case otherVertex: Vertex =>
        val ret = id == otherVertex.id
//        logger.debug(s"Vertex.equals: $this, $obj => $ret")
        ret
      case _ => false
    }
  }

  def withProps(newProps: Map[Int, InnerValLike]) = Vertex(id, ts, newProps, op)

  def toLogString(): String = {
    val (serviceName, columnName) =
      if (!id.storeColId) ("", "")
      else {
        (serviceColumn.service.serviceName, serviceColumn.columnName)
      }
    val ls = ListBuffer(ts, GraphUtil.fromOp(op), "v", id.innerId, serviceName, columnName)
    if (!propsWithName.isEmpty) ls += Json.toJson(propsWithName)
    ls.mkString("\t")
  }
}

object Vertex {

  def toPropKey(labelId: Int): Int = Byte.MaxValue + labelId

  def toLabelId(propKey: Int): Int = propKey - Byte.MaxValue

  def isLabelId(propKey: Int): Boolean = propKey > Byte.MaxValue

  //  val emptyVertex = Vertex(new CompositeId(CompositeId.defaultColId, CompositeId.defaultInnerId, false, true),
  //    System.currentTimeMillis())
  def fromString(s: String): Option[Vertex] = Graph.toVertex(s)

  def apply(queryParam: QueryParam,
            kvs: Seq[org.hbase.async.KeyValue],
            version: String): Option[Vertex] = {
    if (kvs.isEmpty) None
    else {
      val newKVs = kvs.map(HGKeyValue(_))
      Option(VertexGraphStorageDes.fromKeyValues(queryParam, newKVs, version, None))
    }
  }
}
