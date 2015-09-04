package com.daumkakao.s2graph.core

import com.daumkakao.s2graph.core.mysqls._

//import com.daumkakao.s2graph.core.models._

import com.daumkakao.s2graph.core.types._
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

  def rowKey = VertexRowKey(id)(schemaVer)

  def defaultProps = Map(ColumnMeta.lastModifiedAtColumnSeq.toInt -> InnerVal.withLong(ts, schemaVer))


  def qualifiersWithValues = {
    val base = for ((k, v) <- props ++ defaultProps) yield (VertexQualifier(k)(schemaVer).bytes, v.bytes)
    val belongsTo = belongLabelIds.map { labelId =>
      (VertexQualifier(toPropKey(labelId))(schemaVer).bytes -> Array.empty[Byte])
    }
    base ++ belongsTo
  }


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
    val put = new Put(rowKey.bytes)
    for ((q, v) <- qualifiersWithValues) {
      put.addColumn(vertexCf, q, ts, v)
    }
    List(put)
  }

  def buildPutsAsync(): List[PutRequest] = {
    val qualifiers = ListBuffer[Array[Byte]]()
    val values = ListBuffer[Array[Byte]]()
    for ((q, v) <- qualifiersWithValues) {
      qualifiers += q
      values += v
      //        new PutRequest(hbaseTableName.getBytes, rowKey.bytes, vertexCf, qualifier.bytes, v.bytes, ts)
    }
    val put = new PutRequest(hbaseTableName.getBytes, rowKey.bytes, vertexCf, qualifiers.toArray, values.toArray, ts)
    List(put)
  }

  //  def buildPutsAll(): List[Mutation] = {
  //    op match {
  //      case d: Byte if d == GraphUtil.operations("delete") => // delete
  //        buildDelete()
  //      case _ => // insert/update/increment
  //        buildPuts()
  //    }
  //  }
  def buildPutsAll(): List[HBaseRpc] = {
    op match {
      case d: Byte if d == GraphUtil.operations("delete") => buildDeleteAsync()
      //      case dAll: Byte if dAll == GraphUtil.operations("deleteAll") => buildDeleteAllAsync()
      case _ => buildPutsAsync()
    }
  }

  def buildDelete(): List[Delete] = {
    List(new Delete(rowKey.bytes, ts))
  }

  def buildDeleteAsync(): List[DeleteRequest] = {
    List(new DeleteRequest(hbaseTableName.getBytes, rowKey.bytes, vertexCf, ts))
  }

  def buildGet() = {
    new GetRequest(hbaseTableName.getBytes, rowKey.bytes, vertexCf)
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

  def apply(kvs: Seq[org.hbase.async.KeyValue], version: String): Option[Vertex] = {
    if (kvs.isEmpty) None
    else {

      val head = kvs.head
      val headBytes = head.key()
      val (rowKey, _) = VertexRowKey.fromBytes(headBytes, 0, headBytes.length, version)

      var maxTs = Long.MinValue
      val propsMap = new collection.mutable.HashMap[Int, InnerValLike]
      val belongLabelIds = new ListBuffer[Int]

      /**
       *
       * TODO
       * Make sure this does not violate any MVCC Version.
       */

      for {
        kv <- kvs
        kvQual = kv.qualifier()
        (qualifier, _) = VertexQualifier.fromBytes(kvQual, 0, kvQual.length, version)
      } {
        val ts = kv.timestamp()
        if (ts > maxTs) maxTs = ts

        if (isLabelId(qualifier.propKey)) {
          belongLabelIds += toLabelId(qualifier.propKey)
        } else {
          val v = kv.value()
          val (value, _) = InnerVal.fromBytes(v, 0, v.length, version)
          propsMap += (qualifier.propKey -> value)
        }
      }
      assert(maxTs != Long.MinValue)
      Some(Vertex(rowKey.id, maxTs, propsMap.toMap, belongLabelIds = belongLabelIds))
    }
  }
}
