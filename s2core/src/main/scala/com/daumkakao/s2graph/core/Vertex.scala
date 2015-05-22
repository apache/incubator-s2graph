package com.daumkakao.s2graph.core


import HBaseElement._
import com.daumkakao.s2graph.core.models.{HColumnMeta, HServiceColumn, HService}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Delete
import org.apache.hadoop.hbase.client.Mutation
import play.api.libs.json.Json
import scala.collection.mutable.ListBuffer
import org.hbase.async.{DeleteRequest, HBaseRpc, PutRequest, GetRequest}
/**
 *
 */
case class Vertex(id: CompositeId,
  ts: Long,
  props: Map[Byte, InnerVal] = Map.empty[Byte, InnerVal], op: Byte = 0) extends GraphElement {

  import GraphConstant._
  //  import Vertex.{ lastModifiedAtColumn, deletedAtColumn }
  lazy val serviceColumn = HServiceColumn.findById(id.colId)
  lazy val service = HService.findById(serviceColumn.serviceId)
  lazy val (hbaseZkAddr, hbaseTableName) = (service.cluster, service.hTableName)

  lazy val rowKey = VertexRowKey(id)
  //  lazy val defaultProps = Map(defaultColumn -> (DateTime.now().getMillis / 1000).toInt)
  lazy val defaultProps = Map(HColumnMeta.lastModifiedAtColumnSeq -> InnerVal.withLong(ts))
  lazy val qualifiersWithValues = for ((k, v) <- props ++ defaultProps) yield (VertexQualifier(k), v)
  lazy val innerId = id.innerId

  /** TODO: make this as configurable */
  override lazy val serviceName = service.serviceName
  override lazy val isAsync = false
  override lazy val queueKey = Seq(ts.toString, serviceName).mkString("|")
  override lazy val queuePartitionKey = id.innerId.toString

  lazy val propsWithName = for {
    (seq, v) <- props
    meta <- HColumnMeta.findByIdAndSeq(id.colId, seq)
  } yield (meta.name -> v.toString)

  //  lazy val propsWithName = for {
  //    (seq, v) <- props
  //    meta <- ColumnMeta.findByIdAndSeq(id.colId, seq)
  //  } yield (meta.name -> v.toString)

  def buildPuts(): List[Put] = {
    //    play.api.Logger.error(s"put: $this => $rowKey")
    val puts =
      for ((q, v) <- qualifiersWithValues) yield {
        val put = new Put(rowKey.bytes)
        //        play.api.Logger.debug(s"${rowKey.bytes.toList}")
        /**
         * TODO
         * now user need to update one by one(can`t update multiple key values).
         * if user issue update on vertex with multiple key values then they all have same timestamp version.
         */
        // all props have same timestamp version in hbase.
        // This
        //        play.api.Logger.debug(s"VertexBuildPuts: $rowKey, $q")
        put.addColumn(vertexCf, q.bytes, ts, v.bytes)
      }
    puts.toList
  }
  def buildPutsAsync(): List[PutRequest] = {
    val puts =
      for ((q, v) <- qualifiersWithValues) yield {
        new PutRequest(hbaseTableName.getBytes, rowKey.bytes, vertexCf, q.bytes, v.bytes, ts)
      }
    puts.toList
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
      case d: Byte if d == GraphUtil.operations("delete") =>  buildDeleteAsync()
      case _ => buildPutsAsync()
    }
  }
  def buildDelete(): List[Delete] = {
    List(new Delete(rowKey.bytes, ts))
  }
  def buildDeleteAsync(): List[DeleteRequest] = {
    List(new DeleteRequest(hbaseTableName.getBytes, rowKey.bytes, vertexCf, ts))
  }
  //  def buildGet() = {
  //    val get = new Get(rowKey.bytes)
  //    //    play.api.Logger.error(s"get: $this => $rowKey")
  //    get.addFamily(vertexCf)
  //    get
  //  }
  def buildGet() = {
    new GetRequest(hbaseTableName.getBytes, rowKey.bytes, vertexCf)
  }
  def toEdgeVertex() = Vertex(id.updateIsEdge(true), ts, props)
  override def toString(): String = {

    val (serviceName, columnName) = if (id.isEdge) ("", "") else {
      val serviceColumn = HServiceColumn.findById(id.colId)
      (serviceColumn.service.serviceName, serviceColumn.columnName)
    }
    val ls = ListBuffer(ts, GraphUtil.fromOp(op), "v", innerId, serviceName, columnName)
    if (!propsWithName.isEmpty) ls += Json.toJson(propsWithName)
    ls.mkString("\t")
  }
  override def hashCode() = {
    id.hashCode()
  }
  override def equals(obj: Any) = {
    obj match {
      case otherVertex: Vertex =>
        id.equals(otherVertex.id)
      case _ => false
    }
  }
  def withProps(newProps: Map[Byte, InnerVal]) = Vertex(id, ts, newProps, op)
}

object Vertex {

  val emptyVertex = Vertex(CompositeId.emptyCompositeId, System.currentTimeMillis())
  def fromString(s: String): Option[Vertex] = Graph.toVertex(s)

  def apply(kvs: Seq[org.hbase.async.KeyValue]): Option[Vertex] = {
    if (kvs.isEmpty) None
    else {

      val head = kvs.head
      val headBytes = head.key()
      val rowKey = VertexRowKey(headBytes, 0)

      var maxTs = Long.MinValue
      /**
       *
       * TODO
       * Make sure this doens`t violate any MVCC Version.
       */
      val props =
        for {
          kv <- kvs
          kvQual = kv.qualifier()
          qualifier = VertexQualifier(kvQual, 0, kvQual.length)
          value = InnerVal(kv.value(), 0)
          ts = kv.timestamp()
        } yield {
          if (ts > maxTs) maxTs = ts
          (qualifier.propKey, value)
        }
      assert(maxTs != Long.MinValue)
      Some(Vertex(rowKey.id, maxTs, props.toMap))
    }
  }
}