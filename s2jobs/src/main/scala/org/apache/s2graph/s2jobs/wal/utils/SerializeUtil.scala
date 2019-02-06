package org.apache.s2graph.s2jobs.wal.utils

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.core.storage.serde.Serializable
import org.apache.s2graph.core.types._
import org.apache.s2graph.s2jobs.wal.{MemorySchemaManager, WalLog, WalVertex}
import play.api.libs.json.{JsObject, Json}

object SerializeUtil {

  import org.apache.s2graph.core.storage.serde.StorageSerializable._

  private def insertBulkForLoaderAsync(s2: S2Graph, edge: S2Edge, createRelEdges: Boolean = true): Seq[SKeyValue] = {
    val relEdges = if (createRelEdges) edge.relatedEdges else List(edge)

    val snapshotEdgeKeyValues = s2.getStorage(edge.toSnapshotEdge.label).serDe.snapshotEdgeSerializer(edge.toSnapshotEdge).toKeyValues
    val indexEdgeKeyValues = relEdges.flatMap { edge =>
      edge.edgesWithIndex.flatMap { indexEdge =>
        s2.getStorage(indexEdge.label).serDe.indexEdgeSerializer(indexEdge).toKeyValues
      }
    }

    snapshotEdgeKeyValues ++ indexEdgeKeyValues
  }

  def buildEdgeIndexInnerProps(walLog: WalLog,
                               innerProps: Map[LabelMeta, InnerValLike],
                               defaultProps: Map[LabelMeta, InnerValLike],
                               label: Label,
                               labelMetasInOrder: Array[LabelMeta]): Array[(LabelMeta, InnerValLike)] = {
    labelMetasInOrder.map { labelMeta =>
      innerProps.get(labelMeta) match {
        case None =>
          val defaultVal = JSONParser.toInnerVal(labelMeta.defaultValue, labelMeta.dataType, label.schemaVersion)
          val innerVal = defaultProps.getOrElse(labelMeta, defaultVal)

          labelMeta -> innerVal
        case Some(innerVal) => labelMeta -> innerVal
      }
    }
  }

  def buildEdgeInnerDefaultProps(walLog: WalLog,
                                 label: Label,
                                 srcColumn: ServiceColumn,
                                 tgtColumn: ServiceColumn): Map[LabelMeta, InnerValLike] = {
    Map(
      LabelMeta.timestamp -> InnerVal.withLong(walLog.timestamp, label.schemaVersion),
      LabelMeta.from -> JSONParser.toInnerVal(walLog.from, srcColumn.columnType, srcColumn.schemaVersion),
      LabelMeta.to -> JSONParser.toInnerVal(walLog.to, tgtColumn.columnType, tgtColumn.schemaVersion)
    )
  }

  def buildEdgeInnerProps(walLog: WalLog,
                          label: Label,
                          schema: SchemaManager): Map[LabelMeta, InnerValLike] = {
    val ts = walLog.timestamp
    val schemaVer = label.schemaVersion
    val tsInnerVal = InnerVal.withLong(ts, schemaVer)

    val labelMetas = schema.findLabelMetas(walLog.label)
    val props = walLog.propsJson.fields.flatMap { case (key, jsValue) =>
      labelMetas.get(key).flatMap { labelMeta =>
        JSONParser.jsValueToInnerVal(jsValue, labelMeta.dataType, schemaVer).map { innerVal =>
          labelMeta -> innerVal
        }
      }
    }.toMap

    props ++ Map(LabelMeta.timestamp -> tsInnerVal)
  }

  def buildVertexInnerProps(walVertex: WalVertex,
                            column: ServiceColumn,
                            schema: SchemaManager): Map[ColumnMeta, InnerValLike] = {
    val ts = walVertex.timestamp
    val schemaVer = column.schemaVersion
    val tsInnerVal = InnerVal.withLong(ts, schemaVer)

    val columnMetas = schema.findColumnMetas(column)
    val props = Json.parse(walVertex.props).as[JsObject].fields.flatMap { case (key, jsValue) =>
        columnMetas.get(key).flatMap { columnMeta =>
          JSONParser.jsValueToInnerVal(jsValue, columnMeta.dataType, schemaVer).map { innerVal =>
            columnMeta -> innerVal
          }
        }
    }.toMap

    props ++ Map(ColumnMeta.timestamp -> tsInnerVal, ColumnMeta.lastModifiedAtColumn -> tsInnerVal)
  }

  case class PartialResult(label: Label,
                           srcColumn: ServiceColumn,
                           srcVertexId: VertexId,
                           tgtColumn: ServiceColumn,
                           tgtVertexId: VertexId,
                           op: Byte,
                           dir: Int,
                           props: Map[LabelMeta, InnerValLike],
                           defaultProps: Map[LabelMeta, InnerValLike]) {
    val schemaVer = label.schemaVersion

    // need to build Array[Byte] for SKeyValue
    lazy val table = label.hTableName.getBytes("UTF-8")
    lazy val cf = Serializable.edgeCf
    lazy val labelWithDir = LabelWithDirection(label.id.get, dir)
    lazy val labelWithDirBytes = labelWithDir.bytes
    lazy val srcIdBytes = VertexId.toSourceVertexId(srcVertexId).bytes
    lazy val tgtIdBytes = VertexId.toTargetVertexId(tgtVertexId).bytes
  }

  def buildPartialResult(walLog: WalLog,
                         schema: SchemaManager): PartialResult = {
    val label = schema.findLabel(walLog.label)
    val dir = 0

    val srcColumn = schema.findSrcServiceColumn(label)
    val srcInnerVal = JSONParser.toInnerVal(walLog.from, srcColumn.columnType, srcColumn.schemaVersion)
    val srcVertexId = VertexId(srcColumn, srcInnerVal)

    val tgtColumn = schema.findTgtServiceColumn(label)
    val tgtInnerVal = JSONParser.toInnerVal(walLog.to, tgtColumn.columnType, tgtColumn.schemaVersion)
    val tgtVertexId = VertexId(tgtColumn, tgtInnerVal)

    val allProps = buildEdgeInnerProps(walLog, label, schema)
    val defaultProps = buildEdgeInnerDefaultProps(walLog, label, srcColumn, tgtColumn)

    val op = GraphUtil.toOp(walLog.operation).getOrElse(throw new IllegalArgumentException(s"${walLog.operation} operation is not supported."))

    PartialResult(label, srcColumn, srcVertexId, tgtColumn, tgtVertexId, op, dir, allProps, defaultProps)
  }

  def walToIndexEdgeKeyValue(walLog: WalLog,
                             schema: SchemaManager,
                             tallSchemaVersions: Set[String]): Iterable[SKeyValue] = {
    //TODO: Degree Edge is not considered here.
    val pr = buildPartialResult(walLog, schema)
    val label = pr.label
    val srcIdBytes = pr.srcIdBytes
    val tgtIdBytes = pr.tgtIdBytes
    val labelWithDirBytes = pr.labelWithDirBytes
    val allProps = pr.props
    val defaultProps = pr.defaultProps
    val op = pr.op
    val table = pr.table
    val cf = pr.cf
    val isTallSchema = tallSchemaVersions(label.schemaVersion)

    schema.findLabelIndices(label).map { case (indexName, index) =>
      val labelMetasInOrder = schema.findLabelIndexLabelMetas(label.label, indexName)

      val orderedProps = buildEdgeIndexInnerProps(walLog, allProps, defaultProps, label, labelMetasInOrder)
      val idxPropsMap = orderedProps.toMap
      val idxPropsBytes = propsToBytes(orderedProps)
      val metaProps = allProps.filterKeys(m => !idxPropsMap.contains(m))

      val row =
        if (isTallSchema) {
          val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(index.seq, isInverted = false)

          val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)

          val qualifier = idxPropsMap.get(LabelMeta.to) match {
            case None => Bytes.add(idxPropsBytes, tgtIdBytes)
            case Some(_) => idxPropsBytes
          }

          val opByte =
            if (op == GraphUtil.operations("incrementCount")) op
            else GraphUtil.defaultOpByte

          Bytes.add(row, qualifier, Array.fill(1)(opByte))
        } else {
          val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(index.seq, isInverted = false)
          Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
        }

      val qualifier =
        if (isTallSchema) Array.empty[Byte]
        else {
          if (op == GraphUtil.operations("incrementCount")) {
            Bytes.add(idxPropsBytes, tgtIdBytes, Array.fill(1)(op))
          } else {
            idxPropsMap.get(LabelMeta.to) match {
              case None => Bytes.add(idxPropsBytes, tgtIdBytes)
              case Some(_) => idxPropsBytes
            }
          }
        }

      val value = {
        if (op == GraphUtil.operations("incrementCount")) {
          longToBytes(allProps(LabelMeta.count).toString().toLong)
        } else {
          propsToKeyValues(metaProps.toSeq)
        }
      }

      SKeyValue(table, row, cf, qualifier, value, walLog.timestamp, op, durability = true)
    }
  }

  def walToSnapshotEdgeKeyValue(walLog: WalLog,
                                schema: SchemaManager,
                                tallSchemaVersions: Set[String]): Iterable[SKeyValue] = {
    val ts = walLog.timestamp
    val pr = buildPartialResult(walLog, schema)
    val label = pr.label

    val labelWithDirBytes = pr.labelWithDirBytes
    val allProps = pr.props.map { case (labelMeta, innerVal) =>
      labelMeta.seq -> InnerValLikeWithTs(innerVal, ts)
    }

    val op = pr.op
    val table = pr.table
    val cf = pr.cf
    val statusCode = 0.toByte

    val isTallSchema = tallSchemaVersions(label.schemaVersion)

    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(LabelIndex.DefaultSeq, isInverted = true)
    val rowFirstBytes =
      if (isTallSchema) SourceAndTargetVertexIdPair(pr.srcVertexId.innerId, pr.tgtVertexId.innerId).bytes
      else VertexId.toSourceVertexId(pr.srcVertexId).bytes

    val row = Bytes.add(rowFirstBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)

    val qualifier =
      if (isTallSchema) Array.empty[Byte]
      else pr.tgtIdBytes

    val metaPropsBytes = HBaseSerializable.propsToKeyValuesWithTs(allProps.toSeq)

    def statusCodeWithOp(statusCode: Byte, op: Byte): Array[Byte] = {
      val byte = (((statusCode << 4) | op).toByte)
      Array.fill(1)(byte.toByte)
    }

    val value = Bytes.add(statusCodeWithOp(statusCode, op), metaPropsBytes)

    Seq(
      SKeyValue(table, row, cf, qualifier, value, ts, op, durability = true)
    )
  }

  def walToSKeyValues(walLog: WalLog,
                      schema: SchemaManager,
                      tallSchemaVersions: Set[String]): Iterable[SKeyValue] = {
    walToIndexEdgeKeyValue(walLog, schema, tallSchemaVersions) ++
      walToSnapshotEdgeKeyValue(walLog, schema, tallSchemaVersions)
  }

  def walVertexToSKeyValue(v: WalVertex,
                           schema: SchemaManager,
                           tallSchemaVersions: Set[String]): Iterable[SKeyValue] = {
    val column = schema.findServiceColumn(v.service, v.column)
    val service = schema.findColumnService(column.id.get)
    val schemaVer = column.schemaVersion

    val innerVal = JSONParser.toInnerVal(v.id, column.columnType, column.schemaVersion)
    val vertexId = VertexId(column, innerVal)
    val row = vertexId.bytes

    val props = buildVertexInnerProps(v, column, schema)
    //TODO: skip belongLabelIds

    val table = service.hTableName.getBytes("UTF-8")
    val cf = Serializable.vertexCf
    val isTallSchema = tallSchemaVersions(schemaVer)

    props.map { case (columnMeta, innerVal) =>
      val rowBytes =
        if (isTallSchema) Bytes.add(row, Array.fill(1)(columnMeta.seq))
        else row

      val qualifier =
        if (isTallSchema) Array.empty[Byte]
        else intToBytes(columnMeta.seq)

      val value = innerVal.bytes

      SKeyValue(table, rowBytes, cf, qualifier, value, v.timestamp)
    }
  }

  // main public interface. 
  def toSKeyValues(walLog: WalLog,
                   schema: SchemaManager,
                   tallSchemaVersions: Set[String]): Iterable[SKeyValue] = {
    walLog.elem match {
      case "vertex" | "v" => walVertexToSKeyValue(WalVertex.fromWalLog(walLog), schema, tallSchemaVersions)
      case "edge" | "e" => walToSKeyValues(walLog, schema, tallSchemaVersions)
      case _ =>  throw new IllegalArgumentException(s"$walLog ${walLog.elem} is not supported.")
    }
  }

  def toSKeyValues(s2: S2Graph, element: GraphElement, autoEdgeCreate: Boolean = false): Seq[SKeyValue] = {
    if (element.isInstanceOf[S2Edge]) {
      val edge = element.asInstanceOf[S2Edge]
      insertBulkForLoaderAsync(s2, edge, autoEdgeCreate)
    } else if (element.isInstanceOf[S2Vertex]) {
      val vertex = element.asInstanceOf[S2Vertex]
      s2.getStorage(vertex.service).serDe.vertexSerializer(vertex).toKeyValues
    } else {
      Nil
    }
  }

  def sKeyValueToKeyValue(skv: SKeyValue): KeyValue = {
    new KeyValue(skv.row, skv.cf, skv.qualifier, skv.timestamp, Type.Put, skv.value)
  }
}
