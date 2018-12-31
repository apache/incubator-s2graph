package org.apache.s2graph.s2jobs.wal.utils

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.s2graph.core.schema.{ColumnMeta, LabelMeta, ServiceColumn}
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.core.storage.serde.StorageDeserializable
import org.apache.s2graph.core.storage.serde.StorageDeserializable.bytesToInt
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.{GraphUtil, JSONParser, S2Vertex}
import org.apache.s2graph.s2jobs.wal._
import org.apache.spark.sql.Row
import play.api.libs.json.Json

import scala.util.{Failure, Success, Try}

object DeserializeUtil {

  private def statusCodeWithOp(byte: Byte): (Byte, Byte) = {
    val statusCode = byte >> 4
    val op = byte & ((1 << 4) - 1)
    (statusCode.toByte, op.toByte)
  }

  private def bytesToLabelIndexSeqWithIsInverted(bytes: Array[Byte], offset: Int): (Byte, Boolean) = {
    val byte = bytes(offset)
    val isInverted = if ((byte & 1) != 0) true else false
    val labelOrderSeq = byte >> 1
    (labelOrderSeq.toByte, isInverted)
  }

  private def bytesToKeyValues(bytes: Array[Byte],
                               offset: Int,
                               length: Int,
                               schemaVer: String,
                               labelMetaMap: Map[Byte, LabelMeta]): (Array[(LabelMeta, InnerValLike)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(LabelMeta, InnerValLike)](len)
    var i = 0
    while (i < len) {
      val k = labelMetaMap(bytes(pos))
      pos += 1
      val (v, numOfBytesUsed) = InnerVal.fromBytes(bytes, pos, 0, schemaVer)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    val ret = (kvs, pos)
    //    logger.debug(s"bytesToProps: $ret")
    ret
  }

  private def bytesToKeyValuesWithTs(bytes: Array[Byte],
                                     offset: Int,
                                     schemaVer: String,
                                     labelMetaMap: Map[Byte, LabelMeta]): (Array[(LabelMeta, InnerValLikeWithTs)], Int) = {
    var pos = offset
    val len = bytes(pos)
    pos += 1
    val kvs = new Array[(LabelMeta, InnerValLikeWithTs)](len)
    var i = 0
    while (i < len) {
      val k = labelMetaMap(bytes(pos))
      pos += 1
      val (v, numOfBytesUsed) = InnerValLikeWithTs.fromBytes(bytes, pos, 0, schemaVer)
      pos += numOfBytesUsed
      kvs(i) = (k -> v)
      i += 1
    }
    val ret = (kvs, pos)
    //    logger.debug(s"bytesToProps: $ret")
    ret
  }

  def sKeyValueFromRow(row: Row): SKeyValue = {
    val table = row.getAs[Array[Byte]]("table")
    val _row = row.getAs[Array[Byte]]("row")
    val cf = row.getAs[Array[Byte]]("cf")
    val qualifier = row.getAs[Array[Byte]]("qualifier")
    val value = row.getAs[Array[Byte]]("value")
    val timestamp = row.getAs[Long]("timestamp")
    val operation = row.getAs[Int]("operation")
    val durability = row.getAs[Boolean]("durability")

    SKeyValue(table, _row, cf, qualifier, value, timestamp, operation, durability)
  }

  def cellToSKeyValue(cell: Cell): SKeyValue = {
    new SKeyValue(Array.empty[Byte], cell.getRow, cell.getFamily, cell.getQualifier,
      cell.getValue, cell.getTimestamp, SKeyValue.Default)
  }

  def sKeyValueToCell(skv: SKeyValue): Cell = {
    CellUtil.createCell(skv.row, skv.cf, skv.qualifier, skv.timestamp, 4.toByte, skv.value)
  }

  case class RowV3Parsed(srcVertexId: VertexId,
                         labelWithDir: LabelWithDirection,
                         labelIdxSeq: Byte,
                         isInverted: Boolean,
                         pos: Int)

  case class QualifierV3Parsed(idxPropsArray: IndexedSeq[(LabelMeta, InnerValLike)],
                               tgtVertexId: VertexId,
                               op: Byte)

  case class SnapshotRowV3Parsed(srcVertexId: VertexId,
                                 tgtVertexId: VertexId,
                                 labelWithDir: LabelWithDirection,
                                 labelIdxSeq: Byte,
                                 isInverted: Boolean)

  def toRowV3Parsed(row: Array[Byte]): RowV3Parsed = {
    var pos = 0
    val (srcVertexId, srcIdLen) = SourceVertexId.fromBytes(row, pos, row.length, HBaseType.DEFAULT_VERSION)
    pos += srcIdLen
    val labelWithDir = LabelWithDirection(Bytes.toInt(row, pos, 4))
    pos += 4
    val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(row, pos)
    pos += 1

    RowV3Parsed(srcVertexId, labelWithDir, labelIdxSeq, isInverted, pos)
  }

  def toQualifierV4Parsed(row: Array[Byte],
                          offset: Int,
                          schemaVer: String,
                          labelId: Int,
                          labelIdxSeq: Byte,
                          schema: SchemaManager): Try[QualifierV3Parsed] = Try {
    val (idxPropsRaw, endAt) = StorageDeserializable.bytesToProps(row, offset, schemaVer)
    val pos = endAt

    val (tgtVertexIdRaw, tgtVertexIdLen) =
      if (endAt == row.length - 1) (HBaseType.defaultTgtVertexId, 0)
      else TargetVertexId.fromBytes(row, endAt, row.length - 1, schemaVer)

    val op = row.last

    val idxPropsArray = toIndexProps(idxPropsRaw, labelId, labelIdxSeq, schema)

    QualifierV3Parsed(idxPropsArray, tgtVertexIdRaw, op)
  }

  def toQualifierV3Parsed(qualifier: Array[Byte],
                          schemaVer: String,
                          labelId: Int,
                          labelIdxSeq: Byte,
                          schema: SchemaManager): Try[QualifierV3Parsed] = Try {
    val (idxPropsRaw, endAt) =
      StorageDeserializable.bytesToProps(qualifier, 0, schemaVer)

    var pos = endAt

    val (tgtVertexIdRaw, tgtVertexIdLen) =
      if (endAt == qualifier.length) (HBaseType.defaultTgtVertexId, 0)
      else TargetVertexId.fromBytes(qualifier, endAt, qualifier.length, schemaVer)

    pos += tgtVertexIdLen

    val op =
      if (qualifier.length == pos) GraphUtil.defaultOpByte
      else qualifier.last

    val idxPropsArray = toIndexProps(idxPropsRaw, labelId, labelIdxSeq, schema)

    QualifierV3Parsed(idxPropsArray, tgtVertexIdRaw, op)
  }

  def toIndexProps(idxPropsRaw: Array[(LabelMeta, InnerValLike)],
                   labelId: Int,
                   labelIdxSeq: Byte,
                   schema: SchemaManager): IndexedSeq[(LabelMeta, InnerValLike)] = {
    val sortKeyTypesArray = schema.findLabelIndexLabelMetas(labelId, labelIdxSeq)

    val size = idxPropsRaw.length
    (0 until size).map { ith =>
      val meta = sortKeyTypesArray(ith)
      val (k, v) = idxPropsRaw(ith)
      meta -> v
    }
  }

  def toMetaProps(value: Array[Byte],
                  labelId: Int,
                  op: Byte,
                  schemaVer: String,
                  schema: SchemaManager) = {
    /* process props */

    if (op == GraphUtil.operations("incrementCount")) {
      //        val countVal = Bytes.toLong(kv.value)
      val countVal = StorageDeserializable.bytesToLong(value, 0)
      Array(LabelMeta.count -> InnerVal.withLong(countVal, schemaVer))
    } else {
      val (props, _) = bytesToKeyValues(value, 0, value.length, schemaVer, schema.findLabelMetas(labelId))
      props
    }
  }

  def toSnapshotRowV3Parsed(row: Array[Byte],
                            qualifier: Array[Byte]): Try[SnapshotRowV3Parsed] = Try {
    val labelWithDirByteLen = 4
    val labelIndexSeqWithIsInvertedByteLen = 1

    val (srcVertexId, srcIdLen) =
      SourceVertexId.fromBytes(row, 0, row.length, HBaseType.DEFAULT_VERSION)

    val isTallSchema =
      (srcIdLen + labelWithDirByteLen + labelIndexSeqWithIsInvertedByteLen) != row.length

    val (tgtVertexId, pos) =
      if (isTallSchema) {
        val (tgtId, tgtBytesLen) = InnerVal.fromBytes(row, srcIdLen, row.length, HBaseType.DEFAULT_VERSION)

        (TargetVertexId(ServiceColumn.Default, tgtId), srcIdLen + tgtBytesLen)
      } else {
        val (tgtVertexId, _) = TargetVertexId.fromBytes(qualifier, 0, qualifier.length, HBaseType.DEFAULT_VERSION)

        (tgtVertexId, srcIdLen)
      }

    val labelWithDir = LabelWithDirection(Bytes.toInt(row, pos, labelWithDirByteLen))
    val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(row, pos + labelWithDirByteLen)

    SnapshotRowV3Parsed(srcVertexId, tgtVertexId, labelWithDir, labelIdxSeq, isInverted)
  }

  def deserializeIndexEdgeCell(cell: Cell,
                               row: Array[Byte],
                               rowV3Parsed: RowV3Parsed,
                               schema: SchemaManager,
                               tallSchemaVersions: Set[String]): Option[WalLog] = {
    val labelWithDir = rowV3Parsed.labelWithDir
    val labelId = labelWithDir.labelId
    val labelIdxSeq = rowV3Parsed.labelIdxSeq
    val srcVertexId = rowV3Parsed.srcVertexId

    val label = schema.findLabel(labelId)
    val schemaVer = label.schemaVersion
    val isTallSchema = tallSchemaVersions(label.schemaVersion)

    val qualifier = cell.getQualifier

    val isDegree = if (isTallSchema) rowV3Parsed.pos == row.length else qualifier.isEmpty
    val validV3Qualifier = !qualifier.isEmpty

    if (isDegree) None
    else if (!isTallSchema && !validV3Qualifier) None
    else {
      val qualifierParsedTry =
        if (isTallSchema) toQualifierV4Parsed(row, rowV3Parsed.pos, schemaVer, labelId, labelIdxSeq, schema)
        else toQualifierV3Parsed(qualifier, schemaVer, labelId, labelIdxSeq, schema)

      qualifierParsedTry.toOption.map { qualifierParsed =>
        val tgtVertexId = qualifierParsed.tgtVertexId
        val idxPropsArray = qualifierParsed.idxPropsArray
        val op = qualifierParsed.op

        val value = cell.getValue

        val metaPropsArray = toMetaProps(value, labelId, op, schemaVer, schema)

        val mergedProps = (idxPropsArray ++ metaPropsArray).toMap
        val tsInnerVal = mergedProps(LabelMeta.timestamp)
        val propsJson = for {
          (labelMeta, innerValLike) <- mergedProps
          jsValue <- JSONParser.innerValToJsValue(innerValLike, labelMeta.dataType)
        } yield {
          labelMeta.name -> jsValue
        }

        val tgtVertexIdInnerId = mergedProps.getOrElse(LabelMeta.to, tgtVertexId.innerId)

        WalLog(
          tsInnerVal.toIdString().toLong,
          GraphUtil.fromOp(op),
          "edge",
          srcVertexId.innerId.toIdString(),
          tgtVertexIdInnerId.toIdString(),
          schema.findLabelService(labelWithDir.labelId).serviceName,
          label.label,
          Json.toJson(propsJson).toString()
        )
      }
    }
  }

  def indexEdgeResultToWals(result: Result,
                            schema: SchemaManager,
                            tallSchemaVersions: Set[String],
                            tgtDirection: Int = 0): Seq[WalLog] = {
    val rawCells = result.rawCells()

    if (rawCells.isEmpty) Nil
    else {
      val head = rawCells.head
      val row = head.getRow
      val rowV3Parsed = toRowV3Parsed(row)

      val labelWithDir = rowV3Parsed.labelWithDir
      val labelId = labelWithDir.labelId

      val inValidRow = rowV3Parsed.isInverted || !schema.checkLabelExist(labelId) || labelWithDir.dir != tgtDirection

      if (inValidRow) Nil
      else {
        rawCells.flatMap { cell =>
          deserializeIndexEdgeCell(cell, row, rowV3Parsed, schema, tallSchemaVersions)
        }
      }
    }
  }


  def snapshotEdgeResultToWals(result: Result,
                               schema: SchemaManager,
                               tallSchemaVersions: Set[String]): Seq[WalLog] = {
    val rawCells = result.rawCells()

    if (rawCells.isEmpty) Nil
    else {
      val head = rawCells.head
      val row = head.getRow
      val qualifier = head.getQualifier

      toSnapshotRowV3Parsed(row, qualifier) match {
        case Success(v) =>
          val SnapshotRowV3Parsed(srcVertexId, tgtVertexId, labelWithDir, _, isInverted) = v

          if (!isInverted) Nil
          else {
            val label = schema.findLabel(labelWithDir.labelId)
            val schemaVer = label.schemaVersion

            rawCells.map { cell =>
              val value = cell.getValue
              val (_, op) = statusCodeWithOp(value.head)

              val (props, _) = bytesToKeyValuesWithTs(value, 1, schemaVer, schema.findLabelMetas(labelWithDir.labelId))
              val kvsMap = props.toMap
              val tsInnerVal = kvsMap(LabelMeta.timestamp).innerVal

              val propsJson = for {
                (labelMeta, innerValLikeWithTs) <- props
                jsValue <- JSONParser.innerValToJsValue(innerValLikeWithTs.innerVal, labelMeta.dataType)
              } yield {
                labelMeta.name -> jsValue
              }

              WalLog(
                tsInnerVal.toIdString().toLong,
                GraphUtil.fromOp(op),
                "edge",
                srcVertexId.innerId.toIdString(),
                tgtVertexId.innerId.toIdString(),
                schema.findLabelService(labelWithDir.labelId).serviceName,
                label.label,
                Json.toJson(propsJson.toMap).toString()
              )
            }
          }

        case Failure(ex) => Nil
      }
    }
  }

  def walLogToRow(walLog: WalLog): Row = {
    Row.fromSeq(
      Seq(walLog.timestamp, walLog.operation, walLog.elem, walLog.from, walLog.to, walLog.service, walLog.label, walLog.props)
    )
  }

  def walVertexToRow(walVertex: WalVertex): Row = {
    Row.fromSeq(
      Seq(walVertex.timestamp, walVertex.operation, walVertex.elem, walVertex.id, walVertex.service, walVertex.column, walVertex.props)
    )
  }

  def vertexResultToWals(result: Result,
                         schema: SchemaManager,
                         bytesToInt: (Array[Byte], Int) => Int = bytesToInt): Seq[WalVertex] = {
    import scala.collection.mutable

    val rawCells = result.rawCells()

    if (rawCells.isEmpty) Nil
    else {
      val head = rawCells.head
      val row = head.getRow

      val version = HBaseType.DEFAULT_VERSION
      val (vertexInnerId, colId, len) = VertexId.fromBytesRaw(row, 0, row.length, version)

      val column = schema.findServiceColumn(colId)
      val columnMetaSeqExistInRow = len + 1 == row.length

      var maxTs = Long.MinValue
      val propsMap = mutable.Map.empty[ColumnMeta, InnerValLike]
      val belongLabelIds = mutable.ListBuffer.empty[Int]

      rawCells.map { cell =>
        val qualifier = cell.getQualifier

        val propKey =
          if (columnMetaSeqExistInRow) row.last
          else {
            if (qualifier.length == 1) qualifier.head.toInt
            else bytesToInt(qualifier, 0)
          }

        val ts = cell.getTimestamp

        if (ts > maxTs) maxTs = ts

        if (S2Vertex.isLabelId(propKey)) {
          belongLabelIds += S2Vertex.toLabelId(propKey)
        } else {
          val v = cell.getValue

          val (value, _) = InnerVal.fromBytes(v, 0, v.length, version)
          val columnMeta = schema.findServiceColumnMeta(colId, propKey.toByte)
          propsMap += (columnMeta -> value)
        }
      }

      val propsJson = for {
        (k, v) <- propsMap
        jsValue <- JSONParser.innerValToJsValue(v, k.dataType)
      } yield {
        k.name -> jsValue
      }

      val ts = propsMap
        .get(ColumnMeta.timestamp)
        .map(_.toIdString().toLong)
        .getOrElse(maxTs)

      val walVertex = WalVertex(
        ts,
        "insert",
        "vertex",
        vertexInnerId.toIdString(),
        schema.findColumnService(colId).serviceName,
        column.columnName,
        Json.toJson(propsJson.toMap).toString()
      )

      Seq(walVertex)
    }
  }

}
