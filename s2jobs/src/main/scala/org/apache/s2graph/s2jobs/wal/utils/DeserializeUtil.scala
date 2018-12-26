package org.apache.s2graph.s2jobs.wal.utils

import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.s2graph.core.{GraphUtil, JSONParser, SnapshotEdge}
import org.apache.s2graph.core.schema.{Label, LabelIndex, LabelMeta, ServiceColumn}
import org.apache.s2graph.core.storage.SKeyValue
import org.apache.s2graph.core.storage.serde.StorageDeserializable
import org.apache.s2graph.core.types._
import org.apache.s2graph.s2jobs.S2GraphHelper.logger
import org.apache.s2graph.s2jobs.wal.{LabelSchema, WalLog}
import org.apache.spark.sql.Row
import play.api.libs.json.Json

import scala.util.Try


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

  def toRowV3Parsed(cell: Cell): RowV3Parsed = {
    //TODO:
    val row = cell.getRow
    var pos = 0
    val (srcVertexId, srcIdLen) = SourceVertexId.fromBytes(row, pos, row.length, HBaseType.DEFAULT_VERSION)
    pos += srcIdLen
    val labelWithDir = LabelWithDirection(Bytes.toInt(row, pos, 4))
    pos += 4
    val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(row, pos)
    pos += 1

    RowV3Parsed(srcVertexId, labelWithDir, labelIdxSeq, isInverted, pos)
  }

  def toQualifierV3Parsed(qualifier: Array[Byte],
                          schemaVer: String,
                          labelId: Int,
                          labelIdxSeq: Byte,
                          labelIndexLabelMetas: Map[Int, Map[Byte, Array[LabelMeta]]]): Try[QualifierV3Parsed] = Try {
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

    val idxPropsArray = toIndexProps(idxPropsRaw, labelId, labelIdxSeq, labelIndexLabelMetas)

    QualifierV3Parsed(idxPropsArray, tgtVertexIdRaw, op)
  }

  def toIndexProps(idxPropsRaw: Array[(LabelMeta, InnerValLike)],
                   labelId: Int,
                   labelIdxSeq: Byte,
                   labelIndexLabelMetas: Map[Int, Map[Byte, Array[LabelMeta]]]): IndexedSeq[(LabelMeta, InnerValLike)] = {
    val indexLabelMetas = labelIndexLabelMetas.getOrElse(labelId, throw new IllegalArgumentException(s"${labelId} not found in labelIndexLabelMetas"))
    val sortKeyTypesArray = indexLabelMetas.getOrElse(labelIdxSeq, throw new IllegalArgumentException(s"invalid index seq for meta array: ${labelId}, ${labelIdxSeq}"))

    val size = idxPropsRaw.length
    (0 until size).map { ith =>
      val meta = sortKeyTypesArray(ith)
      val (k, v) = idxPropsRaw(ith)
      meta -> v
    }
  }

  def toMetaProps(cell: Cell,
                  labelMetas: Map[Int, Map[Byte, LabelMeta]],
                  labelId: Int,
                  op: Byte,
                  schemaVer: String) = {
    val value = cell.getValue
    /* process props */

    if (op == GraphUtil.operations("incrementCount")) {
      //        val countVal = Bytes.toLong(kv.value)
      val countVal = StorageDeserializable.bytesToLong(value, 0)
      Array(LabelMeta.count -> InnerVal.withLong(countVal, schemaVer))
    } else {
      val (props, endAt) = bytesToKeyValues(value, 0, value.length, schemaVer, labelMetas(labelId))
      props
    }
  }

  def indexEdgeResultToWalsV3(result: Result,
                              labelSchema: LabelSchema,
                              tallSchemaVersions: Set[String] = Set(HBaseType.VERSION4),
                              tgtDirection: Int = 0): Iterable[WalLog] = {
    import scala.collection.mutable
    val buffer = mutable.ArrayBuffer.empty[WalLog]
    val LabelSchema(labelService, labels, _, labelIndexLabelMetas, labelMetas) = labelSchema

    val scanner = result.cellScanner()

    var rowV3Parsed: RowV3Parsed = null
    var breakOut = false

    while (scanner.advance() && !breakOut) {
      val cell = scanner.current()

      if (rowV3Parsed == null) {
        rowV3Parsed = toRowV3Parsed(cell)
      }

      val labelWithDir = rowV3Parsed.labelWithDir
      val labelId = labelWithDir.labelId
      val labelIdxSeq = rowV3Parsed.labelIdxSeq
      val srcVertexId = rowV3Parsed.srcVertexId

      breakOut = rowV3Parsed.isInverted || !labels.contains(labelId) || labelWithDir.dir != tgtDirection

      if (!breakOut) {
        val qualifier = cell.getQualifier
        val validV3Qualifier = !qualifier.isEmpty

        if (validV3Qualifier) {
          val label = labels(labelId)
          val schemaVer = label.schemaVersion

          toQualifierV3Parsed(qualifier, schemaVer, labelId, labelIdxSeq, labelIndexLabelMetas).foreach { qualifierV3Parsed =>
            val tgtVertexId = qualifierV3Parsed.tgtVertexId
            val idxPropsArray = qualifierV3Parsed.idxPropsArray
            val op = qualifierV3Parsed.op

            val metaPropsArray = toMetaProps(cell, labelMetas, labelId, op, schemaVer)

            val mergedProps = (idxPropsArray ++ metaPropsArray).toMap
            val tsInnerVal = mergedProps(LabelMeta.timestamp)
            val propsJson = for {
              (labelMeta, innerValLike) <- mergedProps
              jsValue <- JSONParser.innerValToJsValue(innerValLike, labelMeta.dataType)
            } yield {
              labelMeta.name -> jsValue
            }

            val tgtVertexIdInnerId = mergedProps.getOrElse(LabelMeta.to, tgtVertexId.innerId)

            val walLog = WalLog(
              tsInnerVal.toIdString().toLong,
              GraphUtil.fromOp(op),
              "edge",
              srcVertexId.innerId.toIdString(),
              tgtVertexIdInnerId.toIdString(),
              labelService(labelWithDir.labelId),
              label.label,
              Json.toJson(propsJson).toString()
            )

            buffer += walLog
          }
        }
      }
    }

    buffer
  }

  def indexEdgeKeyValueToRow(kv: SKeyValue,
                             cacheElementOpt: Option[SnapshotEdge],
                             labelSchema: LabelSchema,
                             tallSchemaVersions: Set[String]): Option[WalLog] = {
    try {
      val LabelSchema(labelService, labels, labelIndices, labelIndexLabelMetas, labelMetas) = labelSchema

      var pos = 0
      val (srcVertexId, srcIdLen) = SourceVertexId.fromBytes(kv.row, pos, kv.row.length, HBaseType.DEFAULT_VERSION)
      pos += srcIdLen
      val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
      pos += 4
      val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(kv.row, pos)
      pos += 1

      if (isInverted) None
      else if (!labels.contains(labelWithDir.labelId)) None
      else {
        val label = labels.getOrElse(labelWithDir.labelId, throw new IllegalArgumentException(s"$labelWithDir labelId is not found."))
        val schemaVer = label.schemaVersion

        val isTallSchema = tallSchemaVersions(label.schemaVersion)
        val isDegree = if (isTallSchema) pos == kv.row.length else kv.qualifier.isEmpty

        if (isDegree) {
          None
        } else {
          // not degree edge
          val (idxPropsRaw, endAt) =
            if (isTallSchema) StorageDeserializable.bytesToProps(kv.row, pos, schemaVer)
            else {
              StorageDeserializable.bytesToProps(kv.qualifier, 0, schemaVer)
            }
          pos = endAt

          val (tgtVertexIdRaw, tgtVertexIdLen) = if (isTallSchema) {
            if (endAt == kv.row.length - 1) {
              (HBaseType.defaultTgtVertexId, 0)
            } else {
              TargetVertexId.fromBytes(kv.row, endAt, kv.row.length - 1, schemaVer)
            }
          } else {
            if (endAt == kv.qualifier.length) {
              (HBaseType.defaultTgtVertexId, 0)
            } else {
              TargetVertexId.fromBytes(kv.qualifier, endAt, kv.qualifier.length, schemaVer)
            }
          }
          pos += tgtVertexIdLen

          val op =
            if (isTallSchema) kv.row(kv.row.length - 1)
            else {
              if (kv.qualifier.length == pos) GraphUtil.defaultOpByte
              else kv.qualifier(kv.qualifier.length - 1)
            }

          val indexLabelMetas = labelIndexLabelMetas.getOrElse(labelWithDir.labelId, throw new IllegalArgumentException(s"${labelWithDir.labelId} not found in labelIndexLabelMetas"))
          val sortKeyTypesArray = indexLabelMetas.getOrElse(labelIdxSeq, throw new IllegalArgumentException(s"invalid index seq for meta array: ${label.id.get}, ${labelIdxSeq}"))

          /* process indexProps */
          val size = idxPropsRaw.length
          val idxProps = (0 until size).map { ith =>
            val meta = sortKeyTypesArray(ith)
            val (k, v) = idxPropsRaw(ith)
            meta -> v
          }

          /* process props */
          val metaProps =
            if (op == GraphUtil.operations("incrementCount")) {
              //        val countVal = Bytes.toLong(kv.value)
              val countVal = StorageDeserializable.bytesToLong(kv.value, 0)
              Array(LabelMeta.count -> InnerVal.withLong(countVal, schemaVer))
            } else {
              val (props, endAt) = bytesToKeyValues(kv.value, 0, kv.value.length, schemaVer, labelMetas(labelWithDir.labelId))
              props
            }

          val mergedProps = (idxProps ++ metaProps).toMap
          val tsInnerVal = mergedProps(LabelMeta.timestamp)
          val propsJson = for {
            (labelMeta, innerValLike) <- mergedProps
            jsValue <- JSONParser.innerValToJsValue(innerValLike, labelMeta.dataType)
          } yield {
            labelMeta.name -> jsValue
          }
          /* process tgtVertexId */
          // skip
          val tgtVertexIdInnerId = mergedProps.getOrElse(LabelMeta.to, tgtVertexIdRaw.innerId)

          val wal = WalLog(
            tsInnerVal.toIdString().toLong,
            GraphUtil.fromOp(op),
            "edge",
            srcVertexId.innerId.toIdString(),
            tgtVertexIdInnerId.toIdString(),
            labelService(labelWithDir.labelId),
            label.label,
            Json.toJson(propsJson).toString()
          )

          Option(wal)
        }
      }
    } catch {
      case e: Exception =>
        println(e.toString)
        logger.error(s"$kv", e)
        None
    }
  }

  def snapshotEdgeKeyValueToRow(kv: SKeyValue,
                                cacheElementOpt: Option[SnapshotEdge],
                                labelSchema: LabelSchema): Option[WalLog] = {
    try {
      val LabelSchema(labelService, labels, _, _, labelMetas) = labelSchema

      var pos = 0
      val (srcVertexId, srcIdLen) = SourceVertexId.fromBytes(kv.row, pos, kv.row.length, HBaseType.DEFAULT_VERSION)
      pos += srcIdLen

      val isTallSchema = pos + 5 != kv.row.length
      var tgtVertexId = TargetVertexId(ServiceColumn.Default, srcVertexId.innerId)

      if (isTallSchema) {
        val (tgtId, tgtBytesLen) = InnerVal.fromBytes(kv.row, pos, kv.row.length, HBaseType.DEFAULT_VERSION)
        tgtVertexId = TargetVertexId(ServiceColumn.Default, tgtId)
        pos += tgtBytesLen
      }

      val labelWithDir = LabelWithDirection(Bytes.toInt(kv.row, pos, 4))
      pos += 4
      val (labelIdxSeq, isInverted) = bytesToLabelIndexSeqWithIsInverted(kv.row, pos)
      pos += 1

      if (!isInverted) None
      else {
        val label = labels(labelWithDir.labelId)
        val schemaVer = label.schemaVersion

        var pos = 0
        val (statusCode, op) = statusCodeWithOp(kv.value(pos))
        pos += 1
        val (props, endAt) = bytesToKeyValuesWithTs(kv.value, pos, schemaVer, labelMetas(labelWithDir.labelId))
        val kvsMap = props.toMap
        val tsInnerVal = kvsMap(LabelMeta.timestamp).innerVal

        val propsJson = for {
          (labelMeta, innerValLikeWithTs) <- props
          jsValue <- JSONParser.innerValToJsValue(innerValLikeWithTs.innerVal, labelMeta.dataType)
        } yield {
          labelMeta.name -> jsValue
        }

        pos = endAt

        val wal = WalLog(
          tsInnerVal.toIdString().toLong,
          GraphUtil.fromOp(op),
          "edge",
          srcVertexId.innerId.toIdString(),
          tgtVertexId.innerId.toIdString(),
          labelService(labelWithDir.labelId),
          label.label,
          Json.toJson(propsJson.toMap).toString()
        )

        Option(wal)
      }
    } catch {
      case e: Exception =>
        println(e.toString)
        logger.error(e)
        None
    }
  }
}
