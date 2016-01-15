package com.kakao.s2graph.core


import com.kakao.s2graph.core.GraphExceptions.{InvalidHTableException, LabelAlreadyExistException, LabelNotExistException}
import com.kakao.s2graph.core.Management.JsonModel.{Index, Prop}
import com.kakao.s2graph.core.mysqls._
import com.kakao.s2graph.core.storage.Storage
import com.kakao.s2graph.core.types.HBaseType._
import com.kakao.s2graph.core.types._
import play.api.libs.json.Reads._
import play.api.libs.json._
import scala.util.Try

/**
 * This is designed to be bridge between rest to s2core.
 * s2core never use this for finding models.
 */
object Management extends JSONParser {

  object JsonModel {

    case class Prop(name: String, defaultValue: String, datatType: String)

    object Prop extends ((String, String, String) => Prop)

    case class Index(name: String, propNames: Seq[String])

  }

  import HBaseType._

  val DefaultCompressionAlgorithm = "gz"


  def findService(serviceName: String) = {
    Service.findByName(serviceName, useCache = false)
  }

  def deleteService(serviceName: String) = {
    Service.findByName(serviceName).foreach { service =>
      //      service.deleteAll()
    }
  }

  def updateHTable(labelName: String, newHTableName: String): Try[Int] = Try {
    val targetLabel = Label.findByName(labelName).getOrElse(throw new LabelNotExistException(s"Target label $labelName does not exist."))
    if (targetLabel.hTableName == newHTableName) throw new InvalidHTableException(s"New HTable name is already in use for target label.")

    Label.updateHTableName(targetLabel.label, newHTableName)
  }



  def createServiceColumn(serviceName: String,
                          columnName: String,
                          columnType: String,
                          props: Seq[Prop],
                          schemaVersion: String = DEFAULT_VERSION) = {

    Model withTx { implicit session =>
      val serviceOpt = Service.findByName(serviceName)
      serviceOpt match {
        case None => throw new RuntimeException(s"create service $serviceName has not been created.")
        case Some(service) =>
          val serviceColumn = ServiceColumn.findOrInsert(service.id.get, columnName, Some(columnType), schemaVersion)
          for {
            Prop(propName, defaultValue, dataType) <- props
          } yield {
            ColumnMeta.findOrInsert(serviceColumn.id.get, propName, dataType)
          }
      }
    }
  }

  def deleteColumn(serviceName: String, columnName: String, schemaVersion: String = DEFAULT_VERSION) = {
    Model withTx { implicit session =>
      val service = Service.findByName(serviceName, useCache = false).getOrElse(throw new RuntimeException("Service not Found"))
      val serviceColumns = ServiceColumn.find(service.id.get, columnName, useCache = false)
      val columnNames = serviceColumns.map { serviceColumn =>
        ServiceColumn.delete(serviceColumn.id.get)
        serviceColumn.columnName
      }

      columnNames.getOrElse(throw new RuntimeException("column not found"))
    }
  }

  def findLabel(labelName: String): Option[Label] = {
    Label.findByName(labelName, useCache = false)
  }

  def deleteLabel(labelName: String) = {
    Model withTx { implicit session =>
      Label.findByName(labelName, useCache = false).foreach { label =>
        Label.deleteAll(label)
      }
      labelName
    }
  }

  def addIndex(labelStr: String, indices: Seq[Index]): Try[Label] = {
    Model withTx { implicit session =>
      val label = Label.findByName(labelStr).getOrElse(throw LabelNotExistException(s"$labelStr not found"))
      val labelMetaMap = label.metaPropsInvMap

      indices.foreach { index =>
        val metaSeq = index.propNames.map { name => labelMetaMap(name).seq }
        LabelIndex.findOrInsert(label.id.get, index.name, metaSeq.toList, "none")
      }

      label
    }
  }

  def addProp(labelStr: String, prop: Prop) = {
    Model withTx { implicit session =>
      val labelOpt = Label.findByName(labelStr)
      val label = labelOpt.getOrElse(throw LabelNotExistException(s"$labelStr not found"))

      LabelMeta.findOrInsert(label.id.get, prop.name, prop.defaultValue, prop.datatType)
    }
  }

  def addProps(labelStr: String, props: Seq[Prop]) = {
    Model withTx { implicit session =>
      val labelOpt = Label.findByName(labelStr)
      val label = labelOpt.getOrElse(throw LabelNotExistException(s"$labelStr not found"))

      props.map {
        case Prop(propName, defaultValue, dataType) =>
          LabelMeta.findOrInsert(label.id.get, propName, defaultValue, dataType)
      }
    }
  }

  def addVertexProp(serviceName: String,
                    columnName: String,
                    propsName: String,
                    propsType: String,
                    schemaVersion: String = DEFAULT_VERSION): ColumnMeta = {
    val result = for {
      service <- Service.findByName(serviceName, useCache = false)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName)
    } yield {
        ColumnMeta.findOrInsert(serviceColumn.id.get, propsName, propsType)
      }
    result.getOrElse({
      throw new RuntimeException(s"add property on vertex failed")
    })
  }

  def getServiceLable(label: String): Option[Label] = {
    Label.findByName(label, useCache = true)
  }

  /**
   *
   */

  def toLabelWithDirectionAndOp(label: Label, direction: String): Option[LabelWithDirection] = {
    for {
      labelId <- label.id
      dir = GraphUtil.toDirection(direction)
    } yield LabelWithDirection(labelId, dir)
  }

  def tryOption[A, R](key: A, f: A => Option[R]) = {
    f(key) match {
      case None => throw new GraphExceptions.InternalException(s"$key is not found in DB. create $key first.")
      case Some(r) => r
    }
  }

  def toEdge(ts: Long, operation: String, srcId: String, tgtId: String,
             labelStr: String, direction: String = "", props: String): Edge = {

    val label = tryOption(labelStr, getServiceLable)
    val dir =
      if (direction == "")
//        GraphUtil.toDirection(label.direction)
        GraphUtil.directions("out")
      else
        GraphUtil.toDirection(direction)

    //    logger.debug(s"$srcId, ${label.srcColumnWithDir(dir)}")
    //    logger.debug(s"$tgtId, ${label.tgtColumnWithDir(dir)}")

    val srcVertexId = toInnerVal(srcId, label.srcColumn.columnType, label.schemaVersion)
    val tgtVertexId = toInnerVal(tgtId, label.tgtColumn.columnType, label.schemaVersion)

    val srcColId = label.srcColumn.id.get
    val tgtColId = label.tgtColumn.id.get
    val (srcVertex, tgtVertex) = if (dir == GraphUtil.directions("out")) {
      (Vertex(SourceVertexId(srcColId, srcVertexId), System.currentTimeMillis()),
        Vertex(TargetVertexId(tgtColId, tgtVertexId), System.currentTimeMillis()))
    } else {
      (Vertex(SourceVertexId(tgtColId, tgtVertexId), System.currentTimeMillis()),
        Vertex(TargetVertexId(srcColId, srcVertexId), System.currentTimeMillis()))
    }

    //    val dir = if (direction == "") GraphUtil.toDirection(label.direction) else GraphUtil.toDirection(direction)
    val labelWithDir = LabelWithDirection(label.id.get, dir)
    val op = tryOption(operation, GraphUtil.toOp)

    val jsObject = Json.parse(props).asOpt[JsObject].getOrElse(Json.obj())
    val parsedProps = toProps(label, jsObject.fields).toMap
    val propsWithTs = parsedProps.map(kv => (kv._1 -> InnerValLikeWithTs(kv._2, ts))) ++
      Map(LabelMeta.timeStampSeq -> InnerValLikeWithTs(InnerVal.withLong(ts, label.schemaVersion), ts))

    Edge(srcVertex, tgtVertex, labelWithDir, op, version = ts, propsWithTs = propsWithTs)

  }

  def toVertex(ts: Long, operation: String, id: String, serviceName: String, columnName: String, props: String): Vertex = {
    Service.findByName(serviceName) match {
      case None => throw new RuntimeException(s"$serviceName does not exist. create service first.")
      case Some(service) =>
        ServiceColumn.find(service.id.get, columnName) match {
          case None => throw new RuntimeException(s"$columnName is not exist. create service column first.")
          case Some(col) =>
            val idVal = toInnerVal(id, col.columnType, col.schemaVersion)
            val op = tryOption(operation, GraphUtil.toOp)
            val jsObject = Json.parse(props).asOpt[JsObject].getOrElse(Json.obj())
            val parsedProps = toProps(col, jsObject).toMap
            Vertex(VertexId(col.id.get, idVal), ts, parsedProps, op = op)
        }
    }
  }

  def toProps(column: ServiceColumn, js: JsObject): Seq[(Int, InnerValLike)] = {

    val props = for {
      (k, v) <- js.fields
      meta <- column.metasInvMap.get(k)
    } yield {
        val innerVal = jsValueToInnerVal(v, meta.dataType, column.schemaVersion).getOrElse(
          throw new RuntimeException(s"$k is not defined. create schema for vertex."))

        (meta.seq.toInt, innerVal)
      }
    props

  }

  def toProps(label: Label, js: Seq[(String, JsValue)]): Seq[(Byte, InnerValLike)] = {
    val props = for {
      (k, v) <- js
      meta <- label.metaPropsInvMap.get(k)
      innerVal <- jsValueToInnerVal(v, meta.dataType, label.schemaVersion)
    } yield (meta.seq, innerVal)

    props
  }


  /**
   * update label name.
   */
  def updateLabelName(oldLabelName: String, newLabelName: String) = {
    for {
      old <- Label.findByName(oldLabelName)
    } {
      Label.findByName(newLabelName) match {
        case None =>
          Label.updateName(oldLabelName, newLabelName)
        case Some(_) =>
        //          throw new RuntimeException(s"$newLabelName already exist")
      }
    }
  }
}

class Management(graph: Graph) {
  import Management._
  val storage = graph.storage

  def createTable(zkAddr: String,
                  tableName: String,
                  cfs: List[String],
                  regionMultiplier: Int,
                  ttl: Option[Int],
                  compressionAlgorithm: String = DefaultCompressionAlgorithm): Unit =
    storage.createTable(zkAddr, tableName, cfs, regionMultiplier, ttl, compressionAlgorithm)

  /** HBase specific code */
  def createService(serviceName: String,
                    cluster: String, hTableName: String,
                    preSplitSize: Int, hTableTTL: Option[Int],
                    compressionAlgorithm: String = DefaultCompressionAlgorithm): Try[Service] = {

    Model withTx { implicit session =>
      val service = Service.findOrInsert(serviceName, cluster, hTableName, preSplitSize, hTableTTL, compressionAlgorithm)
      /** create hbase table for service */
      storage.createTable(cluster, hTableName, List("e", "v"), preSplitSize, hTableTTL, compressionAlgorithm)
      service
    }
  }

  /** HBase specific code */
  def createLabel(label: String,
                  srcServiceName: String,
                  srcColumnName: String,
                  srcColumnType: String,
                  tgtServiceName: String,
                  tgtColumnName: String,
                  tgtColumnType: String,
                  isDirected: Boolean = true,
                  serviceName: String,
                  indices: Seq[Index],
                  props: Seq[Prop],
                  consistencyLevel: String,
                  hTableName: Option[String],
                  hTableTTL: Option[Int],
                  schemaVersion: String = DEFAULT_VERSION,
                  isAsync: Boolean,
                  compressionAlgorithm: String = "gz"): Try[Label] = {

    val labelOpt = Label.findByName(label, useCache = false)

    Model withTx { implicit session =>
      labelOpt match {
        case Some(l) =>
          throw new GraphExceptions.LabelAlreadyExistException(s"Label name ${l.label} already exist.")
        case None =>
          /** create all models */
          val newLabel = Label.insertAll(label,
            srcServiceName, srcColumnName, srcColumnType,
            tgtServiceName, tgtColumnName, tgtColumnType,
            isDirected, serviceName, indices, props, consistencyLevel,
            hTableName, hTableTTL, schemaVersion, isAsync, compressionAlgorithm)

          /** create hbase table */
          val service = newLabel.service
          (hTableName, hTableTTL) match {
            case (None, None) => // do nothing
            case (None, Some(hbaseTableTTL)) => throw new RuntimeException("if want to specify ttl, give hbaseTableName also")
            case (Some(hbaseTableName), None) =>
              // create own hbase table with default ttl on service level.
              storage.createTable(service.cluster, hbaseTableName, List("e", "v"), service.preSplitSize, service.hTableTTL, compressionAlgorithm)
            case (Some(hbaseTableName), Some(hbaseTableTTL)) =>
              // create own hbase table with own ttl.
              storage.createTable(service.cluster, hbaseTableName, List("e", "v"), service.preSplitSize, hTableTTL, compressionAlgorithm)
          }
          newLabel
      }
    }
  }

  /**
   * label
   */
  /**
   * copy label when if oldLabel exist and newLabel do not exist.
   * copy label: only used by bulk load job. not sure if we need to parameterize hbase cluster.
   */
  def copyLabel(oldLabelName: String, newLabelName: String, hTableName: Option[String]) = {
    val old = Label.findByName(oldLabelName).getOrElse(throw new LabelAlreadyExistException(s"Old label $oldLabelName not exists."))
    if (Label.findByName(newLabelName).isDefined) throw new LabelAlreadyExistException(s"New label $newLabelName already exists.")

    val allProps = old.metas.map { labelMeta => Prop(labelMeta.name, labelMeta.defaultValue, labelMeta.dataType) }
    val allIndices = old.indices.map { index => Index(index.name, index.propNames) }

    createLabel(newLabelName, old.srcService.serviceName, old.srcColumnName, old.srcColumnType,
      old.tgtService.serviceName, old.tgtColumnName, old.tgtColumnType,
      old.isDirected, old.serviceName,
      allIndices, allProps,
      old.consistencyLevel, hTableName, old.hTableTTL, old.schemaVersion, old.isAsync, old.compressionAlgorithm)
  }
}