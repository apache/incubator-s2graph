/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.core

import org.apache.s2graph.core.GraphExceptions.{LabelNameTooLongException, InvalidHTableException, LabelAlreadyExistException, LabelNotExistException}
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.types.HBaseType._
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.utils.logger
import play.api.libs.json.Reads._
import play.api.libs.json._

import scala.util.Try

/**
 * This is designed to be bridge between rest to s2core.
 * s2core never use this for finding models.
 */
object Management {

  object JsonModel {

    case class Prop(name: String, defaultValue: String, datatType: String)

    object Prop extends ((String, String, String) => Prop)

    case class Index(name: String, propNames: Seq[String], direction: Option[Int] = None, options: Option[String] = None)
  }

  import HBaseType._

  val LABEL_NAME_MAX_LENGTH = 100
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
      val serviceOpt = Service.findByName(serviceName, useCache = false)
      serviceOpt match {
        case None => throw new RuntimeException(s"create service $serviceName has not been created.")
        case Some(service) =>
          val serviceColumn = ServiceColumn.findOrInsert(service.id.get, columnName, Some(columnType), schemaVersion, useCache = false)
          for {
            Prop(propName, defaultValue, dataType) <- props
          } yield {
            ColumnMeta.findOrInsert(serviceColumn.id.get, propName, dataType, useCache = false)
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

  def findLabel(labelName: String, useCache: Boolean = false): Option[Label] = {
    Label.findByName(labelName, useCache = useCache)
  }

  def deleteLabel(labelName: String) = {
    Model withTx { implicit session =>
      Label.findByName(labelName, useCache = false).foreach { label =>
        Label.deleteAll(label)
      }
      labelName
    }
  }

  def markDeletedLabel(labelName: String) = {
    Model withTx { implicit session =>
      Label.findByName(labelName, useCache = false).foreach { label =>
        // rename & delete_at column filled with current time
        Label.markDeleted(label)
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
        LabelIndex.findOrInsert(label.id.get, index.name, metaSeq.toList, "none", index.direction, index.options)
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

  def getServiceLabel(label: String): Option[Label] = {
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

  def toProps(label: Label, js: Seq[(String, JsValue)]): Seq[(LabelMeta, InnerValLike)] = {
    val props = for {
      (k, v) <- js
      meta <- label.metaPropsInvMap.get(k)
      innerVal <- jsValueToInnerVal(v, meta.dataType, label.schemaVersion)
    } yield (meta, innerVal)

    props
  }

  /**
   * update label name.
   */
  def updateLabelName(oldLabelName: String, newLabelName: String) = {
    Model withTx { implicit session =>
      for {
        old <- Label.findByName(oldLabelName, useCache = false)
      } {
        Label.findByName(newLabelName, useCache = false) match {
          case None =>
            Label.updateName(oldLabelName, newLabelName)
          case Some(_) =>
            throw new RuntimeException(s"$newLabelName already exist")
        }
      }
    }
  }

  /**
   * swap label names.
   */
  def swapLabelNames(leftLabel: String, rightLabel: String) = {
    Model withTx { implicit session =>
      val tempLabel = "_" + leftLabel + "_"
      Label.updateName(leftLabel, tempLabel)
      Label.updateName(rightLabel, leftLabel)
      Label.updateName(tempLabel, rightLabel)
    }
  }
}

class Management(graph: S2Graph) {
  import Management._

  def createStorageTable(zkAddr: String,
                  tableName: String,
                  cfs: List[String],
                  regionMultiplier: Int,
                  ttl: Option[Int],
                  compressionAlgorithm: String = DefaultCompressionAlgorithm,
                  replicationScopeOpt: Option[Int] = None,
                  totalRegionCount: Option[Int] = None): Unit = {
    graph.defaultStorage.createTable(zkAddr, tableName, cfs, regionMultiplier, ttl, compressionAlgorithm, replicationScopeOpt, totalRegionCount)
  }


  /** HBase specific code */
  def createService(serviceName: String,
                    cluster: String, hTableName: String,
                    preSplitSize: Int, hTableTTL: Option[Int],
                    compressionAlgorithm: String = DefaultCompressionAlgorithm): Try[Service] = {

    Model withTx { implicit session =>
      val service = Service.findOrInsert(serviceName, cluster, hTableName, preSplitSize, hTableTTL.orElse(Some(Integer.MAX_VALUE)), compressionAlgorithm, useCache = false)
      /** create hbase table for service */
      graph.getStorage(service).createTable(service.cluster, service.hTableName, List("e", "v"), service.preSplitSize, service.hTableTTL, compressionAlgorithm)
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
                  consistencyLevel: String = "weak",
                  hTableName: Option[String] = None,
                  hTableTTL: Option[Int] = None,
                  schemaVersion: String = DEFAULT_VERSION,
                  isAsync: Boolean = false,
                  compressionAlgorithm: String = "gz",
                  options: Option[String] = None): Try[Label] = {

    if (label.length > LABEL_NAME_MAX_LENGTH ) throw new LabelNameTooLongException(s"Label name ${label} too long.( max length : ${LABEL_NAME_MAX_LENGTH}} )")
    if (hTableName.isEmpty && hTableTTL.isDefined) throw new RuntimeException("if want to specify ttl, give hbaseTableName also")

    val labelOpt = Label.findByName(label, useCache = false)
    Model withTx { implicit session =>
      if (labelOpt.isDefined) throw new LabelAlreadyExistException(s"Label name ${label} already exist.")

      /** create all models */
      val newLabel = Label.insertAll(label,
        srcServiceName, srcColumnName, srcColumnType,
        tgtServiceName, tgtColumnName, tgtColumnType,
        isDirected, serviceName, indices, props, consistencyLevel,
        hTableName, hTableTTL, schemaVersion, isAsync, compressionAlgorithm, options)

      /** create hbase table */
      val storage = graph.getStorage(newLabel)
      val service = newLabel.service
      storage.createTable(service.cluster, newLabel.hbaseTableName, List("e", "v"), service.preSplitSize, newLabel.hTableTTL, newLabel.compressionAlgorithm)

      newLabel
    }
  }

  /**
   * label
   */
  /**
   * copy label when if oldLabel exist and newLabel do not exist.
   * copy label: only used by bulk load job. not sure if we need to parameterize hbase cluster.
   */
  def copyLabel(oldLabelName: String, newLabelName: String, hTableName: Option[String]): Try[Label] = {
    val old = Label.findByName(oldLabelName, useCache = false).getOrElse(throw new LabelNotExistException(s"Old label $oldLabelName not exists."))

    val allProps = old.metas(useCache = false).map { labelMeta => Prop(labelMeta.name, labelMeta.defaultValue, labelMeta.dataType) }
    val allIndices = old.indices(useCache = false).map { index => Index(index.name, index.propNames, index.dir, index.options) }

    createLabel(newLabelName, old.srcService.serviceName, old.srcColumnName, old.srcColumnType,
      old.tgtService.serviceName, old.tgtColumnName, old.tgtColumnType,
      old.isDirected, old.serviceName,
      allIndices, allProps,
      old.consistencyLevel, hTableName, old.hTableTTL, old.schemaVersion, old.isAsync, old.compressionAlgorithm, old.options)
  }

  def getCurrentStorageInfo(labelName: String): Try[Map[String, String]] = for {
    label <- Try(Label.findByName(labelName, useCache = false).get)
  } yield {
    val storage = graph.getStorage(label)
    storage.info
  }

  def truncateStorage(labelName: String): Unit = {
    Try(Label.findByName(labelName, useCache = false)).map { labelOpt =>
      labelOpt.map { label =>
        val storage = graph.getStorage(label)
        val zkAddr = label.service.cluster
        storage.truncateTable(zkAddr, label.hbaseTableName)
      }
    }
  }

  def deleteStorage(labelName: String): Unit = {
    Try(Label.findByName(labelName, useCache = false)).map { labelOpt =>
      labelOpt.map { label =>
        val storage = graph.getStorage(label)
        val zkAddr = label.service.cluster
        storage.deleteTable(zkAddr, label.hbaseTableName)
      }
    }
  }
}

