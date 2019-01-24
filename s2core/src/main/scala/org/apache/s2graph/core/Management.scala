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


import java.util.concurrent.Executors

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.s2graph.core.GraphExceptions.{InvalidHTableException, LabelAlreadyExistException, LabelNameTooLongException, LabelNotExistException}
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.types.HBaseType._
import org.apache.s2graph.core.types._
import org.apache.s2graph.core.JSONParser._
import org.apache.s2graph.core.utils.Importer
import play.api.libs.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * This is designed to be bridge between rest to s2core.
  * s2core never use this for finding models.
  */
object Management {

  import HBaseType._
  import scala.collection.JavaConversions._

  val ZookeeperQuorum = "hbase.zookeeper.quorum"
  val ColumnFamilies = "hbase.table.column.family"
  val RegionMultiplier = "hbase.table.region.multiplier"
  val Ttl = "hbase.table.ttl"
  val CompressionAlgorithm = "hbase.table.compression.algorithm"
  val ReplicationScope = "hbase.table.replication.scope"
  val TotalRegionCount = "hbase.table.total.region.count"

  val DefaultColumnFamilies = Seq("e", "v")
  val DefaultCompressionAlgorithm = "gz"
  val LABEL_NAME_MAX_LENGTH = 100


  def newProp(name: String, defaultValue: String, datatType: String): Prop = {
    new Prop(name, defaultValue, datatType)
  }

  def newIndex(name: String, propNames: java.util.List[String], options: String): Index = {
    new Index(name, propNames, options = Option(options))
  }

  object JsonModel {
    import play.api.libs.functional.syntax._

    case class Prop(name: String, defaultValue: String, dataType: String, storeInGlobalIndex: Boolean = false)

    object Prop extends ((String, String, String, Boolean) => Prop)

    case class Index(name: String, propNames: Seq[String], direction: Option[Int] = None, options: Option[String] = None)

    case class HTableParams(cluster: String, hTableName: String,
                            preSplitSize: Int, hTableTTL: Option[Int], compressionAlgorithm: Option[String]) {

      override def toString(): String = {
        s"""HtableParams
           |-- cluster : $cluster
           |-- hTableName : $hTableName
           |-- preSplitSize : $preSplitSize
           |-- hTableTTL : $hTableTTL
           |-- compressionAlgorithm : $compressionAlgorithm
           |""".stripMargin
      }
    }

    implicit object HTableParamsJsonConverter extends Format[HTableParams] {
      def reads(json: JsValue): JsResult[HTableParams] = (
        (__ \ "cluster").read[String] and
          (__ \ "hTableName").read[String] and
          (__ \ "preSplitSize").read[Int] and
          (__ \ "hTableTTL").readNullable[Int] and
          (__ \ "compressionAlgorithm").readNullable[String])(HTableParams.apply _).reads(json)

      def writes(o: HTableParams): JsValue = Json.obj(
        "cluster" -> o.cluster,
        "hTableName" -> o.hTableName,
        "preSplitSize" -> o.preSplitSize,
        "hTableTTL" -> o.hTableTTL,
        "compressionAlgorithm" -> o.compressionAlgorithm
      )
    }
  }

  def findService(serviceName: String) = {
    Service.findByName(serviceName, useCache = false)
  }

  def findServiceColumn(serviceName: String, columnName: String): Option[ServiceColumn] = {
    Service.findByName(serviceName, useCache = false).flatMap { service =>
      ServiceColumn.find(service.id.get, columnName, useCache = false)
    }
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
                          schemaVersion: String = DEFAULT_VERSION,
                          options: Option[String] = None) = {

    Schema withTx { implicit session =>
      val serviceOpt = Service.findByName(serviceName, useCache = false)
      serviceOpt match {
        case None => throw new RuntimeException(s"create service $serviceName has not been created.")
        case Some(service) =>
          val serviceColumn = ServiceColumn.findOrInsert(service.id.get, columnName, Some(columnType), schemaVersion, options, useCache = false)
          for {
            Prop(propName, defaultValue, dataType, storeInGlobalIndex) <- props
          } yield {
            ColumnMeta.findOrInsert(serviceColumn.id.get, propName, dataType,
              defaultValue,
              storeInGlobalIndex = storeInGlobalIndex, useCache = false)
          }
      }
    }
  }

  def deleteColumn(serviceName: String, columnName: String, schemaVersion: String = DEFAULT_VERSION) = {
    Schema withTx { implicit session =>
      val service = Service.findByName(serviceName, useCache = false).getOrElse(throw new RuntimeException("Service not Found"))
      val serviceColumns = ServiceColumn.find(service.id.get, columnName, useCache = false)
      val columnNames = serviceColumns.map { serviceColumn =>
        ServiceColumn.delete(serviceColumn.id.get)
        serviceColumn
      }

      columnNames.getOrElse(throw new RuntimeException("column not found"))
    }
  }

  def findLabel(labelName: String, useCache: Boolean = false): Option[Label] = {
    Label.findByName(labelName, useCache = useCache)
  }

  def findLabels(serviceName: String, useCache: Boolean = false): Seq[Label] = {
    Service.findByName(serviceName, useCache = useCache).map { service =>
      Label.findBySrcServiceId(service.id.get, useCache = useCache)
    }.getOrElse(Nil)
  }

  def deleteLabel(labelName: String): Try[Label] = {
    Schema withTx { implicit session =>
      val label = Label.findByName(labelName, useCache = false).getOrElse(throw GraphExceptions.LabelNotExistException(labelName))
      Label.deleteAll(label)
      label
    }
  }

  def markDeletedLabel(labelName: String) = {
    Schema withTx { implicit session =>
      Label.findByName(labelName, useCache = false).foreach { label =>
        // rename & delete_at column filled with current time
        Label.markDeleted(label)
      }
      labelName
    }
  }

  def addIndex(labelStr: String, indices: Seq[Index]): Try[Label] = {
    Schema withTx { implicit session =>
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
    Schema withTx { implicit session =>
      val labelOpt = Label.findByName(labelStr)
      val label = labelOpt.getOrElse(throw LabelNotExistException(s"$labelStr not found"))

      LabelMeta.findOrInsert(label.id.get, prop.name, prop.defaultValue, prop.dataType, prop.storeInGlobalIndex)
    }
  }

  def addProps(labelStr: String, props: Seq[Prop]) = {
    Schema withTx { implicit session =>
      val labelOpt = Label.findByName(labelStr)
      val label = labelOpt.getOrElse(throw LabelNotExistException(s"$labelStr not found"))

      props.map {
        case Prop(propName, defaultValue, dataType, storeInGlobalIndex) =>
          LabelMeta.findOrInsert(label.id.get, propName, defaultValue, dataType, storeInGlobalIndex)
      }
    }
  }

  def addVertexProp(serviceName: String,
                    columnName: String,
                    propsName: String,
                    propsType: String,
                    defaultValue: String,
                    storeInGlobalIndex: Boolean = false,
                    schemaVersion: String = DEFAULT_VERSION): ColumnMeta = {
    val result = for {
      service <- Service.findByName(serviceName, useCache = false)
      serviceColumn <- ServiceColumn.find(service.id.get, columnName)
    } yield {
      ColumnMeta.findOrInsert(serviceColumn.id.get, propsName, propsType, defaultValue, storeInGlobalIndex)
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
    Schema withTx { implicit session =>
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
    Schema withTx { implicit session =>
      val tempLabel = "_" + leftLabel + "_"
      Label.updateName(leftLabel, tempLabel)
      Label.updateName(rightLabel, leftLabel)
      Label.updateName(tempLabel, rightLabel)
    }
  }

  def toConfig(params: Map[String, Any]): Config = {
    import scala.collection.JavaConversions._

    val filtered = params.filter { case (k, v) =>
      v match {
        case None => false
        case _ => true
      }
    }.map { case (k, v) =>
      val newV = v match {
        case Some(value) => value
        case _ => v
      }
      k -> newV
    }

    ConfigFactory.parseMap(filtered)
  }
}

class Management(graph: S2GraphLike) {

  val importEx = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  import Management._
  import GraphUtil._

  def updateEdgeFetcher(labelName: String, options: String): Unit = {
    val label = Label.findByName(labelName).getOrElse(throw new LabelNotExistException(labelName))

    updateEdgeFetcher(label, stringToOption(options))
  }

  def updateEdgeFetcher(label: Label, options: Option[String]): Unit = {
    val newLabel = options.map(Label.updateOption(label, _)).getOrElse(label)
    graph.resourceManager.getOrElseUpdateEdgeFetcher(newLabel, cacheTTLInSecs = Option(-1))
  }

  def updateVertexFetcher(serviceName: String, columnName: String, options: String): Unit = {
    val service = Service.findByName(serviceName).getOrElse(throw new IllegalArgumentException(s"$serviceName is not exist."))
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse(throw new IllegalArgumentException(s"$columnName is not exist."))

    updateVertexFetcher(column, stringToOption(options))
  }

  def updateVertexFetcher(column: ServiceColumn, options: Option[String]): Unit = {
    val newColumn = options.map(ServiceColumn.updateOption(column, _)).getOrElse(column)
    graph.resourceManager.getOrElseUpdateVertexFetcher(newColumn, cacheTTLInSecs = Option(-1))
  }

  def updateEdgeMutator(labelName: String, options: String): Unit = {
    val label = Label.findByName(labelName).getOrElse(throw new LabelNotExistException(labelName))

    updateEdgeMutator(label, stringToOption(options))
  }

  def updateEdgeMutator(label: Label, options: Option[String]): Unit = {
    val newLabel = options.map(Label.updateOption(label, _)).getOrElse(label)
    graph.resourceManager.getOrElseUpdateEdgeMutator(newLabel, cacheTTLInSecs = Option(-1))
  }

  def updateVertexMutator(serviceName: String, columnName: String, options: String): Unit = {
    val service = Service.findByName(serviceName).getOrElse(throw new IllegalArgumentException(s"$serviceName is not exist."))
    val column = ServiceColumn.find(service.id.get, columnName).getOrElse(throw new IllegalArgumentException(s"$columnName is not exist."))

    updateVertexMutator(column, stringToOption(options))
  }

  def updateVertexMutator(column: ServiceColumn, options: Option[String]): Unit = {
    val newColumn = options.map(ServiceColumn.updateOption(column, _)).getOrElse(column)
    graph.resourceManager.getOrElseUpdateVertexMutator(newColumn, cacheTTLInSecs = Option(-1))
  }

  def createStorageTable(zkAddr: String,
                         tableName: String,
                         cfs: List[String],
                         regionMultiplier: Int,
                         ttl: Option[Int],
                         compressionAlgorithm: String = DefaultCompressionAlgorithm,
                         replicationScopeOpt: Option[Int] = None,
                         totalRegionCount: Option[Int] = None): Unit = {
    val config = toConfig(Map(
      ZookeeperQuorum -> zkAddr,
//      ColumnFamilies -> cfs,
      RegionMultiplier -> regionMultiplier,
      Ttl -> ttl,
      CompressionAlgorithm -> compressionAlgorithm,
      TotalRegionCount -> totalRegionCount
    ))
    graph.defaultStorage.createTable(config, tableName)
  }


  /** HBase specific code */
  def createService(serviceName: String,
                    cluster: String,
                    hTableName: String,
                    preSplitSize: Int,
                    hTableTTL: Int,
                    compressionAlgorithm: String): Service = {
    createService(serviceName, cluster, hTableName, preSplitSize,
      Option(hTableTTL).filter(_ > -1), compressionAlgorithm).get
  }

  def createService(serviceName: String,
                    cluster: String, hTableName: String,
                    preSplitSize: Int, hTableTTL: Option[Int],
                    compressionAlgorithm: String = DefaultCompressionAlgorithm): Try[Service] = {

    Schema withTx { implicit session =>
      val service = Service.findOrInsert(serviceName, cluster, hTableName, preSplitSize, hTableTTL.orElse(Some(Integer.MAX_VALUE)), compressionAlgorithm, useCache = false)
      val config = toConfig(Map(
        ZookeeperQuorum -> service.cluster,
//        ColumnFamilies -> List("e", "v"),
        RegionMultiplier -> service.preSplitSize,
        Ttl -> service.hTableTTL,
        CompressionAlgorithm -> compressionAlgorithm
      ))
      /* create hbase table for service */
      graph.getStorage(service).createTable(config, service.hTableName)
      service
    }
  }

//  def createServiceColumn(serviceName: String,
//                          columnName: String,
//                          columnType: String,
//                          props: java.util.List[Prop],
//                          schemaVersion: String = DEFAULT_VERSION): ServiceColumn =
//    createServiceColumn(serviceName, columnName, columnType, props.toSeq, schemaVersion)

  def createServiceColumn(serviceName: String,
                          columnName: String,
                          columnType: String,
                          props: Seq[Prop],
                          schemaVersion: String = DEFAULT_VERSION,
                          options: Option[String] = None): ServiceColumn = {

    val serviceColumnTry = Schema withTx { implicit session =>
      val serviceOpt = Service.findByName(serviceName, useCache = false)
      serviceOpt match {
        case None => throw new RuntimeException(s"create service $serviceName has not been created.")
        case Some(service) =>
          val serviceColumn = ServiceColumn.findOrInsert(service.id.get, columnName, Some(columnType), schemaVersion, options, useCache = false)
          for {
            Prop(propName, defaultValue, dataType, storeInGlobalIndex) <- props
          } yield {
            ColumnMeta.findOrInsert(serviceColumn.id.get, propName, dataType, defaultValue,
              storeInGlobalIndex = storeInGlobalIndex, useCache = false)
          }

          updateVertexMutator(serviceColumn, None)
          updateVertexFetcher(serviceColumn, None)

          serviceColumn
      }
    }

    serviceColumnTry.get
  }

  def createLabel(labelName: String,
                  srcColumn: ServiceColumn,
                  tgtColumn: ServiceColumn,
                  isDirected: Boolean,
                  serviceName: String,
                  indices: java.util.List[Index],
                  props: java.util.List[Prop],
                  consistencyLevel: String,
                  hTableName: String,
                  hTableTTL: Int,
                  schemaVersion: String,
                  compressionAlgorithm: String,
                  options: String
                 ): Label = {
    import scala.collection.JavaConversions._

    createLabel(labelName,
      srcColumn.service.serviceName, srcColumn.columnName, srcColumn.columnType,
      tgtColumn.service.serviceName, tgtColumn.columnName, tgtColumn.columnType,
      serviceName, indices, props, isDirected, consistencyLevel,
      Option(hTableName), Option(hTableTTL).filter(_ > -1),
      schemaVersion, false, compressionAlgorithm, stringToOption(options)
    ).get
  }

  /** HBase specific code */
  def createLabel(label: String,
                  srcServiceName: String,
                  srcColumnName: String,
                  srcColumnType: String,
                  tgtServiceName: String,
                  tgtColumnName: String,
                  tgtColumnType: String,
                  serviceName: String,
                  indices: Seq[Index],
                  props: Seq[Prop],
                  isDirected: Boolean = true,
                  consistencyLevel: String = "weak",
                  hTableName: Option[String] = None,
                  hTableTTL: Option[Int] = None,
                  schemaVersion: String = DEFAULT_VERSION,
                  isAsync: Boolean = false,
                  compressionAlgorithm: String = "gz",
                  options: Option[String] = None,
                  initFetcherWithOptions: Boolean = false
                 ): Try[Label] = {

    if (label.length > LABEL_NAME_MAX_LENGTH) throw new LabelNameTooLongException(s"Label name ${label} too long.( max length : ${LABEL_NAME_MAX_LENGTH}} )")
    if (hTableName.isEmpty && hTableTTL.isDefined) throw new RuntimeException("if want to specify ttl, give hbaseTableName also")

    val labelOpt = Label.findByName(label, useCache = false)
    val newLabelTry = Schema withTx { implicit session =>
      if (labelOpt.isDefined) throw new LabelAlreadyExistException(s"Label name ${label} already exist.")

      /* create all models */
      val newLabel = Label.insertAll(label,
        srcServiceName, srcColumnName, srcColumnType,
        tgtServiceName, tgtColumnName, tgtColumnType,
        isDirected, serviceName, indices, props, consistencyLevel,
        hTableName, hTableTTL, schemaVersion, isAsync, compressionAlgorithm, options)

      /* create hbase table */
      val storage = graph.getStorage(newLabel)
      val service = newLabel.service
      val config = toConfig(Map(
        ZookeeperQuorum -> service.cluster,
//        ColumnFamilies -> List("e", "v"),
        RegionMultiplier -> service.preSplitSize,
        Ttl -> newLabel.hTableTTL,
        CompressionAlgorithm -> newLabel.compressionAlgorithm
      ))
      storage.createTable(config, newLabel.hbaseTableName)

      newLabel
    }

    newLabelTry.foreach { newLabel =>
      if (initFetcherWithOptions) {
        updateEdgeFetcher(newLabel, options)
      } else {
        updateEdgeFetcher(newLabel, None)
      }
    }

    newLabelTry
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
      old.serviceName,
      allIndices, allProps,
      old.isDirected,
      old.consistencyLevel, hTableName, old.hTableTTL, old.schemaVersion, old.isAsync, old.compressionAlgorithm, old.options)
  }

  def enableVertexGlobalIndex(columnMeats: Seq[ColumnMeta]): Boolean = {
    val successes = columnMeats.map { cm =>
      ColumnMeta.updateStoreInGlobalIndex(cm.id.get, cm.storeInGlobalIndex)
    }.map(_.isSuccess)

    successes.forall(identity)
  }

  def enableEdgeGlobalIndex(labelMetas: Seq[LabelMeta]): Boolean = {
    val successes = labelMetas.map { lm =>
      LabelMeta.updateStoreInGlobalIndex(lm.id.get, lm.storeInGlobalIndex)
    }.map(_.isSuccess)

    successes.forall(identity)
  }

//  def buildGlobalVertexIndex(name: String, propNames: java.util.List[String]): GlobalIndex =
//    buildGlobalIndex(GlobalIndex.VertexType, name, propNames)
//
//  def buildGlobalVertexIndex(name: String, propNames: Seq[String]): GlobalIndex =
//    buildGlobalIndex(GlobalIndex.VertexType, name, propNames)
//
//  def buildGlobalEdgeIndex(name: String, propNames: java.util.List[String]): GlobalIndex =
//    buildGlobalIndex(GlobalIndex.EdgeType, name, propNames)
//
//  def buildGlobalEdgeIndex(name: String, propNames: Seq[String]): GlobalIndex =
//    buildGlobalIndex(GlobalIndex.EdgeType, name, propNames)
//
//  def buildGlobalIndex(elementType: String, name: String, propNames: Seq[String]): GlobalIndex = {
//    GlobalIndex.findBy(elementType, name, false) match {
//      case None =>
//        GlobalIndex.insert(elementType, name, propNames)
//        GlobalIndex.findBy(elementType, name, false).get
//      case Some(oldIndex) => oldIndex
//    }
//  }

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

        val config = toConfig(Map(ZookeeperQuorum -> zkAddr))
        storage.truncateTable(config, label.hbaseTableName)
      }
    }
  }

  def deleteStorage(labelName: String): Unit = {
    Try(Label.findByName(labelName, useCache = false)).map { labelOpt =>
      labelOpt.map { label =>
        val storage = graph.getStorage(label)
        val zkAddr = label.service.cluster

        val config = toConfig(Map(ZookeeperQuorum -> zkAddr))
        storage.deleteTable(config, label.hbaseTableName)
      }
    }
  }
}

