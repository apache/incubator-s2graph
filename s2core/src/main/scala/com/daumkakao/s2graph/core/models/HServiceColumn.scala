package com.daumkakao.s2graph.core.models

import HBaseModel._
import play.api.libs.json.Json

/**
 * Created by shon on 5/15/15.
 */

object HServiceColumn {
  def findById(id: Int, useCache: Boolean = true): HServiceColumn = {
    HBaseModel.find[HServiceColumn](useCache)(Seq(("id" -> id))).get
  }
  def find(serviceId: Int, columnName: String, useCache: Boolean = true): Option[HServiceColumn] = {
    HBaseModel.find[HServiceColumn](useCache)(Seq("serviceId" -> serviceId, "columnName" -> columnName))
  }
  def findsByServiceId(serviceId: Int, useCache: Boolean = true): List[HServiceColumn] = {
    HBaseModel.findsMatch[HServiceColumn](useCache)(Seq("serviceId" -> serviceId))
  }
  def findOrInsert(serviceId: Int, columnName: String, columnType: Option[String]): HServiceColumn = {
    find(serviceId, columnName, useCache = false) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq[HServiceColumn]
        val model = new HServiceColumn(Map("id" -> id, "serviceId" -> serviceId, "columnName" -> columnName,
          "columnType" -> columnType.getOrElse("string")))
        model.create
        model
    }
  }
}
case class HServiceColumn(kvsParam: Map[KEY, VAL]) extends HBaseModel[HServiceColumn]("HServiceColumn", kvsParam) {
  override val columns = Seq("id", "serviceId", "columnName", "columnType")
  val pk = Seq(("id", kvs("id")))
  val idxServiceIdColumnName = Seq(("serviceId", kvs("serviceId")), ("columnName", kvs("columnName")))
  override val idxs = List(pk, idxServiceIdColumnName)
  override def foreignKeys() = {
    List(
      HBaseModel.findsMatch[HColumnMeta](useCache = false)(Seq("columnId" -> kvs("id")))
    )
  }
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val serviceId = kvs("serviceId").toString.toInt
  val columnName = kvs("columnName").toString
  val columnType = kvs("columnType").toString


  lazy val service = HService.findById(serviceId)
  lazy val metas = HColumnMeta.findAllByColumn(id.get)
  lazy val metaNamesMap = (HColumnMeta.lastModifiedAtColumn :: metas).map(x => (x.seq, x.name)) toMap
  lazy val toJson = Json.obj("serviceName" -> service.serviceName, "columnName" -> columnName, "columnType" -> columnType)

}
