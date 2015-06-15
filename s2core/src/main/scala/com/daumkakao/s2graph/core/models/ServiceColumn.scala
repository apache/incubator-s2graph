package com.daumkakao.s2graph.core.models

import Model._
import com.daumkakao.s2graph.core.types2.InnerVal
import play.api.libs.json.Json

/**
 * Created by shon on 5/15/15.
 */

object ServiceColumn {
  def findById(id: Int, useCache: Boolean = true): ServiceColumn = {
    Model.find[ServiceColumn](useCache)(Seq(("id" -> id))).get
  }

  def find(serviceId: Int, columnName: String, useCache: Boolean = true): Option[ServiceColumn] = {
    Model.find[ServiceColumn](useCache)(Seq("serviceId" -> serviceId, "columnName" -> columnName))
  }

  def findsByServiceId(serviceId: Int, useCache: Boolean = true): List[ServiceColumn] = {
    Model.findsMatch[ServiceColumn](useCache)(Seq("serviceId" -> serviceId))
  }

  def findOrInsert(serviceId: Int,
                   columnName: String,
                   columnType: Option[String],
                   schemaVersion: String): ServiceColumn = {
    find(serviceId, columnName, useCache = false) match {
      case Some(s) => s
      case None =>
        val id = Model.getAndIncrSeq[ServiceColumn]
        val model = new ServiceColumn(Map("id" -> id, "serviceId" -> serviceId, "columnName" -> columnName,
          "columnType" -> columnType.getOrElse("string"),
          "schemaVersion" -> schemaVersion))
        model.create
        model
    }
  }
}

case class ServiceColumn(kvsParam: Map[KEY, VAL]) extends Model[ServiceColumn]("HServiceColumn", kvsParam) {
  override val columns = Seq("id", "serviceId", "columnName", "columnType", "schemaVersion")
  val pk = Seq(("id", kvs("id")))
  val idxServiceIdColumnName = Seq(("serviceId", kvs("serviceId")), ("columnName", kvs("columnName")))
  override val idxs = List(pk, idxServiceIdColumnName)

  override def foreignKeys() = {
    List(
      Model.findsMatch[ColumnMeta](useCache = false)(Seq("columnId" -> kvs("id")))
    )
  }

  validate(columns, Seq("schemaVersion"))

  val schemaVersion = kvs.get("schemaVersion").getOrElse(InnerVal.DEFAULT_VERSION).toString
  val id = Some(kvs("id").toString.toInt)
  val serviceId = kvs("serviceId").toString.toInt
  val columnName = kvs("columnName").toString
  val columnType = kvs("columnType").toString


  lazy val service = Service.findById(serviceId)
  lazy val metas = ColumnMeta.findAllByColumn(id.get)
  lazy val metasInvMap = metas.map { meta => meta.name -> meta } toMap
  lazy val metaNamesMap = (ColumnMeta.lastModifiedAtColumn :: metas).map(x => (x.seq.toInt, x.name)) toMap
  //  lazy val metaNamesMap = (ColumnMeta.lastModifiedAtColumn :: metas).map(x => (x.seq, x.name)) toMap
  lazy val toJson = Json.obj("serviceName" -> service.serviceName, "columnName" -> columnName, "columnType" -> columnType)

}
