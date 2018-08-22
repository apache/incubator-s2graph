package org.apache.s2graph.s2jobs.wal.transformer

import org.apache.s2graph.core.JSONParser
import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.{DimVal, WalLog}
import play.api.libs.json.{JsObject, Json}

class ExtractServiceName(taskConf: TaskConf) extends Transformer(taskConf) {
  val serviceDims = Json.parse(taskConf.options.getOrElse("serviceDims", "[]")).as[Set[String]]
  val domainServiceMap = Json.parse(taskConf.options.getOrElse("domainServiceMap", "{}")).as[JsObject].fields.map { case (k, v) =>
      k -> JSONParser.jsValueToString(v)
  }.toMap
  val serviceDimName = taskConf.options.getOrElse("serviceDimName", "serviceDimName")

  override def toDimValLs(walLog: WalLog, propertyKey: String, propertyValue: String): Seq[DimVal] = {
    if (!serviceDims(propertyKey)) Nil
    else {
      val serviceName = domainServiceMap.getOrElse(propertyValue, propertyValue)

      Seq(DimVal(serviceDimName, serviceName))
    }
  }
}

