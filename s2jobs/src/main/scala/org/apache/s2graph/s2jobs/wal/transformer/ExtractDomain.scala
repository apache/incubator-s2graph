package org.apache.s2graph.s2jobs.wal.transformer

import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.utils.UrlUtils
import org.apache.s2graph.s2jobs.wal.{DimVal, WalLog}
import play.api.libs.json.Json

case class ExtractDomain(taskConf: TaskConf) extends Transformer(taskConf) {
  val urlDimensions = Json.parse(taskConf.options.getOrElse("urlDimensions", "[]")).as[Set[String]]
  val hostDimName = taskConf.options.getOrElse("hostDimName", "host")
  val domainDimName= taskConf.options.getOrElse("domainDimName", "domain")
  val keywordDimName = taskConf.options.getOrElse("keywordDimName", "uri_keywords")
  override def toDimValLs(walLog: WalLog, propertyKey: String, propertyValue: String): Seq[DimVal] = {
    if (!urlDimensions(propertyKey)) Nil
    else {
      val (_, domains, kwdOpt) = UrlUtils.extract(propertyValue)

      domains.headOption.toSeq.map(DimVal(hostDimName, _)) ++
        domains.map(DimVal(domainDimName, _)) ++
        kwdOpt.toSeq.map(DimVal(keywordDimName, _))
    }
  }
}
