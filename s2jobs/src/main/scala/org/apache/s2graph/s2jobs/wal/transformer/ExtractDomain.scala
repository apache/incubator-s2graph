package org.apache.s2graph.s2jobs.wal.transformer

import org.apache.s2graph.s2jobs.wal.utils.UrlUtils
import org.apache.s2graph.s2jobs.wal.{DimVal, WalLog}

case class ExtractDomain(urlDimensions: Set[String],
                         hostDimName: String = "host",
                         domainDimName: String = "domain",
                         keywordDimName: String = "uri_keywords") extends Transformer {
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
