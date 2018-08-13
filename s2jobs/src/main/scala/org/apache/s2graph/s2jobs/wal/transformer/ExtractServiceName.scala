package org.apache.s2graph.s2jobs.wal.transformer

import org.apache.s2graph.s2jobs.wal.{DimVal, WalLog}

class ExtractServiceName(serviceDims: Set[String],
                         domainServiceMap: Map[String, String] = Map.empty,
                         serviceDimName: String = "serviceName") extends Transformer {
  override def toDimValLs(walLog: WalLog, propertyKey: String, propertyValue: String): Seq[DimVal] = {
    if (!serviceDims(propertyKey)) Nil
    else {
      val serviceName = domainServiceMap.getOrElse(propertyValue, propertyValue)

      Seq(DimVal(serviceDimName, serviceName))
    }
  }
}

