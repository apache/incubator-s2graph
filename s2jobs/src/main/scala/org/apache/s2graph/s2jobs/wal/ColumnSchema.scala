package org.apache.s2graph.s2jobs.wal

import org.apache.s2graph.core.schema._

case class ColumnSchema(columnService: Map[Int, Service],
                        columns: Map[Int, ServiceColumn],
                        columnMetas: Map[Int, Map[Int, ColumnMeta]]) {

  val services: Set[Service] = columnService.map(_._2).toSet
  val serviceColumns: Map[String, Set[ServiceColumn]] = columnService.groupBy(_._2).map { case (service, ls) =>
    val columnIds = ls.map(_._1)

    service.serviceName -> columnIds.map(columns).toSet
  }
  val serviceColumnMetas: Map[(String, String), Map[String, ColumnMeta]] = columnMetas.map { case (columId, m) =>
    val service = columnService(columId)
    val columnName = columns(columId).columnName

    val rev = m.map { case (seq, columnMeta) =>
      columnMeta.name -> columnMeta
    }

    (service.serviceName, columnName) -> rev
  }

}
