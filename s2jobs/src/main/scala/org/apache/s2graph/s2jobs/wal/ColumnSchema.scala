package org.apache.s2graph.s2jobs.wal

import org.apache.s2graph.core.schema._

case class ColumnSchema(columnService: Map[Int, Service],
                        columns: Map[Int, ServiceColumn],
                        columnMetas: Map[Int, Map[Int, ColumnMeta]]) {

  val serviceMap: Map[String, Service] = columnService.map(t => t._2.serviceName -> t._2)
  val serviceColumnMap: Map[(String, String), ServiceColumn] = {
    columns.map { case (colId, column) =>
      val key = columnService(colId).serviceName -> column.columnName

      key -> column
    }
  }
  val serviceColumnList: Map[String, Set[ServiceColumn]] = columnService.groupBy(_._2).map { case (service, ls) =>
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

  def findService(serviceName: String): Service = {
    serviceMap(serviceName)
  }

  def findServiceColumns(serviceName: String): Set[ServiceColumn] = {
    serviceColumnList(serviceName)
  }

  def findServiceColumn(serviceName: String, columnName: String): ServiceColumn = {
    serviceColumnMap(serviceName -> columnName)
  }

  def findServiceColumnMeta(serviceName: String, columnName: String, name: String): ColumnMeta = {
    serviceColumnMetas(serviceName -> columnName)(name)
  }
}
