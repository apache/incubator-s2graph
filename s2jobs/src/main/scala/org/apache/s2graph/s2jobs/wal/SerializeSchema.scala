package org.apache.s2graph.s2jobs.wal

import org.apache.s2graph.core.schema._

class DeserializeSchema(columnService: Map[Int, Service],
                        columns: Map[Int, ServiceColumn],
                        columnMetas: Map[Int, Map[Byte, ColumnMeta]],
                        labelService: Map[Int, Service],
                        labels: Map[Int, Label],
                        labelIndices: Map[Int, Map[Byte, LabelIndex]],
                        labelIndexLabelMetas: Map[Int, Map[Byte, Array[LabelMeta]]],
                        labelMetas: Map[Int, Map[Byte, LabelMeta]]) {
  def findColumnService(columnId: Int): Service = {
    columnService(columnId)
  }

  def findServiceColumn(columnId: Int): ServiceColumn = {
    columns(columnId)
  }

  def findServiceColumnMeta(columnId: Int, metaSeq: Byte): ColumnMeta = {
    columnMetas(columnId)(metaSeq)
  }

  def findLabel(labelId: Int): Label = {
    labels(labelId)
  }

  def findLabelService(labelId: Int): Service = {
    labelService(labelId)
  }

  def findLabelIndex(labelId: Int, indexSeq: Byte): LabelIndex = {
    labelIndices(labelId)(indexSeq)
  }

  def findLabelIndexLabelMetas(labelId: Int, indexSeq: Byte): Array[LabelMeta] = {
    labelIndexLabelMetas(labelId)(indexSeq)
  }

  def findLabelMetas(labelId: Int): Map[Byte, LabelMeta] = {
    labelMetas(labelId)
  }

  def findLabelMeta(labelId: Int, metaSeq: Byte): LabelMeta = {
    labelMetas(labelId)(metaSeq)
  }

  def checkLabelExist(labelId: Int): Boolean = {
    labels.contains(labelId)
  }

  def checkServiceColumnExist(columnId: Int): Boolean = {
    columns.contains(columnId)
  }
}

class SerializeSchema(services: Map[String, Service],
                      serviceColumns: Map[String, Map[String, ServiceColumn]],
                      serviceColumnSet: Map[String, Set[ServiceColumn]],
                      serviceColumnMetas: Map[String, Map[String, Map[String, ColumnMeta]]],
                      labelService: Map[String, Service],
                      labels: Map[String, Label],
                      labelIndices: Map[String, Map[String, LabelIndex]],
                      labelIndexLabelMetas: Map[String, Map[String, Array[LabelMeta]]],
                      labelMetas: Map[String, Map[String, LabelMeta]]) extends Serializable {
  def findService(serviceName: String): Service = {
    services(serviceName)
  }

  def findServiceColumns(serviceName: String): Set[ServiceColumn] = {
    serviceColumnSet(serviceName)
  }

  def findServiceColumn(serviceName: String, columnName: String): ServiceColumn = {
    serviceColumns(serviceName)(columnName)
  }

  def findServiceColumnMeta(serviceName: String, columnName: String, metaName: String): ColumnMeta = {
    serviceColumnMetas(serviceName)(columnName)(metaName)
  }

  def findLabel(labelName: String): Label = {
    labels(labelName)
  }

  def findLabelService(labelName: String): Service = {
    labelService(labelName)
  }

  def findLabelIndex(labelName: String, indexName: String): LabelIndex = {
    labelIndices(labelName)(indexName)
  }

  def findLabelIndexLabelMetas(labelName: String, indexName: String): Array[LabelMeta] = {
    labelIndexLabelMetas(labelName)(indexName)
  }

  def findLabelMetas(labelName: String): Map[String, LabelMeta] = {
    labelMetas(labelName)
  }

  def findLabelMeta(labelName: String, name: String): LabelMeta = {
    labelMetas(labelName)(name)
  }

  def checkLabelExist(labelName: String) = {
    labels.contains(labelName)
  }
}
