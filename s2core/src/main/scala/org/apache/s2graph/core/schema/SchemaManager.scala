package org.apache.s2graph.core.schema


trait SchemaManager {
  def findColumnService(columnId: Int): Service

  def findServiceColumn(columnId: Int): ServiceColumn

  def findServiceColumnMeta(columnId: Int, metaSeq: Byte): ColumnMeta

  def findLabel(labelId: Int): Label

  def findLabelService(labelId: Int): Service

  def findLabelIndex(labelId: Int, indexSeq: Byte): LabelIndex

  def findLabelIndexLabelMetas(labelId: Int, indexSeq: Byte): Array[LabelMeta]

  def findLabelMetas(labelId: Int): Map[Byte, LabelMeta]

  def findLabelMeta(labelId: Int, metaSeq: Byte): LabelMeta

  def checkLabelExist(labelId: Int): Boolean

  def checkServiceColumnExist(columnId: Int): Boolean

  def findServiceColumn(serviceName: String, columnName: String): ServiceColumn

  def findColumnMetas(serviceName: String, columnName: String): Map[String, ColumnMeta]

  def findColumnMetas(column: ServiceColumn): Map[String, ColumnMeta]

  def findLabel(labelName: String): Label

  def findLabelService(labelName: String): Service

  def findLabelIndices(label: Label): Map[String, LabelIndex]

  def findLabelIndices(labelName: String): Map[String, LabelIndex]

  def findLabelIndex(labelName: String, indexName: String): LabelIndex

  def findLabelIndexLabelMetas(labelName: String, indexName: String): Array[LabelMeta]

  def findLabelMetas(labelName: String): Map[String, LabelMeta]

  def findLabelMeta(labelName: String, name: String): LabelMeta

  def checkLabelExist(labelName: String): Boolean

  def findSrcServiceColumn(label: Label): ServiceColumn

  def findTgtServiceColumn(label: Label): ServiceColumn

  def findSrcServiceColumn(labelName: String): ServiceColumn

  def findTgtServiceColumn(labelName: String): ServiceColumn
}

