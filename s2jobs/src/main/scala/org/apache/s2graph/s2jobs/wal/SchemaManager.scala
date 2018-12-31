package org.apache.s2graph.s2jobs.wal

import org.apache.s2graph.core.schema._

class SchemaManager(serviceLs: Seq[Service],
                    serviceColumnLs: Seq[ServiceColumn],
                    columnMetaLs: Seq[ColumnMeta],
                    labelLs: Seq[Label],
                    labelIndexLs: Seq[LabelIndex],
                    labelMetaLs: Seq[LabelMeta]) {
  private val services = serviceLs.map { service => service.serviceName -> service }.toMap
  private val servicesInverted = serviceLs.map { service => service.id.get -> service }.toMap

  private val columnServiceMap: Map[ServiceColumn, Service] = serviceColumnLs.flatMap { column =>
    servicesInverted.get(column.serviceId).map(column -> _)
  }.toMap

//  private val columnService = columnServiceMap.map(t => t._1.columnName -> t._2)
  private val columnServiceInverted = columnServiceMap.map(t => t._1.id.get -> t._2)

//  private val columns = columnServiceMap.map(t => t._1.columnName -> t._1)
  private val columnsInverted = columnServiceMap.map(t => t._1.id.get -> t._1)

  private val columnMetasMap: Map[ServiceColumn, Seq[ColumnMeta]] =
    columnMetaLs
      .groupBy(_.columnId)
      .flatMap { case (columnId, metas) =>
        columnsInverted.get(columnId).map(_  -> (metas ++ ColumnMeta.reservedMetas))
      }

  private val columnMetas = columnMetasMap.map { case (column, metas) =>
    val innerMap = metas.map { meta =>
      meta.name -> meta
    }.toMap

    column -> innerMap
  }

  private val columnMetasInverted = columnMetasMap.map { case (column, metas) =>
    val innerMap = metas.map { meta =>
      meta.seq -> meta
    }.toMap

    column.id.get -> innerMap
  }

  private val labelsMap = labelLs.flatMap { label =>
    services.get(label.serviceName).map(label -> _)
  }.toMap

  private val labelService = labelsMap.map(t => t._1.label -> t._2)
  private val labelServiceInverted = labelsMap.map(t => t._1.id.get -> t._2)

  private val labels = labelsMap.map(t => t._1.label -> t._1)
  private val labelsInverted = labelsMap.map(t => t._1.id.get -> t._1)

  private val labelMetasMap: Map[Label, Seq[LabelMeta]] =
    labelMetaLs
    .groupBy(_.labelId)
    .flatMap { case (labelId, metas) =>
      labelsInverted.get(labelId).map(_ -> (metas ++ LabelMeta.reservedMetas))
    }

  private val labelMetas = labelMetasMap.map { case (label, metas) =>
    val innerMap = metas.map { meta =>
      meta.name -> meta
    }.toMap

    label.label -> innerMap
  }
  private val labelMetasInverted = labelMetasMap.map { case (label, metas) =>
    val innerMap = metas.map { meta =>
      meta.seq -> meta
    }.toMap

    label.id.get -> innerMap
  }

  private val labelIndicesMap = labelIndexLs
    .groupBy(_.labelId)
    .flatMap { case (labelId, indices) =>
      labelsInverted.get(labelId).map(_ -> indices)
    }

  private val labelIndices = labelIndicesMap.map { case (label, indices) =>
    val innerMap = indices.map { index =>
      index.name -> index
    }.toMap

    label.label -> innerMap
  }

  private val labelIndicesInverted = labelIndicesMap.map { case (label, indices) =>
    val innerMap = indices.map { index =>
      index.seq -> index
    }.toMap

    label.id.get -> innerMap
  }

  private val labelIndexLabelMetas = labelIndicesMap.map { case (label, indices) =>
    val innerMap = indices.map { index =>
      val metas = index.metaSeqs.map { seq =>
        labelMetasInverted(label.id.get)(seq)
      }.toArray

      index.name -> metas
    }.toMap

    label.label -> innerMap
  }

  private val labelIndexLabelMetasInverted = labelIndicesMap.map { case (label, indices) =>
    val innerMap = indices.map { index =>
      val metas = index.metaSeqs.map { seq =>
        labelMetasInverted(label.id.get)(seq)
      }.toArray

      index.seq -> metas
    }.toMap

    label.id.get -> innerMap
  }

  private val serviceIdColumnNameColumn = columnServiceMap.map { case (column, service) =>
    val key = service.id.get -> column.columnName
    key -> column
  }

  private val serviceNameColumnNameColumn = columnServiceMap.map { case (column, service) =>
    val key = service.serviceName -> column.columnName
    key -> column
  }

  def findColumnService(columnId: Int): Service = {
    columnServiceInverted(columnId)
  }

  def findServiceColumn(columnId: Int): ServiceColumn = {
    columnsInverted(columnId)
  }

  def findServiceColumnMeta(columnId: Int, metaSeq: Byte): ColumnMeta = {
    columnMetasInverted(columnId)(metaSeq)
  }

  def findLabel(labelId: Int): Label = {
    labelsInverted(labelId)
  }

  def findLabelService(labelId: Int): Service = {
    labelServiceInverted(labelId)
  }

  def findLabelIndex(labelId: Int, indexSeq: Byte): LabelIndex = {
    labelIndicesInverted(labelId)(indexSeq)
  }

  def findLabelIndexLabelMetas(labelId: Int, indexSeq: Byte): Array[LabelMeta] = {
    labelIndexLabelMetasInverted(labelId)(indexSeq)
  }

  def findLabelMetas(labelId: Int): Map[Byte, LabelMeta] = {
    labelMetasInverted(labelId)
  }

  def findLabelMeta(labelId: Int, metaSeq: Byte): LabelMeta = {
    labelMetasInverted(labelId)(metaSeq)
  }

  def checkLabelExist(labelId: Int): Boolean = {
    labelsInverted.contains(labelId)
  }

  def checkServiceColumnExist(columnId: Int): Boolean = {
    columnsInverted.contains(columnId)
  }

  def findServiceColumn(serviceName: String, columnName: String): ServiceColumn = {
    serviceNameColumnNameColumn(serviceName -> columnName)
  }

  def findColumnMetas(serviceName: String, columnName: String): Map[String, ColumnMeta] = {
    val column = findServiceColumn(serviceName, columnName)
    findColumnMetas(column)
  }

  def findColumnMetas(column: ServiceColumn): Map[String, ColumnMeta] = {
    columnMetas(column)
  }

  def findLabel(labelName: String): Label = {
    labels(labelName)
  }

  def findLabelService(labelName: String): Service = {
    labelService(labelName)
  }

  def findLabelIndices(label: Label): Map[String, LabelIndex] = {
    findLabelIndices(label.label)
  }

  def findLabelIndices(labelName: String): Map[String, LabelIndex] = {
    labelIndices(labelName)
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

  def checkLabelExist(labelName: String): Boolean = {
    labels.contains(labelName)
  }

  def findSrcServiceColumn(label: Label): ServiceColumn = {
    serviceIdColumnNameColumn(label.srcServiceId -> label.srcColumnName)
  }

  def findTgtServiceColumn(label: Label): ServiceColumn = {
    serviceIdColumnNameColumn(label.tgtServiceId -> label.tgtColumnName)
  }

  def findSrcServiceColumn(labelName: String): ServiceColumn = {
    val label = findLabel(labelName)
    serviceIdColumnNameColumn(label.srcServiceId -> label.srcColumnName)
  }

  def findTgtServiceColumn(labelName: String): ServiceColumn = {
    val label = findLabel(labelName)
    serviceIdColumnNameColumn(label.tgtServiceId -> label.tgtColumnName)
  }
}

object SchemaManager {
  def apply(serviceLs: Seq[Service],
            serviceColumnLs: Seq[ServiceColumn],
            columnMetaLs: Seq[ColumnMeta],
            labelLs: Seq[Label],
            labelIndexLs: Seq[LabelIndex],
            labelMetaLs: Seq[LabelMeta]): SchemaManager = {
    new SchemaManager(serviceLs, serviceColumnLs, columnMetaLs, labelLs, labelIndexLs, labelMetaLs)
  }
}
