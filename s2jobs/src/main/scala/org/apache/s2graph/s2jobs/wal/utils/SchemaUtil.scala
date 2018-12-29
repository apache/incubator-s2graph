package org.apache.s2graph.s2jobs.wal.utils

import com.typesafe.config.Config
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.s2jobs.wal.DeserializeSchema


object SchemaUtil {

  import scalikejdbc._

  var initialized = false

  def init(config: Config): Boolean = {
    synchronized {
      if (!initialized) {
        logger.info(s"[SCHEMA]: initializing...")
        logger.info(s"[SCHEMA]: $config")
        Schema.apply(config)
        initialized = true
      }

      initialized
    }
  }

  def labelSrcColumnMap: Map[String, String] = {
    val ls = DB readOnly { implicit session =>
      sql"""SELECT label, src_column_name FROM labels""".map { rs =>
        rs.string("label") -> rs.string("src_column_name")
      }.list().apply()
    }

    ls.toMap
  }

  def parseLabelMeta(rs: WrappedResultSet): LabelMeta = {
    LabelMeta(
      rs.intOpt("id"),
      rs.int("label_id"),
      rs.string("name"),
      rs.byte("seq"),
      rs.string("default_value"),
      rs.string("data_type").toLowerCase,
      false
      //      rs.boolean("store_in_global_index")
    )
  }

  def labelMetaMap(label: Label): Map[Byte, LabelMeta] = {
    val reserved = LabelMeta.reservedMetas.map { m =>
      if (m == LabelMeta.to) m.copy(dataType = label.tgtColumnType)
      else if (m == LabelMeta.from) m.copy(dataType = label.srcColumnType)
      else m
    }

    val labelMetas = DB readOnly { implicit session =>
      sql"""SELECT * FROM label_metas WHERE label_id = ${label.id.get}""".map(parseLabelMeta).list().apply()
    }

    (reserved ++ labelMetas).groupBy(_.seq).map { case (seq, labelMetas) =>
      seq -> labelMetas.head
    }
  }

  def buildVertexDeserializeSchema(serviceNameColumnNames: Map[String, Seq[String]]): DeserializeSchema = {
    val tree = serviceNameColumnNames.flatMap { case (serviceName, columnNames) =>
      val service = Service.findByName(serviceName).getOrElse(throw new IllegalArgumentException(s"$serviceName not eixst."))

      columnNames.map { columnName =>
        val column = ServiceColumn.find(service.id.get, columnName).getOrElse(throw new IllegalArgumentException(s"$columnName not exist."))
        column -> service
      }
    }

    val columnService = tree.map { case (column, service) =>
      column.id.get -> service
    }

    val columns = tree.map { case (column, _) =>
      column.id.get -> column
    }

    val columnMetas = columns.map { case (colId, column) =>
      val innerMap = ColumnMeta.findAllByColumn(colId).map { columnMeta =>
        columnMeta.seq -> columnMeta
      }

      colId -> innerMap.toMap
    }

    new DeserializeSchema(
      columnService = columnService,
      columns = columns,
      columnMetas = columnMetas,
      labelService = Map.empty,
      labels = Map.empty,
      labelIndices = Map.empty,
      labelIndexLabelMetas = Map.empty,
      labelMetas = Map.empty
    )
  }

  def buildEdgeDeserializeSchema(labelNames: Seq[String]): DeserializeSchema = {
    val labels = labelNames.map { labelName =>
      val label = Label.findByName(labelName).getOrElse(throw new IllegalArgumentException(s"$labelName not exist."))
      label.id.get -> label
    }.toMap

    val labelService = labels.map { case (id, label) =>
      id -> label.service
    }

    val labelIndices = labels.map { case (id, label) =>
      id -> label.indicesMap
    }

    val labelMetas = labels.map { case (id, label) =>
      id -> labelMetaMap(labels(id))
    }

    val labelIndexLabelMetas = labelIndices.map { case (id, indicesMap) =>
      val metaPropsMap = labelMetas(id)
      val m = indicesMap.map  { case (idxSeq, labelIndex) =>
        idxSeq -> labelIndex.metaSeqs.flatMap(seq => metaPropsMap.get(seq)).toArray
      }

      id -> m
    }

    new DeserializeSchema(
      columnService = Map.empty,
      columns = Map.empty,
      columnMetas = Map.empty,
      labelService = labelService,
      labels = labels,
      labelIndices = labelIndices,
      labelIndexLabelMetas = labelIndexLabelMetas,
      labelMetas = labelMetas)
  }
}
