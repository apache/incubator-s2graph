package org.apache.s2graph.s2jobs.wal.utils

import com.typesafe.config.Config
import org.apache.s2graph.core.schema.{Label, LabelMeta, Schema}
import org.apache.s2graph.core.utils.logger


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

  def buildSchemaForLabels(labelNames: Seq[String]) = {
    val labels = labelNames.map { labelName =>
      val label = Label.findByName(labelName).getOrElse(throw new IllegalArgumentException(s"$labelName not exist."))
      label.id.get -> label
    }.toMap

    val labelServices = labels.map { case (id, label) =>
      id -> label.serviceName
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

    (labelServices, labels, labelIndices, labelMetas, labelIndexLabelMetas)
  }
}
