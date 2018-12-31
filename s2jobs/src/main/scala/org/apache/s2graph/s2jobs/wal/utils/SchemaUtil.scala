package org.apache.s2graph.s2jobs.wal.utils

import com.typesafe.config.Config
import org.apache.s2graph.core.schema._
import org.apache.s2graph.core.utils.logger
import org.apache.s2graph.s2jobs.wal.SchemaManager


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

  def buildSchemaManager(serviceNameColumnNames: Map[String, Seq[String]],
                         labelNames: Seq[String]): SchemaManager = {
    val _serviceLs = serviceNameColumnNames.keys.toSeq.map { serviceName =>
      Service.findByName(serviceName).getOrElse(throw new IllegalArgumentException(s"$serviceName not found."))
    }
    val labelLs = labelNames.map { labelName =>
      Label.findByName(labelName).getOrElse(throw new IllegalArgumentException(s"$labelName not exist."))
    }

    val serviceLs = _serviceLs ++ labelLs.flatMap { label =>
      Seq(label.srcService, label.tgtService)
    }.distinct

    val serviceColumnLs = serviceLs.flatMap { service =>
      ServiceColumn.findByServiceId(service.id.get)
    }

    val columnMetaLs = serviceColumnLs.flatMap { column =>
      column.metas
    }

    val labelIndexLs = labelLs.flatMap { label =>
      label.indices
    }
    val labelMetaLs = labelLs.flatMap { label  =>
      label.metaProps
    }

    SchemaManager(serviceLs, serviceColumnLs, columnMetaLs, labelLs, labelIndexLs, labelMetaLs)
  }

  def buildVertexDeserializeSchema(serviceNameColumnNames: Map[String, Seq[String]]): SchemaManager = {
    buildSchemaManager(serviceNameColumnNames, Nil)
  }

  def buildEdgeDeserializeSchema(labelNames: Seq[String]): SchemaManager = {
    buildSchemaManager(Map.empty, labelNames)
  }
}
