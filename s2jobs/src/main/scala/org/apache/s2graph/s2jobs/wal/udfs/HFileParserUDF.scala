package org.apache.s2graph.s2jobs.wal.udfs

import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.Management
import org.apache.s2graph.core.schema.{Label, LabelIndex, LabelMeta}
import org.apache.s2graph.core.types.HBaseType
import org.apache.s2graph.s2jobs.udfs.Udf
import org.apache.s2graph.s2jobs.wal.utils.SchemaUtil
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.udf


object HFileParserUDF {

  import org.apache.s2graph.s2jobs.wal.utils.DeserializeUtil._

  def indexEdgeParser(labelServices: Map[Int, String],
                      labels: Map[Int, Label],
                      labelIndices: Map[Int, Map[Byte, LabelIndex]],
                      labelMetas: Map[Int, Map[Byte, LabelMeta]],
                      labelIndexLabelMetas: Map[Int, Map[Byte, Array[LabelMeta]]],
                      tallSchemaVersions: Set[String]) = {
    udf((row: Row) => {
      val skv = sKeyValueFromRow(row)

      indexEdgeKeyValueToRow(skv, None, labelServices, labels, labelIndices, labelMetas, labelIndexLabelMetas, tallSchemaVersions)
    })
  }

  def snapshotEdgeParser(labelServices: Map[Int, String],
                         labels: Map[Int, Label],
                         labelMetas: Map[Int, Map[Byte, LabelMeta]]) = {
    udf((row: Row) => {
      val skv = sKeyValueFromRow(row)
      snapshotEdgeKeyValueToRow(skv, None, labelServices, labels, labelMetas)
    })
  }
}

class HFileParserUDF extends Udf {

  import HFileParserUDF._
  import org.apache.s2graph.s2jobs.wal.utils.TaskConfUtil._

  override def register(ss: SparkSession, name: String, options: Map[String, String]): Unit = {
    val mergedConfig = Management.toConfig(parseMetaStoreConfigs(options))
    val config = ConfigFactory.load(mergedConfig)
    SchemaUtil.init(config)

    val elementType = options.getOrElse("elementType", "indexedge")
    val labelNames = options.get("labelNames").map(_.split(",").toSeq).getOrElse(Nil)

    val (labelServices, labels, labelIndices, labelMetas, labelIndexLabelMetas) = SchemaUtil.buildSchemaForLabels(labelNames)

    //    logger.error(s"LabelServices: $labelServices")
    //    logger.error(s"Labels: $labels")
    //    logger.error(s"LabelIndices: $labelIndices")
    //    logger.error(s"LabelMetas: $labelMetas")
    val tallSchemaVersions = Set(HBaseType.VERSION4)

    val f = elementType.toLowerCase match {
      case "indexedge" => indexEdgeParser(labelServices, labels, labelIndices, labelMetas, labelIndexLabelMetas, tallSchemaVersions)
      case "snapshotedge" => snapshotEdgeParser(labelServices, labels, labelMetas)
      case _ => throw new IllegalArgumentException(s"$elementType is not supported.")
    }

    ss.udf.register(name, f)
  }
}