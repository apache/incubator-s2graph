//package org.apache.s2graph.s2jobs.wal.udfs
//
//import java.nio.ByteBuffer
//
//import com.typesafe.config.ConfigFactory
//import org.apache.hadoop.hbase.client.Result
//import org.apache.s2graph.core.Management
//import org.apache.s2graph.core.types.HBaseType
//import org.apache.s2graph.s2jobs.udfs.Udf
//import org.apache.s2graph.s2jobs.wal.LabelSchema
//import org.apache.s2graph.s2jobs.wal.utils.{DeserializeUtil, SchemaUtil}
//import org.apache.spark.serializer.{KryoSerializer, SerializerInstance}
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.sql.functions.udf
//
//
//object HFileParserUDF {
//
//  import org.apache.s2graph.s2jobs.wal.utils.DeserializeUtil._
//  import org.apache.s2graph.core.storage.SKeyValue
//
////  def resultParser(serDe: SerializerInstance,
////                   labelSchema: LabelSchema,
////                   tallSchemaVersion: Set[String]) = {
////    udf((row: Array[Byte], resultBytes: Array[Byte]) => {
////      val result = serDe.deserialize[Result](ByteBuffer.wrap(resultBytes))
////      DeserializeUtil.resultToWals(row, result, labelSchema, tallSchemaVersion)
////    })
////  }
//
////  def walParser(labelSchema: LabelSchema,
////                tallSchemaVersions: Set[String]) = {
////    udf((row: Array[Byte], kvsRow: Seq[Row]) => {
////      DeserializeUtil.indexEdgeResultToWalsV3IterTest(row, kvsRow, labelSchema, tallSchemaVersions)
////    })
////  }
//
////  def indexEdgeParser(labelSchema: LabelSchema,
////                      tallSchemaVersions: Set[String]) = {
////    udf((row: Row) => {
////      val skv = sKeyValueFromRow(row)
////
////      indexEdgeKeyValueToRow(skv, None, labelSchema, tallSchemaVersions)
////    })
////  }
////
////  def snapshotEdgeParser(labelSchema: LabelSchema) = {
////    udf((row: Row) => {
////      val skv = sKeyValueFromRow(row)
////      snapshotEdgeKeyValueToRow(skv, None, labelSchema)
////    })
////  }
//}
//
//class HFileParserUDF extends Udf {
//
//  import HFileParserUDF._
//  import org.apache.s2graph.s2jobs.wal.utils.TaskConfUtil._
//
//  override def register(ss: SparkSession, name: String, options: Map[String, String]): Unit = {
//    val mergedConfig = Management.toConfig(parseMetaStoreConfigs(options))
//    val config = ConfigFactory.load(mergedConfig)
//    SchemaUtil.init(config)
//
////    val elementType = options.getOrElse("elementType", "indexedge")
//    val labelNames = options.get("labelNames").map(_.split(",").toSeq).getOrElse(Nil)
//
//    val labelSchema = SchemaUtil.buildLabelSchema(labelNames)
//
//    val tallSchemaVersions = Set(HBaseType.VERSION4)
//
////    val f = elementType.toLowerCase match {
////      case "indexedge" => indexEdgeParser(labelSchema, tallSchemaVersions)
////      case "snapshotedge" => snapshotEdgeParser(labelSchema)
////      case _ => throw new IllegalArgumentException(s"$elementType is not supported.")
////    }
//    val f = walParser(labelSchema, tallSchemaVersions)
////    val serDe = new KryoSerializer(ss.sparkContext.getConf).newInstance()
////    val f = resultParser(serDe, labelSchema, tallSchemaVersions)
//
//    ss.udf.register(name, f)
//  }
//}