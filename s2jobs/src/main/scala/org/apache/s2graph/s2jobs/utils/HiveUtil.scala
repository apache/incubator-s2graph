package org.apache.s2graph.s2jobs.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType

object HiveUtil {

  def getTableModificationTime(ss: SparkSession,
                          table: String,
                          dataBase: String): Option[Long] = {
    val sessionCatalog = ss.sessionState.catalog
    val catalogTable = sessionCatalog.getTableMetadata(TableIdentifier(table = table, database = Some(dataBase)))
    catalogTable.properties.get("transient_lastDdlTime").map(_.toLong * 1000)
  }

  def getTableSchema(ss: SparkSession,
                     table: String,
                     dataBase: String): StructType = {
    ss.sql(s"SELECT * FROM $dataBase.$table LIMIT 1").schema
  }

}
