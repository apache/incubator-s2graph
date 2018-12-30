package org.apache.s2graph.s2jobs.wal.utils

import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.client.Result
import org.apache.s2graph.core.schema._
import org.apache.s2graph.s2jobs.wal.{SchemaManager, WalLog}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class DeserializeUtilTest extends FunSuite with Matchers with BeforeAndAfterAll  {
  def initTestSchemaManager(serviceName: String,
                     labelName: String) = {
    val service = Service(Option(1), serviceName, "token", "cluster", "test", 0, None)
    val serviceLs = Seq(service)
    val serviceColumn = ServiceColumn(Option(1), service.id.get, "user", "string", "v3", None)
    val serviceColumnLs = Seq(serviceColumn)
    val columnMeta = ColumnMeta(Option(1), serviceColumn.id.get, "age", 1, "integer", "-1")
    val columnMetaLs = Seq(columnMeta)
    val label = Label(Option(1), labelName,
      service.id.get, serviceColumn.columnName, serviceColumn.columnType,
      service.id.get, serviceColumn.columnName, serviceColumn.columnType,
      true, service.serviceName, service.id.get, "strong",
      "test", None, "v3", false, "gz", None
    )
    val labelLs = Seq(label)
    val labelMeta = LabelMeta(Option(1), label.id.get, "score", 1, "0.0", "double")
    val labelMetaLs = Seq(labelMeta)
    val labelIndex = LabelIndex(Option(1), label.id.get, "pk", 1, Seq(labelMeta.seq), "", None, None)
    val labelIndexLs = Seq(labelIndex)

    SchemaManager(serviceLs,
      serviceColumnLs,
      columnMetaLs,
      labelLs,
      labelIndexLs,
      labelMetaLs
    )
  }

  test("test serialize deserialize") {
    val tallSchemaVersions = Set("v4")
    val schema = initTestSchemaManager("s1", "l1")
    val walLog =
      WalLog(10L,"insert","e","a","x","s1","l1","""{"age": 20, "gender": "M"}""")

    val kvs = SerializeUtil.walToIndexEdgeKeyValue(walLog, schema, tallSchemaVersions)

    val cells = kvs.map { kv =>
      CellUtil.createCell(kv.row, kv.cf, kv.qualifier, kv.timestamp, Type.Put.getCode, kv.value)
    }

    val result = Result.create(cells.toArray)
    val wals = DeserializeUtil.indexEdgeResultToWals(result, schema, tallSchemaVersions)
    wals.foreach(println)
  }
}
