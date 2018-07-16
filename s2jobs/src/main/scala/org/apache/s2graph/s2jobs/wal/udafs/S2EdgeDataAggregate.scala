package org.apache.s2graph.s2jobs.wal.udafs

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object S2EdgeDataAggregate {
  type Element = (Long, String, String, String)

  val emptyRow = new GenericRow(Array(-1L, "empty", "empty", "empty"))

  val elementOrd = Ordering.by[Element, Long](_._1)

  val rowOrdering = new Ordering[Row] {
    override def compare(x: Row, y: Row): Int = {
      x.getAs[Long](0).compareTo(y.getAs[Long](0))
    }
  }

  val rowOrderingDesc = new Ordering[Row] {
    override def compare(x: Row, y: Row): Int = {
      -x.getAs[Long](0).compareTo(y.getAs[Long](0))
    }
  }

  val fields = Seq(
    StructField(name = "timestamp", LongType),
    StructField(name = "to", StringType),
    StructField(name = "label", StringType),
    StructField(name = "props", StringType)
  )

  val arrayType = ArrayType(elementType = StructType(fields))

  def apply(maxNumOfEdges: Int = 1000): GroupByAgg = {
    new GroupByAgg(maxNumOfEdges)
  }

  def mergeTwoSeq[T](prev: Seq[T], cur: Seq[T], size: Int)(implicit ordering: Ordering[T]): Seq[T] = {
    import scala.collection.mutable
    val (n, m) = (cur.size, prev.size)

    var (i, j) = (0, 0)
    var idx = 0
    val arr = new mutable.ArrayBuffer[T](size)

    while (idx < size && i < n && j < m) {
      if (ordering.compare(cur(i), prev(j)) > 0) {
        arr += cur(i)
        i += 1
      } else {
        arr += prev(j)
        j += 1
      }
      idx += 1
    }
    while (idx < size && i < n) {
      arr += cur(i)
      i += 1
    }
    while (idx < size && j < m) {
      arr += prev(j)
      j += 1
    }

    arr
  }
}

class GroupByAgg(maxNumOfEdges: Int = 1000) extends UserDefinedAggregateFunction {

  import S2EdgeDataAggregate._

  implicit val ord = rowOrderingDesc

  val arrayType = ArrayType(elementType = StructType(fields))

  override def inputSchema: StructType = StructType(fields)

  override def bufferSchema: StructType = StructType(Seq(
    StructField(name = "edges", dataType = arrayType),
    StructField(name = "buffered", dataType = BooleanType)
  ))

  override def dataType: DataType = arrayType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, scala.collection.mutable.ListBuffer.empty[Element])
  }

  /* not optimized */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val element = input

    val prev = buffer.getAs[Seq[Row]](0)
    val appended = prev :+ element

    buffer.update(0, appended)
    buffer.update(1, false)
  }

  private def takeTopK(ls: Seq[Row], k: Int) = {
    val sorted = ls.sorted
    if (sorted.size <= k) sorted else sorted.take(k)
  }

  /* not optimized */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val cur = buffer2.getAs[Seq[Row]](0)
    val prev = buffer1.getAs[Seq[Row]](0)

    buffer1.update(0, takeTopK(prev ++ cur, maxNumOfEdges))
    buffer1.update(1, true)
  }

  override def evaluate(buffer: Row): Any = {
    val ls = buffer.getAs[Seq[Row]](0)
    val buffered = buffer.getAs[Boolean](1)
    if (buffered) ls
    else takeTopK(ls, maxNumOfEdges)
  }
}

class GroupByArrayAgg(maxNumOfEdges: Int = 1000) extends UserDefinedAggregateFunction {

  import S2EdgeDataAggregate._

  implicit val ord = rowOrdering

  import scala.collection.mutable

  override def inputSchema: StructType = StructType(Seq(
    StructField(name = "edges", dataType = arrayType)
  ))

  override def bufferSchema: StructType = StructType(Seq(
    StructField(name = "edges", dataType = arrayType)
  ))

  override def dataType: DataType = arrayType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer.update(0, mutable.ListBuffer.empty[Row])

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cur = input.getAs[Seq[Row]](0)
    val prev = buffer.getAs[Seq[Row]](0)
    val merged = mergeTwoSeq(cur, prev, maxNumOfEdges)

    buffer.update(0, merged)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val cur = buffer2.getAs[Seq[Row]](0)
    val prev = buffer1.getAs[Seq[Row]](0)

    buffer1.update(0, mergeTwoSeq(cur, prev, maxNumOfEdges))
  }

  override def evaluate(buffer: Row): Any = buffer.getAs[Seq[Row]](0)
}
