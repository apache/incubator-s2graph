package org.apache.s2graph.s2jobs.wal.udafs

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.annotation.tailrec
import scala.collection.mutable

object WalLogUDAF {
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

  def apply(maxNumOfEdges: Int = 1000): GroupByAggOptimized = {
    new GroupByAggOptimized(maxNumOfEdges)
  }

  def swap[T](array: mutable.Seq[T], i: Int, j: Int) = {
    val tmp = array(i)
    array(i) = array(j)
    array(j) = tmp
  }

  @tailrec
  def percolateDown[T](array: mutable.Seq[T], idx: Int)(implicit ordering: Ordering[T]): Unit = {
    val left = 2 * idx + 1
    val right = 2 * idx + 2
    var smallest = idx

    if (left < array.size && ordering.compare(array(left), array(smallest)) < 0) {
      smallest = left
    }

    if (right < array.size && ordering.compare(array(right), array(smallest)) < 0) {
      smallest = right
    }

    if (smallest != idx) {
      swap(array, idx, smallest)
      percolateDown(array, smallest)
    }
  }

  def percolateUp[T](array: mutable.Seq[T],
                     idx: Int)(implicit ordering: Ordering[T]): Unit = {
    var pos = idx
    var parent = (pos - 1) / 2
    while (parent >= 0 && ordering.compare(array(pos), array(parent)) < 0) {
      // swap pos and parent, since a[parent] > array[pos]
      swap(array, parent, pos)
      pos = parent
      parent = (pos - 1) / 2
    }
  }

  def addToTopK[T](array: mutable.Seq[T],
                   size: Int,
                   newData: T)(implicit ordering: Ordering[T]): mutable.Seq[T] = {
    // use array as minHeap to keep track of topK.
    // parent = (i -1) / 2
    // left child = 2 * i + 1
    // right chiud = 2  * i + 2

    // check if array is already full.
    if (array.size >= size) {
      // compare newData to min. newData < array(0)
      val currentMin = array(0)
      if (ordering.compare(newData, currentMin) < 0) {
        // drop newData
      } else {
        // delete min
        array(0) = newData
        // percolate down
        percolateDown(array, 0)
      }
      array
    } else {
      // append new element into seqeunce since there are room left.
      val newArray = array :+ newData
      val idx = newArray.size - 1
      // percolate up last element
      percolateUp(newArray, idx)
      newArray
    }
  }

  def mergeTwoSeq[T](prev: Seq[T], cur: Seq[T], size: Int)(implicit ordering: Ordering[T]): Seq[T] = {
    import scala.collection.mutable
    val (n, m) = (cur.size, prev.size)

    var (i, j) = (0, 0)
    var idx = 0
    val arr = new mutable.ArrayBuffer[T](size)

    while (idx < size && i < n && j < m) {
      if (ordering.compare(cur(i), prev(j)) < 0) {
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

class GroupByAggOptimized(maxNumOfEdges: Int = 1000) extends UserDefinedAggregateFunction {

  import WalLogUDAF._

  implicit val ord = rowOrdering

  val arrayType = ArrayType(elementType = StructType(fields))

  type ROWS = mutable.Seq[Row]

  override def inputSchema: StructType = StructType(fields)

  override def bufferSchema: StructType = StructType(Seq(
    StructField(name = "edges", dataType = arrayType)
  ))

  override def dataType: DataType = arrayType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, mutable.ArrayBuffer.empty[Row])
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val prev = buffer.getAs[ROWS](0)

    val updated = addToTopK(prev, maxNumOfEdges, input)

    buffer.update(0, updated)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var prev = buffer1.getAs[ROWS](0)
    val cur = buffer2.getAs[ROWS](0)

    cur.filter(_ != null).foreach { row =>
      prev = addToTopK(prev, maxNumOfEdges, row)
    }

    buffer1.update(0, prev)
  }

  override def evaluate(buffer: Row): Any = {
    val ls = buffer.getAs[ROWS](0)
    takeTopK(ls, maxNumOfEdges)
  }

  private def takeTopK(ls: Seq[Row], k: Int) = {
    val sorted = ls.sorted
    if (sorted.size <= k) sorted else sorted.take(k)
  }
}

class GroupByAgg(maxNumOfEdges: Int = 1000) extends UserDefinedAggregateFunction {
  import WalLogUDAF._

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
  import WalLogUDAF._

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

    val merged = mergeTwoSeq(cur, prev, maxNumOfEdges)
    buffer1.update(0, merged)
  }

  override def evaluate(buffer: Row): Any = buffer.getAs[Seq[Row]](0)
}
