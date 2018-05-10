package org.apache.s2graph.core.fetcher.fasttext

import java.nio.{ByteBuffer, ByteOrder}
import java.util

import org.rocksdb.{ColumnFamilyDescriptor, ColumnFamilyHandle, DBOptions, RocksDB}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class Line(labels: Array[Int], words: Array[Long])

case class Entry(wid: Int, count: Long, tpe: Byte, subwords: Array[Long])

object FastText {
  val EOS = "</s>"
  val BOW = "<"
  val EOW = ">"

  val FASTTEXT_VERSION = 12 // Version 1b
  val FASTTEXT_FILEFORMAT_MAGIC_INT32 = 793712314

  val MODEL_CBOW = 1
  val MODEL_SG = 2
  val MODEL_SUP = 3

  val LOSS_HS = 1
  val LOSS_NS = 2
  val LOSS_SOFTMAX = 3

  val DBPathKey = "dbPath"

  def tokenize(in: String): Array[String] = in.split("\\s+") ++ Array("</s>")

  def getSubwords(word: String, minn: Int, maxn: Int): Array[String] = {
    val l = math.max(minn, 1)
    val u = math.min(maxn, word.length)
    val r = l to u flatMap word.sliding
    r.filterNot(s => s == BOW || s == EOW).toArray
  }

  def hash(str: String): Long = {
    var h = 2166136261L.toInt
    for (b <- str.getBytes) {
      h = (h ^ b) * 16777619
    }
    h & 0xffffffffL
  }

}

class FastText(name: String) extends AutoCloseable {

  import FastText._

  private val dbOptions = new DBOptions()
  private val descriptors = new java.util.LinkedList[ColumnFamilyDescriptor]()
  descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY))
  descriptors.add(new ColumnFamilyDescriptor("vocab".getBytes()))
  descriptors.add(new ColumnFamilyDescriptor("i".getBytes()))
  descriptors.add(new ColumnFamilyDescriptor("o".getBytes()))
  private val handles = new util.LinkedList[ColumnFamilyHandle]()
  private val db = RocksDB.openReadOnly(dbOptions, name, descriptors, handles)

  private val defaultHandle = handles.get(0)
  private val vocabHandle = handles.get(1)
  private val inputVectorHandle = handles.get(2)
  private val outputVectorHandle = handles.get(3)

  private val args = FastTextArgs.fromByteArray(db.get(defaultHandle, "args".getBytes("UTF-8")))
  private val wo = loadOutputVectors()
  private val labels = loadLabels()

  println(args)

  require(args.magic == FASTTEXT_FILEFORMAT_MAGIC_INT32)
  require(args.version == FASTTEXT_VERSION)

  // only sup/softmax supported
  // others are the future work.
  require(args.model == MODEL_SUP)
  require(args.loss == LOSS_SOFTMAX)

  private def getVector(handle: ColumnFamilyHandle, key: Long): Array[Float] = {
    val keyBytes = ByteBuffer.allocate(8).putLong(key).array()
    val bb = ByteBuffer.wrap(db.get(handle, keyBytes)).order(ByteOrder.LITTLE_ENDIAN)
    Array.fill(args.dim)(bb.getFloat)
  }

  private def loadOutputVectors(): Array[Array[Float]] =
    Array.tabulate(args.nlabels)(key => getVector(outputVectorHandle, key.toLong))

  private def loadLabels(): Array[String] = {
    val result = new Array[String](args.nlabels)
    val it = db.newIterator(defaultHandle)
    var i = 0
    it.seekToFirst()
    while (it.isValid) {
      val key = ByteBuffer.wrap(it.key()).getInt()
      if (key < args.nlabels) {
        require(i == key)
        result(i) = new String(it.value(), "UTF-8")
        i += 1
      }
      it.next()
    }
    result
  }

  def getInputVector(key: Long): Array[Float] = getVector(inputVectorHandle, key)

  def getOutputVector(key: Long): Array[Float] = getVector(outputVectorHandle, key)

  def getEntry(word: String): Entry = {
    val raw = db.get(vocabHandle, word.getBytes("UTF-8"))
    if (raw == null) {
      Entry(-1, 0L, 1, Array.emptyLongArray)
    } else {
      val bb = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN)
      val wid = bb.getInt
      val count = bb.getLong
      val tpe = bb.get
      val subwords = if (word != EOS && tpe == 0) Array(wid.toLong) ++ computeSubwords(BOW + word + EOW) else Array(wid.toLong)
      Entry(wid, count, tpe, subwords)
    }
  }

  def computeSubwords(word: String): Array[Long] =
    getSubwords(word, args.minn, args.maxn).map { w => args.nwords + (hash(w) % args.bucket.toLong) }

  def getLine(in: String): Line = {
    val tokens = tokenize(in)
    val words = new ArrayBuffer[Long]()
    val labels = new ArrayBuffer[Int]()
    tokens foreach { token =>
      val Entry(wid, count, tpe, subwords) = getEntry(token)
      if (tpe == 0) {
        // addSubwords
        if (wid < 0) { // OOV
          if (token != EOS) {
            words ++= computeSubwords(BOW + token + EOW)
          }
        } else {
          words ++= subwords
        }
      } else if (tpe == 1 && wid > 0) {
        labels += wid - args.nwords
      }
    }
    Line(labels.toArray, words.toArray)
  }

  def computeHidden(input: Array[Long]): Array[Float] = {
    val hidden = new Array[Float](args.dim)
    for (row <- input.map(getInputVector)) {
      var i = 0
      while (i < hidden.length) {
        hidden(i) += row(i) / input.length
        i += 1
      }
    }
    hidden
  }

  def predict(line: Line, k: Int = 1): Array[(String, Float)] = {
    val hidden = computeHidden(line.words)
    val output = wo.map { o =>
      o.zip(hidden).map(a => a._1 * a._2).sum
    }
    val max = output.max
    var i = 0
    var z = 0.0f
    while (i < output.length) {
      output(i) = math.exp((output(i) - max).toDouble).toFloat
      z += output(i)
      i += 1
    }
    i = 0
    while (i < output.length) {
      output(i) /= z
      i += 1
    }
    output.zipWithIndex.sortBy(-_._1).take(k).map { case (prob, i) =>
      labels(i) -> prob
    }
  }

  def close(): Unit = {
    handles.asScala.foreach(_.close())
    dbOptions.close()
    db.close()
  }

}