package org.apache.s2graph.core.fetcher.fasttext

import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.util

import org.apache.s2graph.core.fetcher.fasttext
import org.rocksdb._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object CopyModel {

  def writeArgs(db: RocksDB, handle: ColumnFamilyHandle, args: fasttext.FastTextArgs): Unit = {
    val wo = new WriteOptions().setDisableWAL(true).setSync(false)
    db.put(handle, wo, "args".getBytes("UTF-8"), args.serialize)
    wo.close()
    println("done                                                      ")
  }

  def writeVocab(is: InputStream, db: RocksDB,
                 vocabHandle: ColumnFamilyHandle, labelHandle: ColumnFamilyHandle, args: fasttext.FastTextArgs): Unit = {
    val wo = new WriteOptions().setDisableWAL(true).setSync(false)
    val bb = ByteBuffer.allocate(13).order(ByteOrder.LITTLE_ENDIAN)
    val wb = new ArrayBuffer[Byte]
    for (wid <- 0 until args.size) {
      bb.clear()
      wb.clear()
      var b = is.read()
      while (b != 0) {
        wb += b.toByte
        b = is.read()
      }
      bb.putInt(wid)
      is.read(bb.array(), 4, 9)
      db.put(vocabHandle, wo, wb.toArray, bb.array())

      if (bb.get(12) == 1) {
        val label = wid - args.nwords
        db.put(labelHandle, ByteBuffer.allocate(4).putInt(label).array(), wb.toArray)
      }

      if ((wid + 1) % 1000 == 0)
        print(f"\rprocessing ${100 * (wid + 1) / args.size.toFloat}%.2f%%")
    }
    println("\rdone                                                      ")
    wo.close()
  }

  def writeVectors(is: InputStream, db: RocksDB, handle: ColumnFamilyHandle, args: fasttext.FastTextArgs): Unit = {
    require(is.read() == 0, "not implemented")
    val wo = new WriteOptions().setDisableWAL(true).setSync(false)
    val bb = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN)
    val key = ByteBuffer.allocate(8)
    val value = new Array[Byte](args.dim * 4)
    is.read(bb.array())
    val m = bb.getLong
    val n = bb.getLong
    require(n * 4 == value.length)
    var i = 0L
    while (i < m) {
      key.clear()
      key.putLong(i)
      is.read(value)
      db.put(handle, wo, key.array(), value)
      if ((i + 1) % 1000 == 0)
        print(f"\rprocessing ${100 * (i + 1) / m.toFloat}%.2f%%")
      i += 1
    }
    println("\rdone                                                      ")
    wo.close()
  }

  def printHelp(): Unit = {
    println("usage: CopyModel <in> <out>")
  }

  def copy(in: String, out: String): Unit = {
    RocksDB.destroyDB(out, new Options)

    val dbOptions = new DBOptions()
      .setCreateIfMissing(true)
      .setCreateMissingColumnFamilies(true)
      .setAllowMmapReads(false)
      .setMaxOpenFiles(500000)
      .setDbWriteBufferSize(134217728)
      .setMaxBackgroundCompactions(20)

    val descriptors = new java.util.LinkedList[ColumnFamilyDescriptor]()
    descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY))
    descriptors.add(new ColumnFamilyDescriptor("vocab".getBytes()))
    descriptors.add(new ColumnFamilyDescriptor("i".getBytes()))
    descriptors.add(new ColumnFamilyDescriptor("o".getBytes()))
    val handles = new util.LinkedList[ColumnFamilyHandle]()
    val db = RocksDB.open(dbOptions, out, descriptors, handles)

    val is = new BufferedInputStream(new FileInputStream(in))
    val fastTextArgs = FastTextArgs.fromInputStream(is)

    require(fastTextArgs.magic == FastText.FASTTEXT_FILEFORMAT_MAGIC_INT32)
    require(fastTextArgs.version == FastText.FASTTEXT_VERSION)

    println("step 1: writing args")
    writeArgs(db, handles.get(0), fastTextArgs)
    println("step 2: writing vocab")
    writeVocab(is, db, handles.get(1), handles.get(0), fastTextArgs)
    println("step 3: writing input vectors")
    writeVectors(is, db, handles.get(2), fastTextArgs)
    println("step 4: writing output vectors")
    writeVectors(is, db, handles.get(3), fastTextArgs)
    println("step 5: compactRange")
    db.compactRange()
    println("done")

    handles.asScala.foreach(_.close())
    db.close()
    is.close()
  }

}