package spark.hbase

import java.io.{FileSystem, Serializable}
import java.net.InetSocketAddress
import java.util
import java.util.{Comparator, TreeSet, UUID}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hbase.fs.HFileSystem
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.io.hfile.{CacheConfig, HFileContextBuilder}
import org.apache.hadoop.hbase.regionserver.{HStore, BloomType, StoreFile}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Connection}
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.{SerializableWritable, Partitioner}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._

class HFileRDD(rdd: RDD[KeyValue]) extends Serializable {

  case class KeyFamilyQualifier(val rowKey: Array[Byte], val family: Array[Byte], val qualifier: Array[Byte])
    extends Comparable[KeyFamilyQualifier] {
    override def compareTo(o: KeyFamilyQualifier): Int = {
      var result = Bytes.compareTo(rowKey, o.rowKey)
      if (result == 0) {
        result = Bytes.compareTo(family, o.family)
        if (result == 0) result = Bytes.compareTo(qualifier, o.qualifier)
      }
      result
    }

    override def toString: String = {
      Bytes.toString(rowKey) + ":" + Bytes.toString(family) + ":" + Bytes.toString(qualifier)
    }
  }


  case class BulkLoadPartitioner(startKeys: Array[Array[Byte]]) extends Partitioner {

    override def numPartitions: Int = startKeys.length

    override def getPartition(key: Any): Int = {

      val rowKey: Array[Byte] =
        key match {
          case qualifier: KeyFamilyQualifier =>
            qualifier.rowKey
          case _ =>
            key.asInstanceOf[Array[Byte]]
        }

      val comparator: Comparator[Array[Byte]] = new Comparator[Array[Byte]] {
        override def compare(o1: Array[Byte], o2: Array[Byte]): Int = {
          Bytes.compareTo(o1, o2)
        }
      }
      val partition = util.Arrays.binarySearch(startKeys, rowKey, comparator)
      if (partition < 0) partition * -1 + -2
      else partition
    }
  }

  //  def toHBaseBulk(hbaseConf: Configuration,
  //                  tableName: String,
  //                  numFilesPerRegion: Int,
  //                  tmpPath: String,
  //                  outputPath: String,
  //                  maxMaps: Int,
  //                  maxBandWidth: Int) = {
  //    val hTable = new HTable(hbaseConf, TableName.valueOf(tableName))
  //    try {
  //
  //      val job = toHFile(hbaseConf, tableName, numFilesPerRegion, tmpPath)
  ////      loadHFile(job, new Path(tmpPath), new Path(outputPath), hTable, maxMaps, maxBandWidth)
  //    } finally {
  //      hTable.close()
  //    }
  //  }

  //  private def distCp(conf: Configuration,
  //                     srcPath: Path, tgtPath: Path,
  //                     maxMaps: Int, mapBandWidth: Int) = {
  //    val distCpOption = new DistCpOptions(List(srcPath), tgtPath)
  //    distCpOption.setMaxMaps(maxMaps)
  //    distCpOption.setMapBandwidth(mapBandWidth)
  //    distCpOption.setOverwrite(true)
  //
  //    conf.set(DistCpConstants.CONF_LABEL_BANDWIDTH_MB, s"$mapBandWidth")
  //    conf.set(DistCpConstants.CONF_LABEL_MAX_MAPS, s"$maxMaps")
  //    println(s"DistCp: $conf")
  //    val job = new DistCp(conf, distCpOption)
  ////    job.run(Array("-overwrite", "-m", maxMaps, "-bandwidth", mapBandWidth, srcPath, tgtPath).map(_.toString))
  ////    job.execute()
  //    ToolRunner.run(conf, job, Array("-overwrite", "-m", maxMaps, "-bandwidth", mapBandWidth, srcPath, tgtPath).map(_.toString))
  //  }

  //  def loadHFile(job: Job,
  //                srcPath: Path,
  //                tgtPath: Path,
  //                hTable: HTable,
  //                maxMaps: Int,
  //                maxBandWidth: Int) = {
  //    val conf = job.getConfiguration
  //    val fs = FileSystem.get(conf)
  //    val rwx = new FsPermission("777")
  //
  //    try {
  //      setRecursivePermission(srcPath)
  //      distCp(conf, srcPath, tgtPath, maxMaps, maxBandWidth)
  //
  //
  //      def setRecursivePermission(path: Path): Unit = {
  //        val listFiles = fs.listStatus(path)
  //        listFiles foreach { f =>
  //          val p = f.getPath
  //          fs.setPermission(p, rwx)
  //          if (f.isDirectory && p.getName != "_tmp") {
  //            // create a "_tmp" folder that can be used for HFile splitting, so that we can
  //            // set permissions correctly. This is a workaround for unsecured HBase. It should not
  //            // be necessary for SecureBulkLoadEndpoint (see https://issues.apache.org/jira/browse/HBASE-8495
  //            // and http://comments.gmane.org/gmane.comp.java.hadoop.hbase.user/44273)
  //            FileSystem.mkdirs(fs, new Path(p, "_tmp"), rwx)
  //            setRecursivePermission(p)
  //          }
  //        }
  //      }
  //
  //
  //      val lih = new LoadIncrementalHFiles(hTable.getConfiguration)
  //      lih.doBulkLoad(tgtPath, hTable)
  //    } finally {
  //      //      fs.deleteOnExit(tgtPath)
  //      fs.deleteOnExit(new Path(TotalOrderPartitioner.getPartitionFile(job.getConfiguration)))
  //    }
  //  }

  case class FamilyHFileWriteOptions(val compression: String,
                                val bloomType: String,
                                val blockSize: Int,
                                val dataBlockEncoding: String)


  def bulkLoad(rdd: RDD[KeyValue],
               zkQuorum: String,
               tableName: TableName,
               stagingDir: String,
               familyHFileWriteOptionsMap:
               util.Map[Array[Byte], FamilyHFileWriteOptions] =
               new util.HashMap[Array[Byte], FamilyHFileWriteOptions],
               compactionExclude: Boolean = false,
               maxSize: Long = HConstants.DEFAULT_MAX_FILE_SIZE): Unit = {

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkQuorum)

    val conn = ConnectionFactory.createConnection(conf)
    val regionLocator = conn.getRegionLocator(tableName)
    val startKeys = regionLocator.getStartKeys
    val defaultCompressionStr = conf.get("hfile.compression", Compression.Algorithm.NONE.getName)
    val defaultCompression = Compression.getCompressionAlgorithmByName(defaultCompressionStr)

    val now = System.currentTimeMillis()
    val tableNameByteArray = tableName.getName

    val familyHFileWriteOptionsMapInternal =
      new util.HashMap[ByteArrayWrapper, FamilyHFileWriteOptions]

    val entrySetIt = familyHFileWriteOptionsMap.entrySet().iterator()

    while (entrySetIt.hasNext) {
      val entry = entrySetIt.next()
      familyHFileWriteOptionsMapInternal.put(new ByteArrayWrapper(entry.getKey), entry.getValue)
    }

    /**
     * This will return a new HFile writer when requested
     *
     * @param family       column family
     * @param conf         configuration to connect to HBase
     * @param favoredNodes nodes that we would like to write too
     * @param fs           FileSystem object where we will be writing the HFiles to
     * @return WriterLength object
     */
    def getNewWriter(family: Array[Byte], conf: Configuration,
                     favoredNodes: Array[InetSocketAddress],
                     fs: FileSystem,
                     familydir: Path): WriterLength = {


      var familyOptions = familyHFileWriteOptionsMapInternal.get(new ByteArrayWrapper(family))

      if (familyOptions == null) {
        familyOptions = new FamilyHFileWriteOptions(defaultCompression.toString,
          BloomType.NONE.toString, HConstants.DEFAULT_BLOCKSIZE, DataBlockEncoding.NONE.toString)
        familyHFileWriteOptionsMapInternal.put(new ByteArrayWrapper(family), familyOptions)
      }

      val tempConf = new Configuration(conf)
      tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f)
      val contextBuilder = new HFileContextBuilder()
        .withCompression(Algorithm.valueOf(familyOptions.compression))
        .withChecksumType(HStore.getChecksumType(conf))
        .withBytesPerCheckSum(HStore.getBytesPerChecksum(conf))
        .withBlockSize(familyOptions.blockSize)
      contextBuilder.withDataBlockEncoding(DataBlockEncoding.
        valueOf(familyOptions.dataBlockEncoding))
      val hFileContext = contextBuilder.build()

      if (null == favoredNodes) {
        new WriterLength(0, new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), fs)
          .withOutputDir(familydir).withBloomType(BloomType.valueOf(familyOptions.bloomType))
          .withComparator(KeyValue.COMPARATOR).withFileContext(hFileContext).build())
      } else {
        new WriterLength(0,
          new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), new HFileSystem(fs))
            .withOutputDir(familydir).withBloomType(BloomType.valueOf(familyOptions.bloomType))
            .withComparator(KeyValue.COMPARATOR).withFileContext(hFileContext)
            .withFavoredNodes(favoredNodes).build())
      }
    }

    val regionSplitPartitioner =
      new BulkLoadPartitioner(startKeys)

    //This is where all the magic happens
    //Here we are going to do the following things
    // 1. FlapMap every row in the RDD into key column value tuples
    // 2. Then we are going to repartition sort and shuffle
    // 3. Finally we are going to write out our HFiles
    //    rdd.flatMap(r => flatMap(r))
    rdd.map(kv => new KeyFamilyQualifier(kv.getRow(), kv.getFamily(), kv.getQualifier()) -> kv.getValue())
      .repartitionAndSortWithinPartitions(regionSplitPartitioner).foreachPartition { it =>

      val config = HBaseConfiguration.create()
      config.set("hbase.zookeeper.quorum", zkQuorum)
      val fs = FileSystem.get(config)
      val writerMap = new scala.collection.mutable.HashMap[ByteArrayWrapper, WriterLength]
      var previousRow: Array[Byte] = HConstants.EMPTY_BYTE_ARRAY
      var rollOverRequested = false

      /**
       * This will roll all writers
       */
      def rollWriters(): Unit = {
        writerMap.values.foreach(wl => {
          if (wl.writer != null) {
            //            logDebug("Writer=" + wl.writer.getPath +
            //              (if (wl.written == 0) "" else ", wrote=" + wl.written))
            close(wl.writer)
          }
        })
        writerMap.clear()
        rollOverRequested = false
      }

      /**
       * This function will close a given HFile writer
       * @param w The writer to close
       */
      def close(w: StoreFile.Writer): Unit = {
        if (w != null) {
          w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
            Bytes.toBytes(System.currentTimeMillis()))
          w.appendFileInfo(StoreFile.BULKLOAD_TASK_KEY,
            Bytes.toBytes(regionSplitPartitioner.getPartition(previousRow)))
          w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
            Bytes.toBytes(true))
          w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
            Bytes.toBytes(compactionExclude))
          w.appendTrackedTimestampsToMetadata()
          w.close()
        }
      }

      //Here is where we finally iterate through the data in this partition of the
      //RDD that has been sorted and partitioned
      it.foreach { case (keyFamilyQualifier, cellValue: Array[Byte]) =>

        //This will get a writer for the column family
        //If there is no writer for a given column family then
        //it will get created here.
        val wl = writerMap.getOrElseUpdate(new ByteArrayWrapper(keyFamilyQualifier.family), {

          val familyDir = new Path(stagingDir, Bytes.toString(keyFamilyQualifier.family))

          fs.mkdirs(familyDir)

          val loc: HRegionLocation = {
            try {
              val locator =
                conn.getRegionLocator(TableName.valueOf(tableNameByteArray))
              locator.getRegionLocation(keyFamilyQualifier.rowKey)
            } catch {
              case e: Throwable =>
                //                logWarning("there's something wrong when locating rowkey: " +
                //                  Bytes.toString(keyFamilyQualifier.rowKey))
                null
            }
          }
          if (null == loc) {
            //            if (log.isTraceEnabled) {
            //              logTrace("failed to get region location, so use default writer: " +
            //                Bytes.toString(keyFamilyQualifier.rowKey))
            //            }
            getNewWriter(family = keyFamilyQualifier.family, conf = conf, favoredNodes = null,
              fs = fs, familydir = familyDir)
          } else {
            //            if (log.isDebugEnabled) {
            //              logDebug("first rowkey: [" + Bytes.toString(keyFamilyQualifier.rowKey) + "]")
            //            }
            val initialIsa =
              new InetSocketAddress(loc.getHostname, loc.getPort)
            if (initialIsa.isUnresolved) {
              //              if (log.isTraceEnabled) {
              //                logTrace("failed to resolve bind address: " + loc.getHostname + ":"
              //                  + loc.getPort + ", so use default writer")
              //              }
              getNewWriter(keyFamilyQualifier.family, conf, null, fs, familyDir)
            } else {
              //              if (log.isDebugEnabled) {
              //                logDebug("use favored nodes writer: " + initialIsa.getHostString)
              //              }
              getNewWriter(keyFamilyQualifier.family, conf,
                Array[InetSocketAddress](initialIsa), fs, familyDir)
            }
          }
        })

        val keyValue = new KeyValue(keyFamilyQualifier.rowKey,
          keyFamilyQualifier.family,
          keyFamilyQualifier.qualifier,
          now, cellValue)

        wl.writer.append(keyValue)
        wl.written += keyValue.getLength

        rollOverRequested = rollOverRequested || wl.written > maxSize

        //This will only roll if we have at least one column family file that is
        //bigger then maxSize and we have finished a given row key
        if (rollOverRequested && Bytes.compareTo(previousRow, keyFamilyQualifier.rowKey) != 0) {
          rollWriters()
        }

        previousRow = keyFamilyQualifier.rowKey
      }
      //We have finished all the data so lets close up the writers
      rollWriters()
    }
  }


  /**
   * This is a wrapper class around StoreFile.Writer.  The reason for the
   * wrapper is to keep the length of the file along side the writer
   *
   * @param written The writer to be wrapped
   * @param writer  The number of bytes written to the writer
   */
  class WriterLength(var written: Long, val writer: StoreFile.Writer)

  /**
   * This is a wrapper over a byte array so it can work as
   * a key in a hashMap
   *
   * @param o1 The Byte Array value
   */
  class ByteArrayWrapper(val o1: Array[Byte])
    extends Comparable[ByteArrayWrapper] with Serializable {
    override def compareTo(o2: ByteArrayWrapper): Int = {
      Bytes.compareTo(o1, o2.o1)
    }

    override def equals(o2: Any): Boolean = {
      o2 match {
        case wrapper: ByteArrayWrapper =>
          Bytes.equals(o1, wrapper.o1)
        case _ =>
          false
      }
    }

    override def hashCode(): Int = {
      Bytes.hashCode(o1)
    }
  }

  def toHFile(hbaseConf: Configuration, zkQuorum: String, tableName: String, numFilesPerRegion: Int, tmpPath: String): Unit = {
//    val hTable = new HTable(hbaseConf, TableName.valueOf(tableName))
//
//    val job = Job.getInstance(hbaseConf, this.getClass.getName.split('$')(0))
//    job.getConfiguration.set("hadoop.tmp.dir", s"/tmp/${
//      tableName
//    }_${
//      UUID.randomUUID()
//    }}")
//    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
//    job.setMapOutputValueClass(classOf[KeyValue])
//
//    HFileOutputFormat2.configureIncrementalLoad(job, hTable)
//    def bulkLoad(rdd: RDD[KeyValue],
//                 zkQuorum: String,
//                 tableName: TableName,
//                 stagingDir: String,
//                 familyHFileWriteOptionsMap:
//                 util.Map[Array[Byte], FamilyHFileWriteOptions] =
//                 new util.HashMap[Array[Byte], FamilyHFileWriteOptions],
//                 compactionExclude: Boolean = false,
//                 maxSize: Long = HConstants.DEFAULT_MAX_FILE_SIZE): Unit = {
//
//case class FamilyHFileWriteOptions(val compression: String,
//                                   val bloomType: String,
//                                   val blockSize: Int,
//                                   val dataBlockEncoding: String) extends Serializable

    val familyOptions = FamilyHFileWriteOptions(Algorithm.LZ4.getName, BloomType.ROW.name(), 32768, DataBlockEncoding.FAST_DIFF.name())
    val familyOptionsMap = Map("e".getBytes() -> familyOptions, "v".getBytes() -> familyOptions)
    bulkLoad(rdd, zkQuorum, TableName.valueOf(tableName), tmpPath, familyOptionsMap)
    //    val fs = FileSystem.get(hbaseConf)
    //    val hFilePath = new Path(tmpPath)
    //    fs.makeQualified(hFilePath)
//    rdd.map {
//      kv => KeyFamilyQualifier(kv.getRow, kv.getFamily, kv.getQualifier) ->(kv.getValue, kv.getTimestamp)
//    }
//      .repartitionAndSortWithinPartitions(new BulkLoadPartitioner(hTable.getStartKeys))
//      .map {
//      case (key, (value, ts)) =>
//        new ImmutableBytesWritable(key.rowKey) -> new KeyValue(key.rowKey, key.family, key.qualifier, ts, value)
//    }
//      .saveAsNewAPIHadoopFile(tmpPath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
//    job

  }
}