//package coprocessor.generated
//
//import java.io.IOException
//import java.util
//import com.google.gson.JsonSyntaxException
//import com.google.protobuf.{ RpcCallback, RpcController, Service }
//import coprocessor.generated.GraphStatsProtos._
//import com.daumkakao.s2graph.core.HBaseElement.{ CompositeId, LabelWithDirection }
//import org.apache.hadoop.hbase._
//import org.apache.hadoop.hbase.client.{ Delete, HTableInterface, Scan }
//import org.apache.hadoop.hbase.coprocessor.{ CoprocessorException, CoprocessorService, RegionCoprocessorEnvironment }
//import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
//import org.apache.hadoop.hbase.protobuf.{ ProtobufUtil, ResponseConverter }
//import org.apache.hadoop.hbase.regionserver.InternalScanner
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.log4j.Logger
//import scala.collection.JavaConversions._
//import org.apache.hadoop.hbase.HConstants.OperationStatusCode
//
///**
// * Graph Stats(count related) Endpoint Coprocessor class
// *  - Coprocessor registration process
// *    1. copy assembled jar to hdfs
// *    2. hbase shell> alter '[Table Name]', METHOD => 'table_att', 'coprocessor' => 'hdfs://[namenode host:port]/[jar location]|kgraph.coprocessor.generated.GraphCountEndpoint|[priority]|'
// *       --> example : hbase shell> alter 'cop_test', METHOD => 'table_att', 'coprocessor' => 'hdfs:///user/junek/coprocessor/kgraph-coprocessor.jar|kgraph.coprocessor.generated.GraphCountEndpoint|101|'
// *       ref. https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/coprocessor/package-summary.html
// *  - Client implementation
// *    ref. GraphCountEndpointTest code
// *
// *
// *  ########## protocol buffer code examples start #############
// *
// *
// *     val tablePool = Executors.newFixedThreadPool(100)
// *     val connectionPool = Executors.newFixedThreadPool(10)
// *     val conf = HBaseConfiguration.create()
// *     conf.set("hbase.client.retries.number", "1")
// *     conf.set("hbase.zookeeper.quorum", "fox450.kr2.iwilab.com,dubai095.kr2.iwilab.com,dubai096.kr2.iwilab.com")
// *
// *     val tableName = "s2graph-alpha".getBytes()
// *     val conn: HConnection = HConnectionManager.createConnection(conf, connectionPool)
// *     val table:HTableInterface = conn.getTable(tableName, tablePool)
// *
// *     val argument:WarmupArgument = WarmupArgument.getDefaultInstance()
// *     val voidArgs = VoidArgument.getDefaultInstance
// *     val (minTs, maxTs) = (1421058077148l, 1421058077150l)
// *     val labelArgs = DeleteLabelsArgument.newBuilder()
// *       .setScan(
// *         ClientProtos.Scan.newBuilder()
// *           .addColumn(
// *             ClientProtos.Column.newBuilder()
// *               .setFamily(
// *                 ByteString.copyFrom(Bytes.toBytes("e"))
// *               ).build
// *           )
// *           .setTimeRange(
// *             HBaseProtos.TimeRange.newBuilder().setFrom(minTs).setTo(maxTs).build()
// *           ).build
// *       )
// *     .addId(182).buil
// *
// *     table.coprocessorService(classOf[GraphStatService], null, null,
// *       new Batch.Call[GraphStatService, Long]() {
// *         override def call(counter: GraphStatService): Long = {
// *           val  controller:ServerRpcController = new ServerRpcController();
// *           val rpcCallback: BlockingRpcCallback[WarmupResponse]  = new BlockingRpcCallback[WarmupResponse]();
// *           println(">> Start to call warmup...")
// *           counter.warmup(controller, argument, rpcCallback);
// *           val response:WarmupResponse = rpcCallback.get();
// *           println(">> ... Got response")
// *           if (controller.failedOnException()) {
// *             throw controller.getFailedOn();
// *           }
// *
// *           if (response != null && response.hasCount()) {
// *             val count = response.getCount()
// *             println(s">> Warmed up count : ${count}")
// *             count
// *           } else
// *             0L
// *         }
// *       })
// *
// *     table.coprocessorService(classOf[GraphStatService], null, null,
// *     new Batch.Call[GraphStatService, Long]() {
// *       override def call(counter: GraphStatService): Long = {
// *         val  controller:ServerRpcController = new ServerRpcController();
// *         val rpcCallback: BlockingRpcCallback[CountResponse]  = new BlockingRpcCallback[CountResponse]();
// *         println(">> Start to call cleanup...")
// *         counter.cleanUpDeleteTaggedRows(controller, voidArgs, rpcCallback);
// *         val response:CountResponse = rpcCallback.get();
// *         println(">> ... Got response")
// *         if (controller.failedOnException()) {
// *           throw controller.getFailedOn();
// *         }
// *         if (response != null && response.hasCount()) {
// *           val count = response.getCount()
// *           println(s">> cleaned up count : ${count}")
// *           count
// *         } else
// *         0L
// *       }
// *     })
// *
// *     table.coprocessorService(classOf[GraphStatService], null, null,
// *       new Batch.Call[GraphStatService, Long]() {
// *         override def call(counter: GraphStatService): Long = {
// *           val  controller:ServerRpcController = new ServerRpcController();
// *           val rpcCallback: BlockingRpcCallback[CountResponse]  = new BlockingRpcCallback[CountResponse]();
// *           println(">> Start to call label cleanup...")
// *           counter.cleanUpDeleteLabelsRows(controller, labelArgs, rpcCallback);
// *           val response:CountResponse = rpcCallback.get();
// *           println(">> ... Got response")
// *           if (controller.failedOnException()) {
// *             throw controller.getFailedOn();
// *           }
// *           if (response != null && response.hasCount()) {
// *             val count = response.getCount()
// *             println(s">> ${labelArgs.getJsonDic} Deleted count : ${count}")
// *             count
// *           } else
// *             0L
// *         }
// *       })
// *
// * ########## protocol buffer code examples end #############
// *
// * @author June.k( Junki Kim, june.k@kakao.com )
// * @since 2014. 9. 24.
// */
//
//// example source : http://search-hadoop.com/c/HBase:hbase-client/src/main/java/org/apache/hadoop/hbase/client/coprocessor/package-info.java%7C%7CHTable
//
//class GraphCountEndpoint(var env: RegionCoprocessorEnvironment) extends GraphStatsProtos.GraphStatService with Coprocessor with CoprocessorService {
//  // get log4j logger
//  val logger: Logger = Logger.getLogger(classOf[GraphCountEndpoint])
//
//  /**
//   * Empty constructor
//   */
//  def this() = {
//    this(env = null)
//  }
//
//  val threshold = 10000000
//  val deletesBatchSize = 10000
//  val writeBufferSize = 1024 * 1024 * 2
//
//  /**
//   * Vertex Column Family Name
//   */
//  val vertexCF: Array[Byte] = "v".getBytes()
//  /**
//   * Edge Column Family Name
//   */
//  val edgeCF: Array[Byte] = "e".getBytes()
//
//  private type countFunc = (Array[Byte], java.util.ArrayList[Cell]) => (Array[Byte], Long)
//  private type mutateFunc = (HTableInterface, Any, java.util.ArrayList[Cell]) => Long
//
//  private def rowCountFunc(lastRow: Array[Byte], results: java.util.ArrayList[Cell]) = {
//    var updatedLastRow = lastRow
//    var currentCount = 0L
//    for (kv <- results) {
//      val currentRow: Array[Byte] = CellUtil.cloneRow(kv)
//      if (updatedLastRow == null || !Bytes.equals(updatedLastRow, currentRow)) {
//        updatedLastRow = currentRow
//        currentCount += 1L
//      }
//    }
//    (updatedLastRow, currentCount)
//  }
//  private def deleteRow(cell: Cell) = new Delete(CellUtil.cloneRow(cell)).deleteColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), cell.getTimestamp)
//
//  private def deleteRowAll(cell: Cell) = new Delete(CellUtil.cloneRow(cell))
//
//  private def getLabelWithDir(cell: Cell) = {
//    val compositeId = CompositeId(cell.getRowArray, cell.getRowOffset, true, true)
//    LabelWithDirection(Bytes.toInt(cell.getRowArray, cell.getRowOffset + compositeId.bytesInUse, 4))
//  }
//
//  // dirty and risky
//  private def shouldIncludeLabelId(labelsToDelete: List[Int], cell: Cell) = {
//    val labelWithDir = getLabelWithDir(cell)
//    labelsToDelete.contains(labelWithDir.labelId)
//  }
//  private def shouldExcludeLabelId(labelsToDelete: List[Int], cell: Cell) = !shouldIncludeLabelId(labelsToDelete, cell)
//
//  private def iterateRows(cf: Array[Byte], controller: RpcController, request: ClientProtos.Scan, done: RpcCallback[CountResponse])(f: countFunc) = {
//    // Build scanner using sent by remote client
//    val scan: Scan = ProtobufUtil.toScan(request)
//    if (cf != null) scan.addFamily(cf)
//
//    var scanner: InternalScanner = null
//    try {
//      scanner = env.getRegion().getScanner(scan)
//      val results = new util.ArrayList[Cell]()
//
//      var hasMore: Boolean = false
//      var lastRow: Array[Byte] = null
//      var count: Long = 0
//      do {
//        hasMore = scanner.next(results)
//        val (currentRow, currentCount) = f(lastRow, results)
//        lastRow = currentRow
//        count += currentCount
//        if (count % threshold == 0) logger.info(s"$count")
//        results.clear()
//      } while (hasMore)
//
//      done.run(
//        GraphStatsProtos.CountResponse
//          .newBuilder()
//          .setCount(count)
//          .build())
//    } catch {
//      case ioe: IOException =>
//        ResponseConverter.setControllerException(controller, ioe);
//    } finally {
//      if (scanner != null) {
//        try {
//          scanner.close();
//        } catch {
//          case ignored: IOException =>
//        }
//      }
//    }
//  }
//
//  private def mutateRows(cf: Array[Byte], controller: RpcController, request: DeleteLabelsArgument, done: RpcCallback[CountResponse])(shouldDeleteFunc: (List[Int], Cell) => Boolean, howToDelete: Cell => Delete) = {
//    val scan: Scan =
//      if (request.hasScan) {
//        ProtobufUtil.toScan(request.getScan)
//      } else {
//        val sc = new Scan()
//        sc.addFamily(cf)
//        sc
//      }
//
//
//    var scanner: InternalScanner = null
//
//    val curTableName = env.getRegion.getTableDesc.getTableName
//
//    val table = env.getTable(curTableName)
//    table.setAutoFlush(false, false)
//    table.setWriteBufferSize(writeBufferSize)
//
//    try {
//      logger.debug(s"Got ids : ${request.getIdList}")
//      val labelsToDelete = request.getIdList.map(_.toInt).toList
//
//      logger.debug(s">> LabelsToDelete : $labelsToDelete")
//
//      scanner = env.getRegion().getScanner(scan)
//      val region = env.getRegion()
//      val results = new util.ArrayList[Cell]()
//
//      var hasMore: Boolean = false
//      var count: Long = 0
//      var skippedCount: Long = 0L
//      var deletesSize: Long = 0L
//      var totalDeleteCount: Long = 0L
//      val deletes = new scala.collection.mutable.Queue[Delete]
//
//      do {
//        hasMore = scanner.next(results)
//        for (kv <- results) {
//          if (shouldDeleteFunc(labelsToDelete, kv)) {
//            logger.debug(s"Do Delete")
//            deletes += howToDelete(kv)
//            deletesSize += 1
//            count += 1L
//          } else {
//            logger.debug(s"Delete skip")
//            skippedCount += 1L
//          }
//        }
//        if (deletesSize > deletesBatchSize) {
//          logger.info(s"delete [$deletesSize] key values")
//          val popped = deletes.dequeueAll(_ => true)
//          deletesSize = 0L
//
//          for (status <- region.batchMutate(popped.toArray) if status.getOperationStatusCode() == OperationStatusCode.SUCCESS) {
//            totalDeleteCount += 1L
//          }
//        }
//        if (count % threshold == 0) logger.info(s"$count")
//        results.clear()
//      } while (hasMore)
//
//      logger.info(s"delete [$deletesSize] key values")
//      val popped = deletes.dequeueAll(_ => true)
//      deletesSize = 0L
//
//      for (status <- region.batchMutate(popped.toArray) if status.getOperationStatusCode() == OperationStatusCode.SUCCESS) {
//        totalDeleteCount += 1L
//      }
//
//      done.run(
//        GraphStatsProtos.CountResponse
//          .newBuilder()
//          .setCount(totalDeleteCount)
//          .build())
//    } catch {
//      case ioe: IOException =>
//        ResponseConverter.setControllerException(controller, ioe);
//      case e: JsonSyntaxException =>
//        ResponseConverter.setControllerException(controller, new IOException(e.getMessage));
//    } finally {
//      if (scanner != null) {
//        try {
//          scanner.close();
//        } catch {
//          case ignored: IOException =>
//        }
//      }
//    }
//  }
//  override def getTotalVertexCount(controller: RpcController, request: ClientProtos.Scan, done: RpcCallback[CountResponse]): Unit = {
//    iterateRows(vertexCF, controller, request, done)(rowCountFunc)
//  }
//
//  override def getTotalEdgeCount(controller: RpcController, request: ClientProtos.Scan, done: RpcCallback[CountResponse]): Unit = {
//    iterateRows(edgeCF, controller, request, done)(rowCountFunc)
//  }
//
//  override def cleanUpDeleteLabelsRows(controller: RpcController, request: DeleteLabelsArgument, done: RpcCallback[CountResponse]): Unit = {
//    mutateRows(edgeCF, controller, request, done)(shouldIncludeLabelId, deleteRow)
//  }
//
//  override def cleanUpDeleteLabelsRowsExclude(controller: RpcController, request: DeleteLabelsArgument, done: RpcCallback[CountResponse]): Unit = {
//    mutateRows(edgeCF, controller, request, done)(shouldExcludeLabelId, deleteRowAll)
//  }
//
//
//  //  /**
//  //   * Warm up All rows for caching to BlockCache
//  //   * @param controller
//  //   * @param request
//  //   * @param done
//  //   */
//  //  override def warmup(controller: RpcController, request: ClientProtos.Scan, done: RpcCallback[CountResponse]): Unit = {
//  //    iterateRows(null, controller, request, done)(rowCountFunc)
//  //  }
//
//  override def stop(env: CoprocessorEnvironment): Unit = {
//    // nothing to do
//  }
//
//  /**
//   * Stores a reference to the coprocessor environment provided by the
//   * {@link org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost} from the region where this
//   * coprocessor is loaded.  Since this is a coprocessor endpoint, it always expects to be loaded
//   * on a table region, so always expects this to be an instance of
//   * {@link RegionCoprocessorEnvironment}.
//   * @param env the environment provided by the coprocessor host
//   * @throws IOException if the provided environment is not an instance of
//   * {@code RegionCoprocessorEnvironment}
//   */
//  override def start(env: CoprocessorEnvironment): Unit = {
//    this.env =
//      if (env.isInstanceOf[RegionCoprocessorEnvironment]) {
//        env.asInstanceOf[RegionCoprocessorEnvironment];
//      } else {
//        throw new CoprocessorException("Must be loaded on a table region!");
//      }
//  }
//
//  /**
//   * Just returns a reference to this object, which implements the GraphStatService interface.
//   */
//  override def getService: Service = {
//    this
//  }
//
//}
