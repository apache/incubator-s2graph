package kafka

import java.net.InetAddress
import java.util.{Properties, UUID}

import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerConfig, PartitionTopicInfo, TopicCount}
import kafka.utils._
import org.I0Itec.zkclient.{IZkStateListener, ZkClient}
import org.apache.zookeeper.Watcher.Event.KeeperState

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 4. 21..
 */
case class KafkaHelper(kafkaParams: Map[String, String]) extends Logging {
  lazy val props = {
    val rtn = new Properties()
    for ((k, v) <- kafkaParams) rtn.put(k, v)
    rtn
  }
  lazy val config = new ConsumerConfig(props)
  lazy val zkClient = connectZk()

  private def connectZk() = {
    info("Connecting to zookeeper instance at " + config.zkConnect)
    new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
  }

  def consumerGroupCleanup() {
    val groupDirs = new ZKGroupDirs(config.groupId)
    val dir = groupDirs.consumerGroupDir
    info(s"Cleaning up temporary Zookeeper data under $dir.")
    try {
      zkClient.deleteRecursive(dir)
    } catch {
      case e: Throwable =>
        warn("Error cleaning up temporary Zookeeper data", e)
    }
  }

  // register consumer and handle session expire
  def registerConsumerInZK(topics: Set[String]) {
    val groupDirs = new ZKGroupDirs(config.groupId)
    val topicCountMap = topics.map((_, 1)).toMap
    val topicCount = TopicCount.constructTopicCount(consumerIdString, topicCountMap)

    val stateListener = new IZkStateListener {
      override def handleNewSession(): Unit = {
        info("begin registering consumer " + consumerIdString + " in ZK")
        val timestamp = SystemTime.milliseconds.toString
        val consumerRegistrationInfo = Json.encode(Map("version" -> 1, "subscription" -> topicCount.getTopicCountMap, "pattern" -> topicCount.pattern,
          "timestamp" -> timestamp))

        ZkUtils.createEphemeralPathExpectConflictHandleZKBug(zkClient, groupDirs.consumerRegistryDir + "/" + consumerIdString, consumerRegistrationInfo, null,
          (consumerZKString, consumer) => true, config.zkSessionTimeoutMs)
        info("end registering consumer " + consumerIdString + " in ZK")
      }

      override def handleStateChanged(state: KeeperState): Unit = {}
    }

    stateListener.handleNewSession()
    zkClient.subscribeStateChanges(stateListener)
  }

  val consumerIdString = {
    val consumerUuid = config.consumerId match {
      case Some(consumerId) => // for testing only
        consumerId
      case None => // generate unique consumerId automatically
        val uuid = UUID.randomUUID()
        "%s-%d-%s".format(InetAddress.getLocalHost.getHostName, System.currentTimeMillis, uuid.getMostSignificantBits.toHexString.substring(0,8))
    }
    config.groupId + "_" + consumerUuid
  }

  def getConsumerOffsets(topics: Seq[String]): Map[TopicAndPartition, Long] = {
    for {
      (topic, partitions) <- ZkUtils.getPartitionsForTopics(zkClient, topics).toMap
      partition <- partitions
    } yield {
      val znode = new ZKGroupTopicDirs(config.groupId, topic).consumerOffsetDir + "/" + partition
      // get offset
      val offsetString = ZkUtils.readDataMaybeNull(zkClient, znode)._1
      TopicAndPartition(topic, partition) -> offsetString.map(_.toLong).getOrElse(PartitionTopicInfo.InvalidOffset)
    }
  }

  def commitConsumerOffsets(offsets: Map[TopicAndPartition, Long]) {
    for {
      (tp, offset) <- offsets
    } {
      commitConsumerOffset(tp, offset)
    }
  }

  def commitConsumerOffset(tp: TopicAndPartition, offset: Long): Unit = {
    val topicDirs = new ZKGroupTopicDirs(config.groupId, tp.topic)
    try {
      ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + tp.partition, offset.toString)
    } catch {
      case t: Throwable =>
        // log it and let it go
        warn("exception during commitOffsets",  t)
        throw t
    }
    debug("Committed offset " + offset + " for topic " + tp)
  }

  def shutdown() = {
    zkClient.close()
  }
}
