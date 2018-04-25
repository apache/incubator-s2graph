//package org.apache.s2graph.core.utils
//
//import java.util.Properties
//
//import io.reactivex._
//import io.reactivex.schedulers.Schedulers
//import org.apache.kafka.clients.consumer.KafkaConsumer
//import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
//import org.apache.s2graph.core.ExceptionHandler
//
//
//trait Sync[VALUE] {
//  type KV = (String, VALUE)
//
//  val EmptyValue: VALUE
//  val SyncId: String
//  val SkipSelfBroadcast: Boolean
//
//  val delim = "___" // ! do not change
//
//  // public
//  val emitter = io.reactivex.processors.PublishProcessor.create[KV]()
//
//  def send(key: String): Unit = send(key, EmptyValue)
//
//  def send(key: String, value: VALUE): Unit = sendImpl(encodeKey(key), value)
//
//  def shutdown(): Unit = {
//    emitter.onComplete()
//  }
//
//  protected def emit(kv: KV): Unit = {
//    val (encodedKey, value) = kv
//    val (sid, key) = decodeKey(encodedKey)
//
//    if (SkipSelfBroadcast && sid == SyncId) {
//      logger.syncInfo(s"[Message ignore] sent by self: ${SyncId}")
//      // pass
//    } else {
//      emitter.onNext(key -> value)
//      logger.syncInfo(s"[Message emit success]: selfId: ${SyncId}, from: ${sid}")
//    }
//  }
//
//  protected def sendImpl(key: String, value: VALUE): Unit
//
//  private def encodeKey(key: String): String = s"${SyncId}${delim}${key}"
//
//  private def decodeKey(key: String): (String, String) = {
//    val Array(cid, decodedKey) = key.split(delim)
//    cid -> decodedKey
//  }
//}
//
//class MemorySync(syncId: String) extends Sync[Array[Byte]] {
//
//  override val EmptyValue: Array[Byte] = Array.empty[Byte]
//
//  override val SyncId: String = syncId
//
//  override val SkipSelfBroadcast: Boolean = false
//
//  override protected def sendImpl(key: String, value: Array[Byte]): Unit = emit(key -> value)
//}
//
//object KafkaSync {
//
//  import java.util.Properties
//
//  val keySerializer = "org.apache.kafka.common.serialization.StringSerializer"
//  val valueSerializer = "org.apache.kafka.common.serialization.ByteArraySerializer"
//  val keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"
//  val valueDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
//
//  val maxMessageSize = "15728640" // 15 MB
//
//  def producerConfig(brokers: String): Properties = {
//    val props = ExceptionHandler.toKafkaProp(brokers)
//
//    props.setProperty("key.serializer", keySerializer)
//    props.setProperty("value.serializer", valueSerializer)
//    props.setProperty("message.max.bytes", maxMessageSize)
//    props.setProperty("max.request.size", maxMessageSize)
//
//
//    props
//  }
//
//  def consumerConfig(brokers: String, groupId: String): Properties = {
//    val props = new Properties()
//
//    props.put("bootstrap.servers", brokers)
//    props.put("group.id", groupId)
//    props.put("enable.auto.commit", "false")
//    props.put("key.deserializer", keyDeserializer)
//    props.put("value.deserializer", valueDeserializer)
//    props.put("max.partition.fetch.bytes", maxMessageSize)
//    props.put("fetch.message.max.bytes", maxMessageSize)
//
//    props
//  }
//}
//
//class KafkaSync(topic: String,
//                syncId: String,
//                producerConfig: Properties,
//                consumerConfig: Properties,
//                skipSelfBroadcast: Boolean = true,
//                seekTo: String = "end"
//               ) extends Sync[Array[Byte]] {
//
//  type VALUE = Array[Byte]
//
//  val consumerTimeout: Int = 1000
//
//  override val EmptyValue = Array.empty[Byte]
//
//  override val SyncId = syncId
//
//  override val SkipSelfBroadcast: Boolean = skipSelfBroadcast
//
//  lazy val producer = new KafkaProducer[String, VALUE](producerConfig)
//
//  lazy val consumer = {
//    import scala.collection.JavaConverters._
//
//    val kc = new KafkaConsumer[String, VALUE](consumerConfig)
//    kc.subscribe(Seq(topic).asJava)
//    kc.poll(consumerTimeout * 10) // Just for meta info sync
//
//    if (seekTo == "end") {
//      kc.seekToEnd(kc.assignment())
//    } else if (seekTo == "beginning") {
//      kc.seekToBeginning(kc.assignment())
//    } else {
//      // pass
//    }
//
//    kc
//  }
//
//  // Emit event from kafka consumer: Flowable is just for while loop, not for Observabl
//  Flowable.create(new FlowableOnSubscribe[KV] {
//    import scala.collection.JavaConverters._
//
//    override def subscribe(e: FlowableEmitter[KV]) = {
//      while (true) {
//        val ls = consumer.poll(consumerTimeout).asScala.map(record => record.key() -> record.value())
//        ls.foreach(emit)
//
//        if (ls.nonEmpty) {
//          consumer.commitSync()
//          logger.syncInfo(s"[Kafka consume success]: message size: ${ls.size}]")
//        }
//      }
//
//      e.onComplete()
//    }
//  }, BackpressureStrategy.BUFFER).subscribeOn(Schedulers.single()).subscribe()
//
//  override protected def sendImpl(key: String, value: VALUE): Unit = {
//    val record = new ProducerRecord[String, VALUE](topic, key, value)
//
//    producer.send(record, new Callback {
//      override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
//        if (exception != null) {
//          logger.syncInfo(s"[Kafka produce failed]: from: key: ${key} e: ${exception}")
//        } else {
//          logger.syncInfo(s"[Kafka produce success]: from: ${SyncId} ${metadata}")
//        }
//      }
//    })
//  }
//
//  override def shutdown(): Unit = {
//    super.shutdown()
//
//    producer.close()
//    consumer.close()
//  }
//}
