package org.apache.s2graph.spark.spark

import org.apache.spark.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverError, StreamingListenerReceiverStarted, StreamingListenerReceiverStopped}

class SubscriberListener(ssc: StreamingContext) extends StreamingListener with Logging {
  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
    logInfo("onReceiverError")
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    logInfo("onReceiverStarted")
  }

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
    logInfo("onReceiverStopped")
    ssc.stop()
  }
}
