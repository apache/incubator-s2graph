package org.apache.s2graph.spark.sql.streaming

case class S2SinkStatus(
                         taskId: Int,
                         execTimeMillis: Long,
                         successCnt:Long,
                         failCnt:Long,
                         counter:Map[String, Int]
                       )
