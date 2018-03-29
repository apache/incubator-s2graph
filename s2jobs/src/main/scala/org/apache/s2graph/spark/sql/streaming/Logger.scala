package org.apache.s2graph.spark.sql.streaming

import org.apache.commons.logging.{Log, LogFactory}

trait Logger {
  @transient lazy val logger: Log = LogFactory.getLog(this.getClass)
}
