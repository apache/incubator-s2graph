package org.apache.s2graph.s2jobs

import org.apache.commons.logging.{Log, LogFactory}

trait Logger {
  @transient lazy val logger: Log = LogFactory.getLog(this.getClass)
}
