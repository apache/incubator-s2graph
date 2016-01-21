package org.apache.s2graph.lambda

import org.slf4j.LoggerFactory

trait Logging {
  lazy val logger = LoggerFactory.getLogger(getClass.getSimpleName)
}


