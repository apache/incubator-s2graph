package org.apache.s2graph.spark.sql.streaming

import com.typesafe.config.Config
import org.apache.s2graph.core.S2Graph

import scala.concurrent.ExecutionContext

class S2SinkContext(config: Config)(implicit ec: ExecutionContext = ExecutionContext.Implicits.global){
  println(s">>>> S2SinkContext Created...")
  private lazy val s2Graph = new S2Graph(config)
  def getGraph: S2Graph = {
    s2Graph
  }
}

object S2SinkContext {
  private var s2SinkContext:S2SinkContext = null

  def apply(config:Config):S2SinkContext = {
    if (s2SinkContext == null) {
      s2SinkContext = new S2SinkContext(config)
    }
    s2SinkContext
  }
}
