package org.apache.s2graph.s2jobs.wal.transformer

import org.apache.s2graph.s2jobs.task.TaskConf
import org.apache.s2graph.s2jobs.wal.{DimVal, WalLog}

/**
  * decide how to transform walLog's each property key value to Seq[DimVal]
  */
abstract class Transformer(taskConf: TaskConf) extends Serializable {
  def toDimValLs(walLog: WalLog, propertyKey: String, propertyValue: String): Seq[DimVal] = {
    val dim = s"${walLog.label}:${propertyKey}"
    val value = propertyValue

    Seq(DimVal(dim, value))
  }
}
