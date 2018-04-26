package org.apache.s2graph.core.model

import java.util.concurrent.atomic.AtomicInteger

trait ImportStatus {
  val done: AtomicInteger

  def isCompleted: Boolean

  def percentage: Int

  val total: Int
}

class ImportRunningStatus(val total: Int) extends ImportStatus {
  require(total > 0, s"Total should be positive: $total")

  val done = new AtomicInteger(0)

  def isCompleted: Boolean = total == done.get

  def percentage = 100 * done.get / total
}

case object ImportDoneStatus extends ImportStatus {
  val total = 1

  val done = new AtomicInteger(1)

  def isCompleted: Boolean = true

  def percentage = 100
}

object ImportStatus {
  def apply(total: Int): ImportStatus = new ImportRunningStatus(total)

  def unapply(importResult: ImportStatus): Option[(Boolean, Int, Int)] =
    Some((importResult.isCompleted, importResult.total, importResult.done.get))
}