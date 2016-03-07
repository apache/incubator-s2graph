/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package s2.counter.core

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 8..
 */
case class TimedQualifier(q: TimedQualifier.IntervalUnit.Value, ts: Long) {
  import TimedQualifier.IntervalUnit._

  def dateTime: Long = {
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    dateFormat.format(ts).toLong
  }

  def add(amount: Int): TimedQualifier = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(ts)
    q match {
      case MINUTELY =>
        cal.add(Calendar.MINUTE, amount)
      case HOURLY =>
        cal.add(Calendar.HOUR, amount)
      case DAILY =>
        cal.add(Calendar.DAY_OF_MONTH, amount)
      case MONTHLY =>
        cal.add(Calendar.MONTH, amount)
      case TOTAL =>
    }
    copy(ts = cal.getTimeInMillis)
  }
}

object TimedQualifier {
  object IntervalUnit extends Enumeration {
    type IntervalUnit = Value
    val TOTAL = Value("t")
    val MONTHLY = Value("M")
    val DAILY = Value("d")
    val HOURLY = Value("H")
    val MINUTELY = Value("m")
  }

  def apply(q: String, ts: Long): TimedQualifier = TimedQualifier(IntervalUnit.withName(q), ts)

  import IntervalUnit._

  def getTsUnit(intervalUnit: IntervalUnit.IntervalUnit): Long = {
    intervalUnit match {
      case MINUTELY => 1 * 60 * 1000l
      case HOURLY => 60 * 60 * 1000l
      case DAILY => 24 * 60 * 60 * 1000l
      case MONTHLY => 31 * 24 * 60 * 60 * 1000l
      case v: IntervalUnit.IntervalUnit =>
        throw new RuntimeException(s"unsupported operation for ${v.toString}")
    }
  }

  def getQualifiers(intervals: Seq[IntervalUnit], millis: Long): Seq[TimedQualifier] = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(millis)

    val newCal = Calendar.getInstance()
    newCal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), 1, 0, 0, 0)
    newCal.set(Calendar.MILLISECOND, 0)
    val month = newCal.getTimeInMillis
    val Seq(day, hour, minute) = {
      for {
        field <- Seq(Calendar.DATE, Calendar.HOUR_OF_DAY, Calendar.MINUTE)
      } yield {
        newCal.set(field, cal.get(field))
        newCal.getTimeInMillis
      }
    }

    for {
      interval <- intervals
    } yield {
      val ts = interval match {
        case MINUTELY => minute
        case HOURLY => hour
        case DAILY => day
        case MONTHLY => month
        case TOTAL => 0L
      }
      TimedQualifier(interval, ts)
    }
  }

  // descending order
  def getQualifiersToLimit(intervals: Seq[IntervalUnit], limit: Int, tsOpt: Option[Long] = None): Seq[TimedQualifier] = {
    val ts = tsOpt.getOrElse(System.currentTimeMillis())
    for {
      interval <- intervals
      newLimit = if (interval == TOTAL) 1 else limit
      i <- 0 until (-newLimit, -1)
    } yield {
      val newMillis = nextTime(interval, ts, i)
      TimedQualifier(interval, newMillis)
    }
  }

  private def nextTime(interval: IntervalUnit, ts: Long, i: Int): Long = {
    val newCal = Calendar.getInstance()
    newCal.setTimeInMillis(ts)
    newCal.set(Calendar.MILLISECOND, 0)
    interval match {
      case MINUTELY =>
        newCal.set(Calendar.SECOND, 0)
        newCal.add(Calendar.MINUTE, i)
        newCal.getTimeInMillis
      case HOURLY =>
        newCal.set(Calendar.SECOND, 0)
        newCal.set(Calendar.MINUTE, 0)
        newCal.add(Calendar.HOUR_OF_DAY, i)
        newCal.getTimeInMillis
      case DAILY =>
        newCal.set(Calendar.SECOND, 0)
        newCal.set(Calendar.MINUTE, 0)
        newCal.set(Calendar.HOUR_OF_DAY, 0)
        newCal.add(Calendar.DAY_OF_MONTH, i)
        newCal.getTimeInMillis
      case MONTHLY =>
        newCal.set(Calendar.SECOND, 0)
        newCal.set(Calendar.MINUTE, 0)
        newCal.set(Calendar.HOUR_OF_DAY, 0)
        newCal.set(Calendar.DAY_OF_MONTH, 1)
        newCal.add(Calendar.MONTH, i)
        newCal.getTimeInMillis
      case TOTAL =>
        0L
    }
  }

  def getTimeList(interval: IntervalUnit, from: Long, to: Long, rst: List[Long] = Nil): List[Long] = {
    interval match {
      case TOTAL => List(0)
      case _ =>
        val next = nextTime(interval, from, 1)
        if (next < from) {
          // ignore
          getTimeList(interval, next, to, rst)
        }
        else if (next < to) {
          // recall
          getTimeList(interval, next, to, rst :+ next)
        } else {
          // end condition
          rst :+ next
        }
    }
  }

  // for reader
  def getQualifiersToLimit(intervals: Seq[IntervalUnit],
                           limit: Int,
                           optFrom: Option[Long],
                           optTo: Option[Long]): Seq[List[TimedQualifier]] = {
    val newLimit = limit - 1
    for {
      interval <- intervals
    } yield {
      {
        (optFrom, optTo) match {
          case (Some(from), Some(to)) =>
            getTimeList(interval, from, to)
          case (Some(from), None) =>
            getTimeList(interval, from, nextTime(interval, from, newLimit))
          case (None, Some(to)) =>
            getTimeList(interval, nextTime(interval, to, -newLimit), to)
          case (None, None) =>
            val current = System.currentTimeMillis()
            getTimeList(interval, nextTime(interval, current, -newLimit), current)
        }
      }.map { ts =>
        TimedQualifier(interval, ts)
      }
    }
  }

  def getTimeRange(intervals: Seq[IntervalUnit],
                   limit: Int,
                   optFrom: Option[Long],
                   optTo: Option[Long]): Seq[(TimedQualifier, TimedQualifier)] = {
    val newLimit = limit - 1
    val maxInterval = intervals.maxBy {
      case MINUTELY => 0
      case HOURLY => 1
      case DAILY => 2
      case MONTHLY => 3
      case TOTAL => 4
    }
    val minInterval = intervals.minBy {
      case MINUTELY => 0
      case HOURLY => 1
      case DAILY => 2
      case MONTHLY => 3
      case TOTAL => 4
    }
    val (from, to) = (optFrom, optTo) match {
      case (Some(f), Some(t)) =>
        (f, t)
      case (Some(f), None) =>
        (f, nextTime(minInterval, f, newLimit))
      case (None, Some(t)) =>
        (nextTime(maxInterval, t, -newLimit), t)
      case (None, None) =>
        val current = System.currentTimeMillis()
        (nextTime(maxInterval, current, -newLimit), nextTime(minInterval, current, 0))
    }
    for {
      interval <- intervals
    } yield {
      (TimedQualifier(interval, from), TimedQualifier(interval, to))
    }
  }
}
