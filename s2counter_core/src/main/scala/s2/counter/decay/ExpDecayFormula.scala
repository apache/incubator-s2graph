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

package s2.counter.decay

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 26..
 */
case class ExpDecayFormula(halfLifeInMillis: Double) extends DecayFormula {
  val decayRate = - Math.log(2) / halfLifeInMillis

  override def apply(value: Double, millis: Long): Double = {
    value * Math.pow(Math.E, decayRate * millis)
  }

  override def apply(value: Double, seconds: Int): Double = {
    apply(value, seconds * 1000L)
  }
}

object ExpDecayFormula {
  @deprecated("do not use. just experimental", "0.14")
  def byWindowTime(windowInMillis: Long, pct: Double): ExpDecayFormula = {
    val halfLife = windowInMillis * Math.log(0.5) / Math.log(pct)
    ExpDecayFormula(halfLife)
  }

  def byMeanLifeTime(millis: Long): ExpDecayFormula = {
    ExpDecayFormula(millis * Math.log(2))
  }
}
