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

package org.apache.s2graph.rest.play.benchmark

import play.api.test.{FakeApplication, PlaySpecification, WithApplication}

import scala.annotation.tailrec
import scala.util.Random

class SamplingBenchmarkSpec extends BenchmarkCommon with PlaySpecification {
  "sample" should {
    implicit val app = FakeApplication()

    "sample benchmark" in new WithApplication(app) {
      @tailrec
      def randomInt(n: Int, range: Int, set: Set[Int] = Set.empty[Int]): Set[Int] = {
        if (set.size == n) set
        else randomInt(n, range, set + Random.nextInt(range))
      }

      // sample using random array
      def randomArraySample[T](num: Int, ls: List[T]): List[T] = {
        val randomNum = randomInt(num, ls.size)
        var sample = List.empty[T]
        var idx = 0
        ls.foreach { e =>
          if (randomNum.contains(idx)) sample = e :: sample
          idx += 1
        }
        sample
      }

      // sample using shuffle
      def shuffleSample[T](num: Int, ls: List[T]): List[T] = {
        Random.shuffle(ls).take(num)
      }

      // sample using random number generation
      def rngSample[T](num: Int, ls: List[T]): List[T] = {
        var sampled = List.empty[T]
        val N = ls.size // population
        var t = 0 // total input records dealt with
        var m = 0 // number of items selected so far

        while (m < num) {
          val u = Random.nextDouble()
          if ( (N - t)*u < num - m) {
            sampled = ls(t) :: sampled
            m += 1
          }
          t += 1
        }
        sampled
      }

      // test data
      val testLimit = 500000
      val testNum = 10
      val testData = (0 to 1000).toList

      // dummy for warm-up
      (0 to testLimit) foreach { n =>
        randomArraySample(testNum, testData)
        shuffleSample(testNum, testData)
        rngSample(testNum, testData)
      }

      duration("Random Array Sampling") {
        (0 to testLimit) foreach { _ =>
          val sampled = randomArraySample(testNum, testData)
        }
      }

      duration("Shuffle Sampling") {
        (0 to testLimit) foreach { _ =>
          val sampled = shuffleSample(testNum, testData)
        }
      }

      duration("RNG Sampling") {
        (0 to testLimit) foreach { _ =>
          val sampled = rngSample(testNum, testData)
        }
      }
    }


  }
}
