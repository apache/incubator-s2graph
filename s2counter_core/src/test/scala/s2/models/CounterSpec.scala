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

package s2.models

import org.specs2.mutable.Specification
import s2.models.Counter.ItemType

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 11..
 */
class CounterSpec extends Specification {
  "Counter" should {
    "dimension auto combination" in {
      val policy = Counter(
        useFlag = true,
        2,
        "test",
        "test_case",
        ItemType.LONG,
        autoComb = true,
        "p1,p2,p3",
        useProfile = false,
        None,
        useRank = true,
        0,
        None,
        None,
        None,
        None,
        None,
        None
      )

      policy.dimensionSp mustEqual Array("p1", "p2", "p3")
      policy.dimensionList.map { arr => arr.toSeq }.toSet -- Set(Seq.empty[String], Seq("p1"), Seq("p2"), Seq("p3"), Seq("p1", "p2"), Seq("p1", "p3"), Seq("p2", "p3"), Seq("p1", "p2", "p3")) must beEmpty
    }
  }
}
