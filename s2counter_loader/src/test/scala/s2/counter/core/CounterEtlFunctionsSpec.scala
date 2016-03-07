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

import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, BeforeAndAfterAll, FlatSpec}
import s2.models.DBModel

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 7. 3..
 */
class CounterEtlFunctionsSpec extends FlatSpec with BeforeAndAfterAll with Matchers {
  override def beforeAll: Unit = {
    DBModel.initialize(ConfigFactory.load())
  }

  "CounterEtlFunctions" should "parsing log" in {
    val data =
      """
        |1435107139287	insert	e	aaPHfITGUU0B_150212123559509	abcd	test_case	{"cateid":"100110102","shopid":"1","brandid":""}
        |1435106916136	insert	e	Tgc00-wtjp2B_140918153515441	efgh	test_case	{"cateid":"101104107","shopid":"2","brandid":""}
      """.stripMargin.trim.split('\n')
    val items = {
      for {
        line <- data
        item <- CounterEtlFunctions.parseEdgeFormat(line)
      } yield {
        item.action should equal("test_case")
        item
      }
    }

    items should have size 2
  }
}
