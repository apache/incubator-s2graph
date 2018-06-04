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

package org.apache.s2graph.graphql

import com.typesafe.config.ConfigFactory
import org.apache.s2graph.graphql.types.S2Directive
import org.scalatest._

class DirectiveTest extends FunSuite with Matchers with BeforeAndAfterAll {
  var testGraph: TestGraph = _

  override def beforeAll = {
    val config = ConfigFactory.load()
    testGraph = new EmptyGraph(config)
    testGraph.open()
  }

  override def afterAll(): Unit = {
    testGraph.cleanup()
  }

  test("transform") {
    val input = "20170601_A0"
    val code =
      """ (s: String) => {
          val date = s.split("_").head
          s"http://abc.xy.com/IMG_${date}.png"
      }

      """.stripMargin
    val actual = S2Directive.resolveTransform(code, input)
    val expected = "http://abc.xy.com/IMG_20170601.png"

    actual shouldBe expected
  }
}
