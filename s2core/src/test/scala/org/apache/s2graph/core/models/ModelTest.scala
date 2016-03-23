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

package org.apache.s2graph.core.models

import org.apache.s2graph.core.{CommonTest, TestCommonWithModels}
import org.apache.s2graph.core.mysqls.Label
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class ModelTest extends FunSuite with Matchers with TestCommonWithModels with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    versions map { v =>
      val ver = s"v$v"
      initTests(ver)
    }
  }

  override def afterAll(): Unit = {
    graph.shutdown()
  }

  versions map { n =>
    val ver = s"v$n"
    val label = labelName(ver)
    test(s"test Label.findByName $ver", CommonTest) {
      val labelOpt = Label.findByName(label, useCache = false)
      println(labelOpt)
      labelOpt.isDefined shouldBe true
      val indices = labelOpt.get.indices
      indices.size > 0 shouldBe true
      println(indices)
      val defaultIndexOpt = labelOpt.get.defaultIndex
      println(defaultIndexOpt)
      defaultIndexOpt.isDefined shouldBe true
      val metas = labelOpt.get.metaProps
      println(metas)
      metas.size > 0 shouldBe true
      val srcService = labelOpt.get.srcService
      println(srcService)
      val tgtService = labelOpt.get.tgtService
      println(tgtService)
      val service = labelOpt.get.service
      println(service)
      val srcColumn = labelOpt.get.srcService
      println(srcColumn)
      val tgtColumn = labelOpt.get.tgtService
      println(tgtColumn)
    }
  }
}
