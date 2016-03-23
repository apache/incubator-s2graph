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

package org.apache.s2graph.core

import org.apache.s2graph.core.OrderingUtil._
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.JsString

class OrderingUtilTest extends FunSuite with Matchers {
  test("test SeqMultiOrdering", CommonTest) {
    val jsLs: Seq[Seq[Any]] = Seq(
      Seq(0, "a"),
      Seq(0, "b"),
      Seq(1, "a"),
      Seq(1, "b"),
      Seq(2, "c")
    )

    // number descending, string ascending
    val sortedJsLs: Seq[Seq[Any]] = Seq(
      Seq(2, "c"),
      Seq(1, "a"),
      Seq(1, "b"),
      Seq(0, "a"),
      Seq(0, "b")
    )

    val ascendingLs: Seq[Boolean] = Seq(false, true)
    val resultJsLs = jsLs.sorted(new SeqMultiOrdering[Any](ascendingLs))

    resultJsLs.toString() should equal(sortedJsLs.toString())
  }

  test("test tuple 1 TupleMultiOrdering", CommonTest) {
    val jsLs: Seq[(Any, Any, Any, Any)] = Seq(
      (0, None, None, None),
      (0, None, None, None),
      (1, None, None, None),
      (1, None, None, None),
      (2, None, None, None)
    )

    val sortedJsLs: Seq[(Any, Any, Any, Any)] = Seq(
      (2, None, None, None),
      (1, None, None, None),
      (1, None, None, None),
      (0, None, None, None),
      (0, None, None, None)
    )

    val ascendingLs: Seq[Boolean] = Seq(false)
    val resultJsLs = jsLs.sorted(TupleMultiOrdering[Any](ascendingLs))

    resultJsLs.toString() should equal(sortedJsLs.toString())
  }

  test("test tuple 2 TupleMultiOrdering", CommonTest) {
    val jsLs: Seq[(Any, Any, Any, Any)] = Seq(
      (0, "a", None, None),
      (0, "b", None, None),
      (1, "a", None, None),
      (1, "b", None, None),
      (2, "c", None, None)
    )

    // number descending, string ascending
    val sortedJsLs: Seq[(Any, Any, Any, Any)] = Seq(
      (2, "c", None, None),
      (1, "a", None, None),
      (1, "b", None, None),
      (0, "a", None, None),
      (0, "b", None, None)
    )

    val ascendingLs: Seq[Boolean] = Seq(false, true)
    val resultJsLs = jsLs.sorted(TupleMultiOrdering[Any](ascendingLs))

    resultJsLs.toString() should equal(sortedJsLs.toString())
  }

  test("test tuple 3 TupleMultiOrdering", CommonTest) {
    val jsLs: Seq[(Any, Any, Any, Any)] = Seq(
      (0, "a", 0l, None),
      (0, "a", 1l, None),
      (0, "b", 0l, None),
      (1, "a", 0l, None),
      (1, "b", 0l, None),
      (2, "c", 0l, None)
    )

    val sortedJsLs: Seq[(Any, Any, Any, Any)] = Seq(
      (0, "a", 1l, None),
      (0, "a", 0l, None),
      (0, "b", 0l, None),
      (1, "a", 0l, None),
      (1, "b", 0l, None),
      (2, "c", 0l, None)
    )

    val ascendingLs: Seq[Boolean] = Seq(true, true, false)
    val resultJsLs = jsLs.sorted(TupleMultiOrdering[Any](ascendingLs))

    resultJsLs.toString() should equal(sortedJsLs.toString())
  }

  test("test tuple 4 TupleMultiOrdering", CommonTest) {
    val jsLs: Seq[(Any, Any, Any, Any)] = Seq(
      (0, "a", 0l, JsString("a")),
      (0, "a", 0l, JsString("b")),
      (0, "a", 1l, JsString("a")),
      (0, "b", 0l, JsString("b")),
      (1, "a", 0l, JsString("b")),
      (1, "b", 0l, JsString("b")),
      (2, "c", 0l, JsString("b"))
    )

    val sortedJsLs: Seq[(Any, Any, Any, Any)] = Seq(
      (0, "a", 0l, JsString("b")),
      (0, "a", 0l, JsString("a")),
      (0, "a", 1l, JsString("a")),
      (0, "b", 0l, JsString("b")),
      (1, "a", 0l, JsString("b")),
      (1, "b", 0l, JsString("b")),
      (2, "c", 0l, JsString("b"))
    )

    val ascendingLs: Seq[Boolean] = Seq(true, true, true, false)
    val resultJsLs = jsLs.sorted(TupleMultiOrdering[Any](ascendingLs))

    resultJsLs.toString() should equal(sortedJsLs.toString())
  }
}

