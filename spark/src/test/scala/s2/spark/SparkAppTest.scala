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

package s2.spark

import org.scalatest.{FunSuite, Matchers}

/**
 * Created by alec.k on 14. 12. 26..
 */

object TestApp extends SparkApp {
  // 상속받은 클래스에서 구현해줘야 하는 함수
  override def run(): Unit = {
    validateArgument("topic", "phase")
  }
}

class SparkAppTest extends FunSuite with Matchers {
  test("parse argument") {
    TestApp.main(Array("s2graphInreal", "real"))
    TestApp.getArgs(0) shouldEqual "s2graphInreal"
  }
}
