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

package org.apache.s2graph.core.storage.hbase

import org.scalatest.{FunSuite, Matchers}

class AsynchbaseStorageTest extends FunSuite with Matchers {

  /** need secured cluster */
  //  test("test secure cluster connection", HBaseTest) {
  //    val config = ConfigFactory.parseMap(
  //      Map(
  //        "hbase.zookeeper.quorum" -> "localhost",
  //        "hbase.security.auth.enable" -> "true",
  //        "hbase.security.authentication" -> "kerberos",
  //        "hbase.kerberos.regionserver.principal" -> "hbase/_HOST@LOCAL.HBASE",
  //        "hbase.sasl.clientconfig" -> "Client",
  //        "java.security.krb5.conf" -> "krb5.conf",
  //        "java.security.auth.login.config" -> "async-client.jaas.conf")
  //    )
  //
  //    val client = AsynchbaseStorage.makeClient(config)
  //    val table = "test".getBytes()
  //
  //    val putRequest = new PutRequest(table, "a".getBytes(), "e".getBytes, "a".getBytes, "a".getBytes)
  //    val getRequest = new GetRequest(table, "a".getBytes(), "e".getBytes)
  //    val ret = client.put(putRequest).join()
  //    val kvs = client.get(getRequest).join()
  //    for {
  //      kv <- kvs
  //    } {
  //      println(kv.toString)
  //    }
  //  }
}