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

package s2.util

import org.apache.hadoop.hbase.util.Bytes

import scala.util.hashing.MurmurHash3

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 27..
 */
object Hashes {
  def sha1(s: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-1")
    Bytes.toHex(md.digest(s.getBytes("UTF-8")))
  }
  
  private def positiveHash(h: Int): Int = {
    if (h < 0) -1 * (h + 1) else h
  }

  def murmur3(s: String): Int = {
    val hash = MurmurHash3.stringHash(s)
    positiveHash(hash)
  }
}
