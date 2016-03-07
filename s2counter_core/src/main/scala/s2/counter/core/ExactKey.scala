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

import s2.models.Counter
import s2.models.Counter.ItemType
import s2.models.Counter.ItemType.ItemType
import s2.util.Hashes

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 27..
 */
trait ExactKeyTrait {
  def policyId: Int
  def version: Byte
  def itemType: ItemType
  def itemKey: String
}

case class ExactKey(policyId: Int, version: Byte, itemType: ItemType, itemKey: String) extends ExactKeyTrait
case class BlobExactKey(policyId: Int, version: Byte, itemType: ItemType, itemKey: String, itemId: String) extends ExactKeyTrait

object ExactKey {
  def apply(policy: Counter, itemId: String, checkItemType: Boolean): ExactKeyTrait = {
    if (checkItemType && policy.itemType == ItemType.BLOB) {
      BlobExactKey(policy.id, policy.version, ItemType.BLOB, Hashes.sha1(itemId), itemId)
    } else {
      ExactKey(policy.id, policy.version, policy.itemType, itemId)
    }
  }
}
