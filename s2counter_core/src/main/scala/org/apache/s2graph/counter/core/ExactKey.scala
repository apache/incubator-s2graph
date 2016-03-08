package org.apache.s2graph.counter.core

import org.apache.s2graph.counter.models.Counter
import org.apache.s2graph.counter.models.Counter.ItemType
import org.apache.s2graph.counter.models.Counter.ItemType.ItemType
import org.apache.s2graph.counter.util.Hashes

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
