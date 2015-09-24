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
//  def apply(policy: Counter, itemId: String, orgItemId: String): ExactKeyTrait = BlobExactKey(policy.id, policy.itemType, itemId, orgItemId)
}
