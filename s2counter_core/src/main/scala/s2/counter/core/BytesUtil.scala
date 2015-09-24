package s2.counter.core

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 11..
 */
trait BytesUtil {
  def getRowKeyPrefix(id: Int): Array[Byte]

  def toBytes(key: ExactKeyTrait): Array[Byte]
  def toBytes(eq: ExactQualifier): Array[Byte]
  def toBytes(tq: TimedQualifier): Array[Byte]

//  def toIntExactKey(bytes: Array[Byte]): ExactKeyTrait
//  def toLongExactKey(bytes: Array[Byte]): ExactKeyTrait
//  def toStringExactKey(bytes: Array[Byte]): ExactKeyTrait
//  def toBlobExactKey(bytes: Array[Byte], orgItemId: Array[Byte]): ExactKeyTrait
  def toExactQualifier(bytes: Array[Byte]): ExactQualifier
  def toTimedQualifier(bytes: Array[Byte]): TimedQualifier
}
