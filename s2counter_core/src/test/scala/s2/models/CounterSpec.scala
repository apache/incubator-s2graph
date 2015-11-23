package s2.models

import org.specs2.mutable.Specification
import s2.models.Counter.ItemType

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 6. 11..
 */
class CounterSpec extends Specification {
  "Counter" should {
    "dimension auto combination" in {
      val policy = Counter(
        useFlag = true,
        2,
        "test",
        "test_case",
        ItemType.LONG,
        autoComb = true,
        "p1,p2,p3",
        useProfile = false,
        None,
        useRank = true,
        0,
        None,
        None,
        None,
        None,
        None,
        None
      )

      policy.dimensionSp mustEqual Array("p1", "p2", "p3")
      policy.dimensionList.map { arr => arr.toSeq }.toSet -- Set(Seq.empty[String], Seq("p1"), Seq("p2"), Seq("p3"), Seq("p1", "p2"), Seq("p1", "p3"), Seq("p2", "p3"), Seq("p1", "p2", "p3")) must beEmpty
    }
  }
}
