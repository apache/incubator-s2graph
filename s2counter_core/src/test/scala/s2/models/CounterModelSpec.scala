package s2.models

import com.typesafe.config.ConfigFactory
import org.specs2.mutable.Specification

/**
 * Created by hsleep(honeysleep@gmail.com) on 15. 5. 26..
 */
class CounterModelSpec extends Specification {
  val config = ConfigFactory.load()

  DBModel.initialize(config)

  "CounterModel" should {
    val model = new CounterModel(config)
    "findById" in {
      model.findById(0, useCache = false) must beNone
    }

    "findByServiceAction using cache" in {
      val service = "test"
      val action = "test_action"
      val counter = Counter(useFlag = true, 2, service, action, Counter.ItemType.STRING,
        autoComb = true, "", useProfile = true, None, useRank = true, 0, None, None, None, None, None, None)
      model.createServiceAction(counter)
      model.findByServiceAction(service, action, useCache = false) must beSome
      val opt = model.findByServiceAction(service, action, useCache = true)
      opt must beSome
      model.findById(opt.get.id) must beSome
      model.deleteServiceAction(opt.get)
      model.findById(opt.get.id) must beSome
      model.findById(opt.get.id, useCache = false) must beNone
    }

    "create and delete policy" in {
      val (service, action) = ("test", "test_case")
      for {
        policy <- model.findByServiceAction(service, action, useCache = false)
      } {
        model.deleteServiceAction(policy)
      }
      model.createServiceAction(Counter(useFlag = true, 2, service, action, Counter.ItemType.STRING,
        autoComb = true, "", useProfile = true, None, useRank = true, 0, None, None, None, None, None, None))
      model.findByServiceAction(service, action, useCache = false).map { policy =>
        policy.service mustEqual service
        policy.action mustEqual action
        model.deleteServiceAction(policy)
        policy
      } must beSome
      model.findByServiceAction(service, action, useCache = false) must beNone
    }
  }
}
