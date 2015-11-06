package benchmark

import play.api.libs.json.JsNumber
import play.api.test.{FakeApplication, PlaySpecification, WithApplication}
import play.libs.Json

class JsonBenchmarkSpec extends BenchmarkCommon with PlaySpecification {
  "to json" should {
    implicit val app = FakeApplication()

    "json benchmark" in new WithApplication(app) {

      duration("map to json") {
        (0 to 100) foreach { n =>
          val numberMaps = (0 to 100).map { n => (n.toString -> JsNumber(n * n)) }.toMap
          Json.toJson(numberMaps)
        }
      }

      duration("directMakeJson") {
        (0 to 100) foreach { n =>
          var jsObj = play.api.libs.json.Json.obj()
          (0 to 100).foreach { n =>
            jsObj += (n.toString -> JsNumber(n * n))
          }
        }
      }

      duration("map to json 2") {
        (0 to 500) foreach { n =>
          val numberMaps = (0 to 100).map { n => (n.toString -> JsNumber(n * n)) }.toMap
          Json.toJson(numberMaps)
        }
      }

      duration("directMakeJson 2") {
        (0 to 500) foreach { n =>
          var jsObj = play.api.libs.json.Json.obj()
          (0 to 100).foreach { n =>
            jsObj += (n.toString -> JsNumber(n * n))
          }
        }
      }
    }
  }
}
