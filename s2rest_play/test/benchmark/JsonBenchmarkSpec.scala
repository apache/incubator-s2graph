package benchmark

import play.api.libs.json.JsNumber
import play.libs.Json

class JsonBenchmarkSpec extends BenchmarkCommon {
  "to json" >> {
    "json benchmark" >> {

      duration("map to json") {
        (0 to 10) foreach { n =>
          val numberMaps = (0 to 100).map { n => (n.toString -> JsNumber(n * n)) }.toMap
          Json.toJson(numberMaps)
        }
      }

      duration("directMakeJson") {
        (0 to 10) foreach { n =>
          var jsObj = play.api.libs.json.Json.obj()
          (0 to 10).foreach { n =>
            jsObj += (n.toString -> JsNumber(n * n))
          }
        }
      }

      duration("map to json 2") {
        (0 to 50) foreach { n =>
          val numberMaps = (0 to 10).map { n => (n.toString -> JsNumber(n * n)) }.toMap
          Json.toJson(numberMaps)
        }
      }

      duration("directMakeJson 2") {
        (0 to 50) foreach { n =>
          var jsObj = play.api.libs.json.Json.obj()
          (0 to 10).foreach { n =>
            jsObj += (n.toString -> JsNumber(n * n))
          }
        }
      }
      true
    }
    true
  }
}
