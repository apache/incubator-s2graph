package controllers

import com.kakao.s2graph.core.mysqls.Label
import play.api.http.HeaderNames
import play.api.test.Helpers._
import play.api.test.{FakeApplication, FakeRequest}

import scala.concurrent.Await

/**
 * Created by mojo22jojo(hyunsung.jo@gmail.com) on 15. 10. 13..
 */
class AdminControllerSpec extends SpecCommon {
  init()
  "EdgeControllerSpec" should {
    "update htable" in {
      running(FakeApplication()) {
        val insertUrl = s"/graphs/updateHTable/$testLabelName/$newHTableName"

        val req = FakeRequest("POST", insertUrl).withBody("").withHeaders(HeaderNames.CONTENT_TYPE -> "text/plain")

        Await.result(route(req).get, HTTP_REQ_WAITING_TIME)
        Label.findByName(testLabelName, useCache = true).get.hTableName mustEqual newHTableName
      }
    }
  }
}
