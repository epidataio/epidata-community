/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import play.api.test._
import play.api.test.Helpers._
import securesocialtest.WithLoggedUser
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ApplicationSpec extends Specification {

  import WithLoggedUser._

  "Application" should {

    "retrieve public asset" in new WithApplication {
      val asset = route(FakeRequest(GET, "/assets/images/favicon.png")).get
      status(asset) must equalTo(OK)
    }

    "send 404 on a bad request" in new WithApplication {
      route(FakeRequest(GET, "/boum")) must beNone
    }

    "retrieve the notebook route" in new WithLoggedUser(minimalApp) {
      val notebook = route(FakeRequest(GET, "/notebook").withCookies(cookie)).get
      status(notebook) must equalTo(OK)
    }

    "fail to retrieve the notebook route without authentication" in new WithApplication {
      val notebook = route(FakeRequest(GET, "/notebook")).get
      status(notebook) must equalTo(SEE_OTHER)
    }

  }
}
