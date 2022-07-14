/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

import SQLite.DB
import com.datastax.driver.core.exceptions.InvalidQueryException
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import play.api.test._
import play.api.test.Helpers._
import org.scalatestplus.junit.JUnitRunner
import play.api.libs.json._
import play.core.SourceMapper
import java.io.File
import play.api._

@RunWith(classOf[JUnitRunner])
class DeviceSpec extends Specification {

  object Fixtures {
    val truncateSQL = s"TRUNCATE iot_devices"
    def truncate = DB.executeUpdate(DB.prepare(truncateSQL))
    def cleanUp = {
      truncate
    }

    def install = {
      cleanUp
      DB.executeUpdate(DB.prepare("INSERT OR REPLACE INTO iot_devices (iot_device_id, iot_device_token) VALUES(\"device_1\", \"epidata123\");"))
      DB.executeUpdate(DB.prepare("INSERT OR REPLACE INTO iot_devices (iot_device_id, iot_device_token) VALUES(\"device_2\", \"NonDefaultToken\");"))
    }
  }

  class FakeApp
    extends FakeApplication(
      withGlobal = Some(new GlobalSettings {
        override def onStart(app: Application) = Global.onStart(app)

        override def onRouteRequest(req: RequestHeader): Option[Handler] =
          automated_test.Routes.handlerFor(req) orElse super.onRouteRequest(req)

        override def onStop(app: Application) = Global.onStop(app)
      }))

  object FakeApp {
    def apply() = new FakeApp()
  }

  "Device" should {

    "throw an exception with non-existing token" in new WithApplication(FakeApp()) {
      Fixtures.install
      val create = route(FakeRequest(
        GET,
        "/authenticate/device",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#[{
            #"device_id": "device_1",
            #"device_token": "NonExistentToken",
            #}]""".stripMargin('#'))).withCookies(cookie)).get
      status(create) must equalTo(SEE_OTHER)
    }

    "throw an exception with non-existing device_id" in new WithApplication(FakeApp()) {
      Fixtures.install
      val create = route(FakeRequest(
        GET,
        "/authenticate/device",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#[{
            #"device_id": "NonExistentDevice",
            #"device_token": "epidata123",
            #}]""".stripMargin('#'))).withCookies(cookie)).get
      status(create) must equalTo(SEE_OTHER)
    }

    "throw an exception with non-existing device_id and token" in new WithApplication(FakeApp()) {
      Fixtures.install
      val create = route(FakeRequest(
        GET,
        "/authenticate/device",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#[{
            #"device_id": "NonExistentDevice",
            #"device_token": "NonExistentToken",
            #}]""".stripMargin('#'))).withCookies(cookie)).get
      status(create) must equalTo(SEE_OTHER)
    }

    "create new jwt_token with proper device_id and token pair" in new WithApplication(FakeApp()) {
      Fixtures.install
      val create = route(FakeRequest(
        GET,
        "/authenticate/device",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#[{
            #"device_id": "device_1",
            #"device_token": "epidata123",
            #}]""".stripMargin('#'))).withCookies(cookie)).get
      status(create) must equalTo(Ok)
    }

  }

}