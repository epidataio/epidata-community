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
import play.api.test.Helpers._
import play.api.libs.ws._
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.mvc.Action
import scala.concurrent.duration._
import scala.concurrent._
import play.mvc.Results

@RunWith(classOf[JUnitRunner])
class DeviceSpec extends Specification {

  object Fixtures {
    val truncateSQL = s"DELETE FROM iot_devices"
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

  "Device" should {

    val application: Application = GuiceApplicationBuilder().build()

    "DeviceAuth authenticate test" in new WithServer(app = application, port = 9443) {
      // The test payment gateway requires a callback to this server before it returns a result...
      var device_id = "device_1"
      var device_token = "NonExistentToken"

      val ws = app.injector.instanceOf[WSClient]

      var response = Await.result(ws.url("http://localhost:9443/authenticate/device").withQueryString("device_id" -> device_id, "device_token" -> device_token).get(), 1000 millis)

      response.status must equalTo(BAD_REQUEST)

      device_id = "NonExistentDevice"
      device_token = "epidata123"

      response = Await.result(ws.url("http://localhost:9443/authenticate/device").withQueryString("device_id" -> device_id, "device_token" -> device_token).get(), 1000 millis)

      response.status must equalTo(BAD_REQUEST)

      device_id = "NonExistentDevice"
      device_token = "NonExistentToken"

      response = Await.result(ws.url("http://localhost:9443/authenticate/device").withQueryString("device_id" -> device_id, "device_token" -> device_token).get(), 1000 millis)

      response.status must equalTo(BAD_REQUEST)

      device_id = "device_1"
      device_token = "epidata123"

      response = Await.result(ws.url("http://localhost:9443/authenticate/device").withQueryString("device_id" -> device_id, "device_token" -> device_token).get(), 1000 millis)

      response.status must equalTo(OK)
    }
    /*
    "throw an exception with non-existing device_id" in new WithServer(app = application, port = 9443) {
      // The test payment gateway requires a callback to this server before it returns a result...
      val device_id = "NonExistentDevice"
      val device_token = "epidata123"
      val ws = app.injector.instanceOf[WSClient]
      // await is from play.api.test.FutureAwaits
      val response = Await.result(ws.url("http://localhost:9443/authenticate/device").withQueryString("device_id" -> device_id, "device_token" -> device_token).get(), 1000 millis)
      response.status must equalTo(BAD_REQUEST)
    }
    "throw an exception with non-existing device_id and token" in new WithServer(app = application, port = 9443) {
      // The test payment gateway requires a callback to this server before it returns a result...
      val device_id = "NonExistentDevice"
      val device_token = "NonExistentToken"
      val ws = app.injector.instanceOf[WSClient]
      // await is from play.api.test.FutureAwaits
      val response = Await.result(ws.url("http://localhost:9443/authenticate/device").withQueryString("device_id" -> device_id, "device_token" -> device_token).get(), 1000 millis)
      response.status must equalTo(BAD_REQUEST)
    }
    "create new jwt_token with proper device_id and token pair" in new WithServer(app = application, port = 9443) {
      // The test payment gateway requires a callback to this server before it returns a result...
      val device_id = "device_1"
      val device_token = "epidata123"
      val ws = app.injector.instanceOf[WSClient]
      // await is from play.api.test.FutureAwaits
      val response = Await.result(ws.url("http://localhost:9443/authenticate/device").withQueryString("device_id" -> device_id, "device_token" -> device_token).get(), 1000 millis)
      response.status must equalTo(OK)
    }
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
*/
  }
}