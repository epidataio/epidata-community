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
import play.api.test.Helpers._
import play.api.libs.ws._
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.mvc.Action
import scala.concurrent.duration._
import scala.concurrent._
import play.api.mvc._

@RunWith(classOf[JUnitRunner])
class DeviceAuthSpec extends Specification {

  object Fixtures {
    val truncateSQL = s"DELETE FROM iot_devices"
    def truncate = DB.executeUpdate(DB.prepare(truncateSQL))
    def cleanUp = {
      truncate
    }

    def install = {
      cleanUp
      DB.executeUpdate(DB.prepare("INSERT OR REPLACE INTO iot_devices (iot_device_id, iot_device_token) VALUES(\"iot_device_1\", \"epidata_123\");"))
      DB.executeUpdate(DB.prepare("INSERT OR REPLACE INTO iot_devices (iot_device_id, iot_device_token) VALUES(\"iot_device_2\", \"epidata_456\");"))
    }
  }

  "Device" should {

    "throw an exception with non-existing device_token for json body" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        POST,
        "/authenticate/deviceWeb").withTextBody("{\"device_id\": \"iot_device_1\", \"device_token\": \"NonExistentToken\"}")
      val result = controller.authenticateWeb()(request)

      status(result) must equalTo(BAD_REQUEST)

    }

    "throw an exception with non-existing device_id for json body" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        POST,
        "/authenticate/deviceWeb").withTextBody("{\"device_id\": \"NonExistentDevice\", \"device_token\": \"epidata_123\"}")
      val result = controller.authenticateWeb()(request)

      status(result) must equalTo(BAD_REQUEST)

    }

    "throw an exception with non-existing device_id and token for json body" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        POST,
        "/authenticate/deviceWeb").withTextBody("{\"device_id\": \"NonExistentDevice\", \"device_token\": \"NonExistentToken\"}")
      val result = controller.authenticateWeb()(request)

      status(result) must equalTo(BAD_REQUEST)

    }

    "create new jwt_token with proper device_id and token pair for json body" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        POST,
        "/authenticate/deviceWeb").withTextBody("{\"device_id\": \"iot_device_1\", \"device_token\": \"epidata_123\"}")
      val result = controller.authenticateWeb()(request)

      status(result) must equalTo(OK)

    }

    "throw an exception with empty string id and token for json body" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        POST,
        "/authenticate/deviceWeb").withTextBody("{\"device_id\": \"\", \"device_token\": \"\"}")
      val result = controller.authenticateWeb()(request)

      status(result) must equalTo(BAD_REQUEST)

    }

    "throw an exception with no id and token for json body" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        POST,
        "/authenticate/deviceWeb")
      val result = controller.authenticateWeb()(request)

      status(result) must equalTo(BAD_REQUEST)

    }

    "throw an exception with non-existing device_token for header" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        GET,
        "/authenticate/deviceApp").withHeaders("device_id" -> "iot_device_1", "device_token" -> "NonExistentToken")
      val result = controller.authenticateApp()(request)

      status(result) must equalTo(BAD_REQUEST)

    }

    "throw an exception with non-existing device_id for header" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        GET,
        "/authenticate/deviceApp").withHeaders("device_id" -> "NonExistentDevice", "device_token" -> "epidata_123")
      val result = controller.authenticateApp()(request)

      status(result) must equalTo(BAD_REQUEST)

    }

    "throw an exception with non-existing device_id and token for header" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        GET,
        "/authenticate/deviceApp").withHeaders("device_id" -> "NonExistentDevice", "device_token" -> "NonExistentToken")
      val result = controller.authenticateApp()(request)

      status(result) must equalTo(BAD_REQUEST)

    }

    "create new jwt_token with proper device_id and token pair for header" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        GET,
        "/authenticate/deviceApp").withHeaders("device_id" -> "iot_device_1", "device_token" -> "epidata_123")
      val result = controller.authenticateApp()(request)

      status(result) must equalTo(OK)

    }

    "throw an exception with empty string id and token for header" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        GET,
        "/authenticate/deviceApp").withHeaders("device_id" -> "", "device_token" -> "")
      val result = controller.authenticateApp()(request)

      status(result) must equalTo(BAD_REQUEST)

    }

    "throw an exception with no id and token for header" in new WithApplication {

      Fixtures.install

      val controller = app.injector.instanceOf[controllers.DeviceAuth]
      val request = FakeRequest(
        GET,
        "/authenticate/deviceApp")
      val result = controller.authenticateApp()(request)

      status(result) must equalTo(BAD_REQUEST)

    }
  }
}