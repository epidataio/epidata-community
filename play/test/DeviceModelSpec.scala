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
import models.Device
import authentikat.jwt._
import play.api.Play
import scala.util.parsing.json._
import org.scalatest.Assertions._
import scala.collection.mutable.{ Map => MutableMap }

@RunWith(classOf[JUnitRunner])
class DeviceModelSpec extends Specification {

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

    "create a jwt token with given device ID" in new WithApplication() {
      Fixtures.install
      val deviceID1 = "DeviceToken1"
      val deviceToken1 = Device.createToken(deviceID1)

      val secretKey = Play.current.configuration.getString("application.secret").get
      if (!JsonWebToken.validate(deviceToken1, secretKey))
        throw new Exception("Json Web Token is not valid")
      else {
        val payload: Option[Map[String, String]] = deviceToken1 match {
          case JsonWebToken(header, claimsSet, signature) =>
            claimsSet.asSimpleMap.toOption
          case x =>
            None
        }
        val deviceIDresult = payload.getOrElse(Map.empty[String, String]).get("dev")
        val deviceID: String = deviceIDresult match {
          case None => "" //Or handle the lack of a value another way: throw an error, etc.
          case Some(s: String) => s //return the string to set your value
        }
        deviceID must equalTo(deviceID1)
      }
    }

    //authenticated Time
    "create a jwt token with given device ID" in new WithApplication() {
      Fixtures.install
      val deviceID1 = "DeviceToken1"

      val authenticatedAt = System.currentTimeMillis / 1000

      val deviceToken1 = Device.createToken(deviceID1)

      val secretKey = Play.current.configuration.getString("application.secret").get
      if (!JsonWebToken.validate(deviceToken1, secretKey))
        throw new Exception("Json Web Token is not valid")
      else {
        val payload: Option[Map[String, String]] = deviceToken1 match {
          case JsonWebToken(header, claimsSet, signature) =>
            claimsSet.asSimpleMap.toOption
          case x =>
            None
        }
        val issueTime = payload.getOrElse(Map.empty[String, String]).get("iat")
        val issueTimeInt: String = issueTime match {
          case None => "" //Or handle the lack of a value another way: throw an error, etc.
          case Some(s: String) => s //return the string to set your value
        }
        val number: AnyVal = 5
        val timeRange: Long = number.asInstanceOf[Number].longValue
        issueTimeInt.toLong - authenticatedAt must be < timeRange
      }
    }

    //expire Time
    "create a jwt token with given device ID" in new WithApplication() {
      Fixtures.install
      val deviceID1 = "DeviceToken1"

      val authenticatedAt = System.currentTimeMillis / 1000
      val connectionTimeOut = Play.current.configuration.getString("device.timeout").get.toInt
      val expTimeStamp: Long = (authenticatedAt + connectionTimeOut)

      val deviceToken1 = Device.createToken(deviceID1)

      val secretKey = Play.current.configuration.getString("application.secret").get
      if (!JsonWebToken.validate(deviceToken1, secretKey))
        throw new Exception("Json Web Token is not valid")
      else {
        val payload: Option[Map[String, String]] = deviceToken1 match {
          case JsonWebToken(header, claimsSet, signature) =>
            claimsSet.asSimpleMap.toOption
          case x =>
            None
        }
        val issueTime = payload.getOrElse(Map.empty[String, String]).get("exp")
        val expireTimeInt: String = issueTime match {
          case None => "" //Or handle the lack of a value another way: throw an error, etc.
          case Some(s: String) => s //return the string to set your value
        }
        val number: AnyVal = 5
        val timeRange: Long = number.asInstanceOf[Number].longValue

        expireTimeInt.toLong - expTimeStamp must be < timeRange
      }
    }

    //authenticate funtion: if the given token is different with the one in db
    "authenticate funtion: if the given token is different with the one in db" in new WithApplication() {
      Fixtures.install
      val deviceID1 = "device_1"
      val deviceToken1 = "epidata123"

      try {
        val auth = Device.authenticate(deviceID1, deviceToken1)
        fail()
      } catch {

        case _: Exception => // Expected, so continue
      }
    }

    //authenticate funtion: if the given token is same with the one in db
    "authenticate funtion: if the given token is same with the one in db" in new WithApplication() {
      Fixtures.install
      val deviceID1 = "device_1"
      val deviceToken1 = "epidata123"

      val authenticatedAt = System.currentTimeMillis / 1000
      val newJWTToken = Device.authenticate(deviceID1, deviceToken1)
      val rs = DB.execute(DB.binds(DB.prepare(s"SELECT iot_device_token, authenticated_at,connection_timeout FROM iot_devices WHERE iot_device_id = ?"), deviceID1))

      var mmap = MutableMap[String, String]()

      while (rs.next()) {
        mmap = MutableMap("device_token" -> rs.getString(1), "authenticated_at" -> rs.getString(2), "connection_timeout" -> rs.getString(3))
      }

      val updatedAuthenTime = mmap.get("authenticated_at")
      val updatedAuthenTimeString: String = updatedAuthenTime match {
        case None => "" //Or handle the lack of a value another way: throw an error, etc.
        case Some(s: String) => s //return the string to set your value
      }

      val number: AnyVal = 5
      val timeRange: Long = number.asInstanceOf[Number].longValue

      authenticatedAt - updatedAuthenTimeString.toLong must be < timeRange

    }

    //validate funtion:
    "check if authenticated time are within a same range before and after validate" in new WithApplication() {
      Fixtures.install
      val deviceID1 = "device_1"
      val deviceToken1 = "epidata123"

      //current time
      val secretKey = Play.current.configuration.getString("application.secret").get
      val authenticatedAtBefore = System.currentTimeMillis / 1000

      //create deviceToken
      val deviceToken = Device.createToken(deviceID1)

      //validate device
      val updatedToken = Device.validate(deviceToken)

      //decode the updated token
      if (!JsonWebToken.validate(updatedToken, secretKey))
        throw new Exception("Json Web Token is not valid")
      else {
        val payload: Option[Map[String, String]] = updatedToken match {
          case JsonWebToken(header, claimsSet, signature) =>
            claimsSet.asSimpleMap.toOption
          case x =>
            None
        }
        val authenticatedAtAfter = payload.getOrElse(Map.empty[String, String]).get("iat")
        val authenticatedAtAfterInt: String = authenticatedAtAfter match {
          case None => "" //Or handle the lack of a value another way: throw an error, etc.
          case Some(s: String) => s //return the string to set your value
        }
        val number: AnyVal = 5
        val timeRange: Long = number.asInstanceOf[Number].longValue
        authenticatedAtBefore - authenticatedAtAfterInt.toLong must be < timeRange
      }
    }

    //validate funtion:
    "check if expire time are within a same range before and after validate" in new WithApplication() {
      Fixtures.install
      val deviceID1 = "device_1"
      val deviceToken1 = "epidata123"

      //current time
      val secretKey = Play.current.configuration.getString("application.secret").get
      val authenticatedAt = System.currentTimeMillis / 1000
      val connectionTimeOut = Play.current.configuration.getString("device.timeout").get.toInt
      val expTimeStampBefore: Long = (authenticatedAt + connectionTimeOut)

      //create deviceToken
      val deviceToken = Device.createToken(deviceID1)

      //validate device
      val updatedToken = Device.validate(deviceToken)

      //decode the updated token
      if (!JsonWebToken.validate(updatedToken, secretKey))
        throw new Exception("Json Web Token is not valid")
      else {
        val payload: Option[Map[String, String]] = updatedToken match {
          case JsonWebToken(header, claimsSet, signature) =>
            claimsSet.asSimpleMap.toOption
          case x =>
            None
        }
        val expTimeStampAfter = payload.getOrElse(Map.empty[String, String]).get("exp")
        val expTimeStampAfterInt: String = expTimeStampAfter match {
          case None => "" //Or handle the lack of a value another way: throw an error, etc.
          case Some(s: String) => s //return the string to set your value
        }
        val number: AnyVal = 5
        val timeRange: Long = number.asInstanceOf[Number].longValue
        expTimeStampBefore - expTimeStampAfterInt.toLong must be < timeRange
      }
    }
  }

}