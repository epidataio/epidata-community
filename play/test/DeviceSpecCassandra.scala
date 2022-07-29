/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

import cassandra.DB
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
class DeviceSpecCassandra extends Specification {

  object Fixtures {
    val truncateSQL = s"TRUNCATE epidata_test.iot_devices"
    //    def truncate = DB.cql(truncateSQL)
    def cleanUp = {
      DB.cql(truncateSQL)
    }

    def install = {
      cleanUp
      DB.cql("INSERT INTO epidata_test.iot_devices (iot_device_id, iot_device_token) VALUES(\'device_1\',\'epidata123\');")
      DB.cql("INSERT INTO epidata_test.iot_devices (iot_device_id, iot_device_token) VALUES(\'device_2\', \'NonDefaultToken\');")

    }
  }

  "Device" should {


    //create a JWT Token with empty device ID
    "createToken function: create a jwt token with empty device ID" in new WithApplication() {
      try {
        val deviceID1 = ""
        val deviceToken1 = Device.createToken(deviceID1)
        fail()
      } catch {
        case _: Exception =>
      }
    }

    //create a JWT Token with null device ID
    "createToken function: create a jwt token with null device ID" in new WithApplication() {
      try {
        val deviceID1 = null
        val deviceToken1 = Device.createToken(deviceID1)
        fail()
      } catch {
        case _: Exception =>
      }
    }

    //create a JWT Token with given device ID
    "createToken function: create a jwt token with given device ID" in new WithApplication() {
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
    "createToken function: checking authenticated time when creating a jwt token with given device ID" in new WithApplication() {

      val deviceID1 = "DeviceToken1"

      val authenticatedAt1 = System.currentTimeMillis / 1000

      val deviceToken1 = Device.createToken(deviceID1)

      val authenticatedAt2 = System.currentTimeMillis / 1000

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

        issueTimeInt.toLong must be <= authenticatedAt2
        issueTimeInt.toLong must be >= authenticatedAt1
        //        issueTimeInt.toLong - authenticatedAt must be < timeRange
      }
    }

    //expire Time
    "createToken function: checking expire time when creating a jwt token with given device ID" in new WithApplication() {
      val deviceID1 = "DeviceToken1"

      val connectionTimeOut = Play.current.configuration.getString("device.timeout").get.toInt

      val authenticatedAt1 = System.currentTimeMillis / 1000
      val expTimeStamp1: Long = (authenticatedAt1 + connectionTimeOut)

      val deviceToken1 = Device.createToken(deviceID1)

      val authenticatedAt2 = System.currentTimeMillis / 1000
      val expTimeStamp2: Long = (authenticatedAt2 + connectionTimeOut)

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

        expireTimeInt.toLong >= expTimeStamp1
        expireTimeInt.toLong <= expTimeStamp2
      }
    }

    //authenticate funtion: if the ID is empty
    "authenticate funtion: if the ID is empty" in new WithApplication() {

      val deviceID1 = ""
      val deviceToken1 = "epidata123"

      try {
        val auth = Device.authenticate(deviceID1, deviceToken1)
        fail()
      } catch {
        case _: Exception => // Expected, so continue
      }
    }

    //authenticate funtion: if the ID is null
    "authenticate funtion: if the ID is null" in new WithApplication() {

      val deviceID1 = null
      val deviceToken1 = "epidata123"

      try {
        val auth = Device.authenticate(deviceID1, deviceToken1)
        fail()
      } catch {
        case _: Exception => // Expected, so continue
      }
    }

    //authenticate funtion: if the token is empty
    "authenticate funtion: if the token is empty" in new WithApplication() {

      val deviceID1 = "device_1"
      val deviceToken1 = ""

      try {
        val auth = Device.authenticate(deviceID1, deviceToken1)
        fail()
      } catch {
        case _: Exception => // Expected, so continue
      }
    }

    //authenticate funtion: if the token is null
    "authenticate funtion: if the token is null" in new WithApplication() {

      val deviceID1 = "device_1"
      val deviceToken1 = null

      try {
        val auth = Device.authenticate(deviceID1, deviceToken1)
        fail()
      } catch {
        case _: Exception => // Expected, so continue
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
      val rs = DB.cql(s"SELECT iot_device_token, authenticated_at,connection_timeout FROM iot_devices WHERE iot_device_id = \'device_1\'").one()

      var mmap = MutableMap[String, String]()

      if (rs != null) {
        mmap = MutableMap("device_token" -> rs.getString(0), "authenticated_at" -> rs.getLong(1).toString, "connection_timeout" -> rs.getInt(2).toString)
      }

      val deviceTokenInDB = mmap.get("device_token")
      val deviceTokenInDBString: String = deviceTokenInDB match {
        case None => "" //Or handle the lack of a value another way: throw an error, etc.
        case Some(s: String) => s //return the string to set your value
      }
      deviceToken1 must equalTo(deviceTokenInDBString)
    }

    //authenticate funtion: if the authnticated_at is in between the two timestamps
    "authenticate funtion: if the authnticated_at is in between the two timestamps" in new WithApplication() {
      Fixtures.install
      val deviceID1 = "device_1"
      val deviceToken1 = "epidata123"

      val authenticatedAt1 = System.currentTimeMillis / 1000
      val newJWTToken = Device.authenticate(deviceID1, deviceToken1)
      val rs = DB.cql(s"SELECT iot_device_token, authenticated_at,connection_timeout FROM iot_devices WHERE iot_device_id = \'device_1\'").one()

      val authenticatedAt2 = System.currentTimeMillis / 1000
      var mmap = MutableMap[String, String]()

      if (rs != null) {
        mmap = MutableMap("device_token" -> rs.getString(0), "authenticated_at" -> rs.getLong(1).toString, "connection_timeout" -> rs.getInt(2).toString)
      }

      val updatedAuthenTime = mmap.get("authenticated_at")
      val updatedAuthenTimeString: String = updatedAuthenTime match {
        case None => "" //Or handle the lack of a value another way: throw an error, etc.
        case Some(s: String) => s //return the string to set your value
      }

      updatedAuthenTimeString.toLong >= authenticatedAt1
      updatedAuthenTimeString.toLong <= authenticatedAt2
    }

    //validate funtion: if Device Token is empty
    "validate funtion: if Device Token is empty" in new WithApplication() {

      //create deviceToken
      val deviceToken = ""

      try {
        //validate device
        val updatedToken = Device.validate(deviceToken)
        fail()
      } catch {
        case _: Exception => // Expected, so continue
      }
    }

    //validate funtion: if Device Token is null
    "validate funtion: if Device Token is null" in new WithApplication() {

      //create deviceToken
      val deviceToken = null

      try {
        //validate device
        val updatedToken = Device.validate(deviceToken)
        fail()
      } catch {
        case _: Exception => // Expected, so continue
      }
    }

    //validate funtion:authenticated time
    "validate funtion: check if authenticated time are within a same range before and after validate" in new WithApplication() {

      val deviceID1 = "device_1"
      val deviceToken1 = "epidata123"

      //current time
      val secretKey = Play.current.configuration.getString("application.secret").get
      val timestampBefore = System.currentTimeMillis / 1000

      //create deviceToken
      val deviceToken = Device.createToken(deviceID1)

      //validate device
      val updatedToken = Device.validate(deviceToken)

      val timestampAfter = System.currentTimeMillis / 1000

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

        authenticatedAtAfterInt.toLong >= timestampBefore
        authenticatedAtAfterInt.toLong <= timestampAfter
      }
    }

    //validate funtion: expire time
    "validate funtion: check if expire time are within a same range before and after validate" in new WithApplication() {

      val deviceID1 = "device_1"
      val deviceToken1 = "epidata123"

      //current time
      val secretKey = Play.current.configuration.getString("application.secret").get
      val connectionTimeOut = Play.current.configuration.getString("device.timeout").get.toInt

      val authenticatedAt1 = System.currentTimeMillis / 1000
      val expTimeStampBefore: Long = (authenticatedAt1 + connectionTimeOut)

      //create deviceToken
      val deviceToken = Device.createToken(deviceID1)

      //validate device
      val updatedToken = Device.validate(deviceToken)

      val authenticatedAt2 = System.currentTimeMillis / 1000
      val expTimeStampAfter: Long = (authenticatedAt2 + connectionTimeOut)

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
        val expTimeStamp = payload.getOrElse(Map.empty[String, String]).get("exp")
        val expTimeStampInt: String = expTimeStamp match {
          case None => "" //Or handle the lack of a value another way: throw an error, etc.
          case Some(s: String) => s //return the string to set your value
        }

        expTimeStampInt.toLong >= expTimeStampBefore
        expTimeStampInt.toLong <= expTimeStampAfter
      }
    }

  }

}