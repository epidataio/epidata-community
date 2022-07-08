/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import cassandra.DB
import SQLite.{ DB => DBLite }
import java.util.Date
import service._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.Row
import securesocial.core.AuthenticationMethod
import securesocial.core.OAuth2Info
import securesocial.core.services
import scala.concurrent.Future
import securesocial.core.{ PasswordInfo, BasicProfile }
import java.sql.ResultSet
import java.sql.Timestamp
import java.text.SimpleDateFormat
import play.api.Configuration
import authentikat.jwt._
import play.api.Play
import scala.util.parsing.json._
import org.json4s.JsonDSL._
import org.json4s.JValue

object Device {
  def createToken(deviceID: String): String = {

    val currTimeStamp: Long = System.currentTimeMillis / 1000
    val connectionTimeOut = Play.current.configuration.getString("device.timeout").get.toInt
    val expTimeStamp: Long = (System.currentTimeMillis - connectionTimeOut) / 1000
    val payload = s"""{"ss": "epidata.io","dev":${deviceID}, "access":"ingestion", "iat": ${currTimeStamp},""exp": ${expTimeStamp}, "timeout": ${connectionTimeOut}"""

    val token = generateToken(payload)

    println(token)
    token
  }

  def generateToken(payload: String): String = {
    val JwtSecretAlgo = "HS256"

    val header = JwtHeader(JwtSecretAlgo)
    val claimsSet = JwtClaimsSet(payload)
    val secretKey = Play.current.configuration.getString("application.secret")

    JsonWebToken(header, claimsSet, secretKey.get)

  }

  def getCurrentdateTimeStamp: Timestamp = {
    //timestamp format
    val today = java.util.Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now: String = timeFormat.format(today)
    val currTime = java.sql.Timestamp.valueOf(now)

    //unix timestamp format
    val timestamp: Long = System.currentTimeMillis / 1000
    println(timestamp)

    currTime
  }

  def authenticate(deviceID: String, deviceToken: String): String = {
    val deviceMap = DeviceService.queryDevice(deviceID)
    val retrievedToken = deviceMap.get("device_token")
    if (retrievedToken == deviceToken) {
      val jwttoken = createToken(deviceID)
      val authenticatedAt = getCurrentdateTimeStamp
      DeviceService.updateDevice(deviceToken, authenticatedAt)
      jwttoken
    } else {
      throw new Exception("Device Token does not match")
    }

  }

  def validate(deviceJWT: String): String = {
    val secretKey = Play.current.configuration.getString("application.secret").get
    if (!JsonWebToken.validate(deviceJWT, secretKey))
      throw new Exception("Json Web Token is not valid")
    else {
      var payload = deviceJWT match {
        case JsonWebToken(header, claimsSet, signature) => claimsSet.asJsonString
        case _ => None
      }
      //      val payload: Option[JValue] = deviceJWT match {
      //        case JsonWebToken(header, claimsSet, signature) =>
      //          Option(claimsSet.jvalue)
      //        case x =>
      //          None
      //
      //          val returnval = (payload.get \ "deviceID").values
      //          println("here", returnval.get("deviceID"))
      //      }

      //      val json = JSON.parse(payload)
      //      val newJson = json.as[JsObject] ++ Json.obj("iat" -> newIssueTime, "exp" -> newExp)
      //            val timeout: Long = (json \ "timeout").as[Long]
      //      val currDeviceId = (payload.get \ "deviceID").values

      //Need to get the deviceID

      val newIssueTime: Long = System.currentTimeMillis / 1000

      val connectionTimeOut = Play.current.configuration.getString("device.timeout").get.toInt
      val expTimeStamp: Long = (System.currentTimeMillis - connectionTimeOut) / 1000

      //deviceID placeholder
      val payload1 = s"""{"ss": "epidata.io","dev":"aa", "access":"ingestion", "iat": ${newIssueTime},""exp": ${expTimeStamp}, "timeout": ${connectionTimeOut}"""

      generateToken(payload1)
    }

  }

}

