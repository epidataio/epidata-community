/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package models

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
import scala.collection.mutable.{ Map => MutableMap }

object Device {

  //create the JWT Token by taking in a deviceID
  def createToken(deviceID: String): String = {
    if (deviceID == null || deviceID.trim.isEmpty) {
      throw new Exception("Device ID is empty")
    }

    //get time stamp
    val currTimeStamp: Long = System.currentTimeMillis / 1000
    val connectionTimeOut = Play.current.configuration.getString("device.timeout").get.toInt
    val expTimeStamp: Long = (currTimeStamp + connectionTimeOut)

    //create payload Map
    val payload = Map("ss" -> "epidata.io", "dev" -> deviceID, "access" -> "ingestion", "iat" -> currTimeStamp, "exp" -> expTimeStamp, "timeout" -> connectionTimeOut)

    //generate JWT Token
    val token = generateToken(payload)
    token
  }

  //a private helper function to generate JWT Token. Takes in a payload
  def generateToken(payload: Map[String, Any]): String = {

    val JwtSecretAlgo = "HS256"

    //parameters
    val header = JwtHeader(JwtSecretAlgo)
    val claimsSet = JwtClaimsSet(payload)
    val secretKey = Play.current.configuration.getString("application.secret")

    //Create the token
    JsonWebToken(header, claimsSet, secretKey.get)
  }

  //authenticate the device by checking if deviceToken is matched
  def authenticate(deviceID: String, deviceToken: String): String = {
    if (deviceID == null || deviceID.trim.isEmpty) {
      throw new Exception("Device ID is empty")
    }
    if (deviceToken == null || deviceToken.trim.isEmpty) {
      throw new Exception("Device Token is empty")
    }

    var deviceMap = MutableMap[String, String]()
    if (Configs.deviceDB.equals("cassandra")) {
      deviceMap = NoSQLDeviceService.queryDevice(deviceID)
    } else {
      deviceMap = SQLiteDeviceService.queryDevice(deviceID)

    }

    val retrievedToken = deviceMap.get("device_token")

    val deviceTokenString: String = retrievedToken match {
      case None => "" //Or handle the lack of a value another way: throw an error, etc.
      case Some(s: String) => s //return the string to set your value
    }
    if (deviceTokenString.equals(deviceToken)) {
      val jwtToken = createToken(deviceID)
      val authenticatedAt = System.currentTimeMillis / 1000

      if (Configs.deviceDB.equals("cassandra")) {
        NoSQLDeviceService.updateDevice(deviceID, authenticatedAt)
      } else {
        SQLiteDeviceService.updateDevice(deviceID, authenticatedAt)
      }
      jwtToken
    } else {
      throw new Exception("Device Token does not match")
    }
  }

  //validate deviceJWT using jwt library
  def validate(deviceJWT: String): String = {
    if (deviceJWT == null || deviceJWT.trim.isEmpty) {
      throw new Exception("Device JWT Token is empty")
    }
    val secretKey = Play.current.configuration.getString("application.secret").get
    if (!JsonWebToken.validate(deviceJWT, secretKey))
      throw new Exception("Json Web Token is not valid")
    else {
      val payload: Option[Map[String, String]] = deviceJWT match {
        case JsonWebToken(header, claimsSet, signature) =>
          claimsSet.asSimpleMap.toOption
        case x =>
          None
      }
      val deviceID = payload.getOrElse(Map.empty[String, String]).get("dev")

      val deviceIDString: String = deviceID match {
        case None => "" //Or handle the lack of a value another way: throw an error, etc.
        case Some(s: String) => s //return the string to set your value
      }

      val newIssueTime: Long = System.currentTimeMillis / 1000
      val connectionTimeOut = Play.current.configuration.getString("device.timeout").get.toInt
      val expTimeStamp: Long = (newIssueTime + connectionTimeOut)

      val newPayload = Map("ss" -> "epidata.io", "dev" -> deviceIDString, "access" -> "ingestion", "iat" -> newIssueTime, "exp" -> expTimeStamp, "timeout" -> connectionTimeOut)
      val newtoken = generateToken(newPayload)

      newtoken
    }

  }

}
