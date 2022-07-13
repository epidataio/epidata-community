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
    //get time stamp
    val currTimeStamp: Long = System.currentTimeMillis / 1000
    val connectionTimeOut = Play.current.configuration.getString("device.timeout").get.toInt
    val expTimeStamp: Long = (currTimeStamp + connectionTimeOut)

    //create payload Map
    val payload = Map("ss" -> "epidata.io", "dev" -> deviceID, "access" -> "ingestion", "iat" -> currTimeStamp, "exp" -> expTimeStamp, "timeout" -> connectionTimeOut)

    //generate jwt token
    val token = generateToken(payload)
    token
  }

  def generateToken(payload: Map[String, Any]): String = {

    val JwtSecretAlgo = "HS256"

    //parameters
    val header = JwtHeader(JwtSecretAlgo)
    val claimsSet = JwtClaimsSet(payload)
    val secretKey = Play.current.configuration.getString("application.secret")

    JsonWebToken(header, claimsSet, secretKey.get)
  }

  def authenticate(deviceID: String, deviceToken: String): String = {

    val deviceMap = DeviceService.queryDevice(deviceID)
    val retrievedToken = deviceMap.get("device_token")
    if (retrievedToken.equals(deviceToken)) {
      val jwttoken = createToken(deviceID)
      val authenticatedAt = System.currentTimeMillis / 1000
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
      val payload: Option[Map[String, String]] = deviceJWT match {
        case JsonWebToken(header, claimsSet, signature) =>
          claimsSet.asSimpleMap.toOption
        case x =>
          None
      }
      val deviceID = payload.getOrElse(Map.empty[String, String]).get("dev")

      val newIssueTime: Long = System.currentTimeMillis / 1000
      val connectionTimeOut = Play.current.configuration.getString("device.timeout").get.toInt
      val expTimeStamp: Long = (newIssueTime + connectionTimeOut)

      val newPayload = Map("ss" -> "epidata.io", "dev" -> deviceID, "access" -> "ingestion", "iat" -> newIssueTime, "exp" -> expTimeStamp, "timeout" -> connectionTimeOut)
      val newtoken = generateToken(newPayload)

      newtoken
    }

  }

}
