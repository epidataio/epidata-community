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

object Device {

  def createToken(): String = {
    val returnval = "a"

    returnval
  }

  def getCurrentdateTimeStamp: Timestamp = {
    val today = java.util.Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now: String = timeFormat.format(today)
    val currTime = java.sql.Timestamp.valueOf(now)
    currTime
  }

  def authenticate(device_id: String, device_token: String): String = {
    val devicemap = DeviceService.queryDevice(device_id)
    val retrieved_token = devicemap.get("device_token")
    if (retrieved_token == device_token) {
      val jwttoken = createToken()
      val authenticated_at = getCurrentdateTimeStamp
      DeviceService.updateDevice(device_token, jwttoken, authenticated_at)
      jwttoken
    } else {
      throw new Exception("Device Token does not match")

    }

  }
}

