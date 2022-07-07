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
import java.time

object Device {

  def createToken(): String = {

    val returnval = "a"

    returnval
  }

  def authenticate(device_id: String, device_token: String): String = {
    val devicemap = DeviceService.queryDevice(device_id)
    val retrieved_token = devicemap.get("device_token")
    if (retrieved_token == device_token) {
      val jwttoken = createToken()
      val authenticated_at = time.LocalDateTime.now()
      DeviceService.updateDevice(device_token, jwttoken, authenticated_at)
      jwttoken
    } else {
      ""
    }

  }
}

