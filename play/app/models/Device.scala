/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import cassandra.DB
import SQLite.{ DB => DBLite }
import service.Configs
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.Row
import securesocial.core.AuthenticationMethod
import securesocial.core.OAuth2Info
import securesocial.core.services
import scala.concurrent.Future
import securesocial.core.{ PasswordInfo, BasicProfile }
import java.sql.ResultSet

object Device {

  def createToken(): Any = {

  }

  def authenticate(device_id: String, device_token: String): Any = {

  }

  private lazy val sqlInsertStatement =
    """#INSERT OR REPLACE INTO iot_device (
         #iot_device_id,
         #iot_device_token,
         #connection_timeout,
         #authenticated_at,
         #json_wet_token) VALUES (?, ?, ?, ?, ?)""".stripMargin('#')

  private lazy val cqlInsertStatement =
    """#INSERT INTO users (
      #iot_device_id,
      #iot_device_token,
      #connection_timeout,
      #authenticated_at,
      #json_wet_token) VALUES (?, ?, ?, ?, ?)""".stripMargin('#')

}
