/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import org.apache.kafka.clients.producer.{ Callback, KafkaProducer, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.PartitionInfo
import SQLite.DB

import java.util
import java.time
import java.sql.{ Connection, CallableStatement, DriverManager, PreparedStatement, ResultSet, SQLException, Statement }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import java.nio.ByteBuffer
import com.epidata.lib.models.{ MeasurementSummary, MeasurementsKeys, Measurement => Model }
import com.epidata.lib.models.util.{ Binary, JsonHelpers }
import scala.collection.mutable.{ Map => MutableMap }

import java.sql.PreparedStatement

object DeviceService {

  def insertDevice(device_id: String, device_token: String): Unit = {
    val insertStatements = InsertsDevicestring()
    val stm = DB.prepare(insertStatements)
    DB.binds(stm, device_id, device_token)
    DB.executeUpdate(stm)
  }

  def updateDevice(device_id: String, jwt_token: String, expiry_time: time.LocalDateTime): Unit = {
    val updateStatements = updateDevicestring()
    val stm = DB.prepare(updateStatements)
    DB.binds(stm, jwt_token, expiry_time, device_id)
    DB.executeUpdate(stm)
  }

  def queryDevice(device_id: String): MutableMap[String, String] = {
    val quertyDeviceStatements = preparequertyDevice()
    val stm = DB.prepare(quertyDeviceStatements)
    DB.binds(stm, device_id)

    val rs = DB.execute(stm)
    val mmap = MutableMap[String, String]()

    while (rs.next()) {
      val mmap = MutableMap("device_token" -> rs.getString(1), "json_wet_token" -> rs.getString(2), "authenticated_at" -> rs.getString(3), "connection_timeout" -> rs.getString(4))
    }
    mmap
  }
  def queryToken(jwt_token: String): String = {
    val queryTokenStatements = preparequeryToken()
    val stm = DB.prepare(queryTokenStatements)
    DB.binds(stm, jwt_token)
    val rs = DB.execute(stm)
    while (rs.next()) {
      val returnval = rs.getString(1)
      returnval
    }
    ""
  }

  def deleteDevice(device_id: String): Unit = {
    val deleteDeviceStatements = preparedeleteDevice()
    val stm = DB.prepare(deleteDeviceStatements)
    DB.binds(stm, device_id)
    DB.executeUpdate(stm)
  }

  private def InsertsDevicestring() = s"""#INSERT OR REPLACE INTO iot_devices (
                                         #iot_device_id,
                                         #iot_device_token) VALUES (?, ?)""".stripMargin('#')

  private def updateDevicestring() = s"""#UPDATE iot_devices
SET json_wet_token = ?,
connection_timeout = ?
WHERE iot_device_id = ?""".stripMargin('#')

  private def preparequertyDevice() = s"""#SELECT iot_device_token, json_wet_token, authenticated_at,connection_timeout FROM iot_devices WHERE iot_device_id = ?""".stripMargin('#')

  private def preparequeryToken() = s"""#SELECT iot_device_id FROM iot_devices WHERE json_wet_token = ?""".stripMargin('#')

  private def preparedeleteDevice() = s"""#DELETE FROM iot_devices WHERE iot_device_id = ?""".stripMargin('#')

}