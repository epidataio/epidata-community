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
import java.sql.Timestamp
import java.sql.PreparedStatement

object DeviceService {

  def insertDevice(deviceID: String, deviceToken: String): Unit = {
    val insertStatements = InsertsDevicestring()
    val stm = DB.prepare(insertStatements)
    DB.binds(stm, deviceID, deviceToken)
    DB.executeUpdate(stm)
  }

  def updateDevice(deviceID: String, issueTime: Timestamp): Unit = {
    val updateStatements = updateDevicestring()
    val stm = DB.prepare(updateStatements)
    DB.binds(stm, issueTime, deviceID)
    DB.executeUpdate(stm)
  }

  def queryDevice(deviceID: String): MutableMap[String, String] = {
    val quertyDeviceStatements = preparequertyDevice()
    val stm = DB.prepare(quertyDeviceStatements)
    DB.binds(stm, deviceID)

    val rs = DB.execute(stm)
    val mmap = MutableMap[String, String]()

    while (rs.next()) {
      val mmap = MutableMap("device_token" -> rs.getString(1), "json_wet_token" -> rs.getString(2), "authenticated_at" -> rs.getString(3), "connection_timeout" -> rs.getString(4))
    }
    mmap
  }

  def deleteDevice(deviceID: String): Unit = {
    val deleteDeviceStatements = preparedeleteDevice()
    val stm = DB.prepare(deleteDeviceStatements)
    DB.binds(stm, deviceID)
    DB.executeUpdate(stm)
  }

  private def InsertsDevicestring() = s"""#INSERT OR REPLACE INTO iot_devices (
                                         #iot_device_id,
                                         #iot_device_token) VALUES (?, ?)""".stripMargin('#')

  private def updateDevicestring() = s"""#UPDATE iot_devices
SET connection_timeout = ?
WHERE iot_device_id = ?""".stripMargin('#')

  private def preparequertyDevice() = s"""#SELECT iot_device_token, json_wet_token, authenticated_at,connection_timeout FROM iot_devices WHERE iot_device_id = ?""".stripMargin('#')

  private def preparedeleteDevice() = s"""#DELETE FROM iot_devices WHERE iot_device_id = ?""".stripMargin('#')

}