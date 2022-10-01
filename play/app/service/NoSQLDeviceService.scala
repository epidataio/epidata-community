/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package models

import org.apache.kafka.clients.producer.{ Callback, KafkaProducer, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.PartitionInfo
import cassandra.DB

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

object NoSQLDeviceService {

  //insert deviceID and deviceToken to DB
  def insertDevice(deviceID: String, deviceToken: String): Unit = {
    val insertStatements = s"""INSERT INTO iot_devices (iot_device_id,iot_device_token) VALUES (${deviceID}, ${deviceToken})"""
    DB.cql(insertStatements)
  }

  //update issueTime using deviceId
  def updateDevice(deviceID: String, issueTime: Long): Unit = {
    val updateStatements = s"UPDATE iot_devices SET authenticated_at = ${issueTime} WHERE iot_device_id =\'" + deviceID + "\'"
    DB.cql(updateStatements)
  }

  //query the device using deviceID
  def queryDevice(deviceID: String): MutableMap[String, String] = {
    val quertyDeviceStatements = s"SELECT iot_device_token, authenticated_at,connection_timeout FROM iot_devices WHERE iot_device_id = \'" + deviceID + "\'"

    val rs = DB.cql(quertyDeviceStatements).one()
    var mmap = MutableMap[String, String]()

    if (rs != null) {
      mmap = MutableMap("device_token" -> rs.getString(0), "authenticated_at" -> rs.getLong(1).toString, "connection_timeout" -> rs.getInt(2).toString)
    }
    mmap
  }

  //delete the device using deviceID
  def deleteDevice(deviceID: String): Unit = {
    val deleteDeviceStatements = s"""DELETE FROM iot_devices WHERE iot_device_id = ${deviceID}"""
    DB.cql(deleteDeviceStatements)
  }

}
