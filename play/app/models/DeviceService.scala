/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import org.apache.kafka.clients.producer.{ Callback, KafkaProducer, ProducerRecord, RecordMetadata }
import org.apache.kafka.common.PartitionInfo
import SQLite.DB

import java.util
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import java.nio.ByteBuffer
import com.epidata.lib.models.{ MeasurementSummary, MeasurementsKeys, Measurement => Model }
import com.epidata.lib.models.util.{ Binary, JsonHelpers }
import scala.collection.mutable.{ Map => MutableMap }

import java.sql.PreparedStatement

object DeviceService {

  def insertDevice(device_id: String, device_token: String): Unit = {

  }

  def updateDevice(device_id: String, jwt_token: String, expiry_time: Long): Unit = {

  }

  def quertyDevice(device_id: String): MutableMap[String, String] = {

    val mmap = MutableMap("device_token" -> "a")
    mmap
  }
  def queryToken(jwt_token: String): String = {

    val returnval = "a"
    returnval
  }

  def deleteDevice(device_id: String): Unit = {

  }

}
