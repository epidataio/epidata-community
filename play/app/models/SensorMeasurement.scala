/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import java.util.Date

import com.epidata.lib.models.{ Measurement, SensorMeasurement => BaseSensorMeasurement, SensorMeasurementCleansed => BaseSensorMeasurementCleansed, SensorMeasurementSummary => BaseSensorMeasurementSummary }
import play.api.Logger
import play.api.libs.json._
import _root_.util.Ordering
import models.SensorMeasurement.keyForMeasurementTopic
import service.{ Configs, DataService, KafkaService, ZMQProducer, ZMQInit }

import scala.collection.convert.WrapAsScala
import scala.language.implicitConversions

object SensorMeasurement {

  import com.epidata.lib.models.SensorMeasurement._
  val logger: Logger = Logger(this.getClass())

  val name: String = "SensorMeasurement"

  private def keyForMeasurementTopic(measurement: BaseSensorMeasurement): String = {
    val key =
      s"""
         |${measurement.customer}${DataService.Delim}
         |${measurement.customer_site}${DataService.Delim}
         |${measurement.collection}${DataService.Delim}
         |${measurement.dataset}${DataService.Delim}
         |${measurement.epoch}
       """.stripMargin
    DataService.getMd5(key)
  }

  /**
   * Insert a Double sensor measurement into the database.
   * @param sensorMeasurement The SensorMeasurement to insert.
   */
  def insert(sensorMeasurement: BaseSensorMeasurement, sqliteEnable: Boolean) = {
    if (sqliteEnable) {
      SQLiteMeasurementService.insert(sensorMeasurement)
    } else {
      MeasurementService.insert(sensorMeasurement)
    }
  }

  def insert(sensorMeasurementList: List[BaseSensorMeasurement], sqliteEnable: Boolean) = {
    if (sqliteEnable) {
      SQLiteMeasurementService.bulkInsert(sensorMeasurementList)
    } else {
      MeasurementService.bulkInsert(sensorMeasurementList)
    }
  }

  /**
   * Insert a Double cleansed sensor measurement into the database.
   * @param sensorMeasurementCleansed The SensorMeasurementCleansed to insert.
   */
  def insertCleansed(sensorMeasurementCleansed: BaseSensorMeasurementCleansed, sqliteEnable: Boolean) = {
    if (sqliteEnable) {
      SQLiteMeasurementService.insertCleansed(sensorMeasurementCleansed)
    } else {
      // To Do
      //MeasurementService.insert(sensorMeasurementCleansed)
    }
  }

  def insertCleansed(sensorMeasurementCleansedList: List[BaseSensorMeasurementCleansed], sqliteEnable: Boolean) = {
    if (sqliteEnable) {
      SQLiteMeasurementService.bulkInsertCleansed(sensorMeasurementCleansedList)
    } else {
      // To Do
      //MeasurementService.bulkInsert(sensorMeasurementCleansedList)
    }
  }

  /**
   * Insert a Double cleansed sensor measurement into the database.
   * @param sensorMeasurementCleansed The SensorMeasurementCleansed to insert.
   */
  def insertSummary(sensorMeasurementSummary: BaseSensorMeasurementSummary, sqliteEnable: Boolean) = {
    if (sqliteEnable) {
      SQLiteMeasurementService.insertSummary(sensorMeasurementSummary)
    } else {
      // To Do
      //MeasurementService.insertSummary(sensorMeasurementSummary)
    }
  }

  def insertSummary(sensorMeasurementSummaryList: List[BaseSensorMeasurementSummary], sqliteEnable: Boolean) = {
    if (sqliteEnable) {
      SQLiteMeasurementService.bulkInsertSummary(sensorMeasurementSummaryList)
    } else {
      // To Do
      //MeasurementService.bulkInsertSummary(sensorMeasurementSummaryList)
    }
  }

  def insertRecordFromKafka(str: String) = {
    BaseSensorMeasurement.jsonToSensorMeasurement(str) match {
      case Some(sensorMeasurement) => insert(sensorMeasurement, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertRecordFromZMQ(str: String): Unit = {
    println("insertRecordFromZMQ called. str: " + str + "\n")
    BaseSensorMeasurement.jsonToSensorMeasurement(str) match {
      case Some(sensorMeasurement) => insert(sensorMeasurement, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertCleansedRecordFromZMQ(str: String): Unit = {
    println("insertCleansedRecordFromZMQ called. str: " + str + "\n")
    BaseSensorMeasurementCleansed.jsonToSensorMeasurementCleansed(str) match {
      case Some(sensorMeasurementCleansed) => insertCleansed(sensorMeasurementCleansed, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertSummaryRecordFromZMQ(str: String): Unit = {
    println("insertSummaryRecordFromZMQ called. str: " + str + "\n")
    BaseSensorMeasurementSummary.jsonToSensorMeasurementSummary(str) match {
      case Some(sensorMeasurementSummary) => insertSummary(sensorMeasurementSummary, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  /**
   * Insert a measurement into the kafka.
   * @param sensorMeasurement The Measurement to insert.
   */
  def insertToKafka(sensorMeasurement: BaseSensorMeasurement): Unit = {

    val key = keyForMeasurementTopic(sensorMeasurement)
    val value = BaseSensorMeasurement.toJson(sensorMeasurement)
    KafkaService.sendMessage(Measurement.KafkaTopic, key, value)
  }

  def insertToKafka(sensorMeasurementList: List[BaseSensorMeasurement]): Unit = {
    sensorMeasurementList.foreach(m => insertToKafka(m))
    if (Configs.twoWaysIngestion) {
      insert(sensorMeasurementList, Configs.measDBLite)
    }
  }

  /**
   * Insert a measurement into the ZMQ.
   * @param sensorMeasurement The Measurement to insert.
   */
  def insertToZMQ(sensorMeasurement: BaseSensorMeasurement): Unit = {
    val key = keyForMeasurementTopic(sensorMeasurement)
    val value = BaseSensorMeasurement.toJson(sensorMeasurement)
    println("insertToZMQ called. key: " + key + ", value: " + value + "\n")
    ZMQInit._ZMQProducer.push(key, value)
    ZMQInit._ZMQProducer.pub(key, value)
  }

  def insertToZMQ(sensorMeasurementList: List[BaseSensorMeasurement]): Unit = {
    println("Bulk insertToZMQ called.\n")
    sensorMeasurementList.foreach(m => insertToZMQ(m))
    if (Configs.twoWaysIngestion) {
      insert(sensorMeasurementList, Configs.measDBLite)
    }
  }

  /**
   * Find sensor measurements in the database matching the specified parameters.
   * @param company
   * @param site
   * @param station
   * @param sensor
   * @param beginTime Beginning of query time interval, inclusive
   * @param endTime End of query time interval, exclusive
   * @param ordering Timestamp ordering of results, if specified.
   */
  @Deprecated
  def find(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    ordering: Ordering.Value,
    tableName: String = com.epidata.lib.models.Measurement.DBTableName): List[BaseSensorMeasurement] = MeasurementService.find(company, site, station, sensor, beginTime, endTime, ordering, tableName)
    .map(measurementToSensorMeasurement)

  /** Convert a list of SensorMeasurement to a json representation. */
  def toJson(sensorMeasurements: List[BaseSensorMeasurement]): String = BaseSensorMeasurement.toJson(sensorMeasurements)

}
