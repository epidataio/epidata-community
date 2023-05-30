/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package models

import java.util.Date

import com.epidata.lib.models.{ Measurement, MeasurementCleansed, MeasurementSummary, SensorMeasurement => BaseSensorMeasurement, SensorMeasurementCleansed => BaseSensorMeasurementCleansed, SensorMeasurementSummary => BaseSensorMeasurementSummary }
import play.api.Logger
import play.api.libs.json._
import _root_.util.Ordering
import models.SensorMeasurement.keyForMeasurementTopic
import service.{ Configs, DataService, KafkaService, ZMQProducer, ZMQInit }

import scala.collection.convert.WrapAsScala
import scala.language.implicitConversions

object SensorMeasurement {

  import com.epidata.lib.models.SensorMeasurement._
  import com.epidata.lib.models.SensorMeasurementCleansed._
  import com.epidata.lib.models.SensorMeasurementSummary._

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
   * Insert a sensor measurement into the database.
   * @param sensorMeasurement The SensorMeasurement data to insert.
   */
  def insert(sensorMeasurement: BaseSensorMeasurement, sqliteEnable: Boolean) = {
    if (sqliteEnable) {
      SQLiteMeasurementService.insert(sensorMeasurement)
      // logger.info("insert called. sensorMeasurement: " + sensorMeasurement)
    } else {
      MeasurementService.insert(sensorMeasurement)
    }
  }

  /**
   * Insert multiple sensor measurements into the database.
   * @param sensorMeasurementList Multiple SensorMeasurement data to insert.
   */
  def insert(sensorMeasurementList: List[BaseSensorMeasurement], sqliteEnable: Boolean): Unit = {
    if (sqliteEnable) {
      SQLiteMeasurementService.bulkInsert(sensorMeasurementList.map(sensorMeasurementToMeasurement))
      // logger.info("Bulk insert called. sensorMeasurementList map: " + sensorMeasurementList.map(sensorMeasurementToMeasurement))
    } else {
      MeasurementService.bulkInsert(sensorMeasurementList.map(sensorMeasurementToMeasurement))
    }
  }

  /**
   * Insert a sensor measurement cleansed data into the database.
   * @param sensorMeasurementCleansed The SensorMeasurementCleansed data to insert.
   */
  def insertCleansed(sensorMeasurementCleansed: BaseSensorMeasurementCleansed, sqliteEnable: Boolean) = {
    if (sqliteEnable) {
      SQLiteMeasurementService.insertCleansed(sensorMeasurementCleansed)
    } else {
      // To Do
      //MeasurementService.insertCleansed(sensorMeasurementCleansed)
    }
  }

  /**
   * Insert multiple sensor measurement cleansed data into the database.
   * @param sensorMeasurementCleansedList Multiple SensorMeasurementCleansed data to insert.
   */
  def insertCleansed(sensorMeasurementCleansedList: List[BaseSensorMeasurementCleansed], sqliteEnable: Boolean) = {
    if (sqliteEnable) {
      SQLiteMeasurementService.bulkInsertCleansed(sensorMeasurementCleansedList.map(sensorMeasurementCleansedToMeasurementCleansed))
    } else {
      // To Do
      //MeasurementService.bulkInsert(sensorMeasurementCleansedList.map(sensorMeasurementCleansedToMeasurementCleansed))
    }
  }

  /**
   * Insert a sensor measurement summary into the database.
   * @param sensorMeasurementSummary The SensorMeasurementSummary to insert.
   */
  def insertSummary(sensorMeasurementSummary: BaseSensorMeasurementSummary, sqliteEnable: Boolean) = {
    if (sqliteEnable) {
      SQLiteMeasurementService.insertSummary(sensorMeasurementSummary)
    } else {
      // To Do
      //MeasurementService.insertSummary(sensorMeasurementSummary)
    }
  }

  /**
   * Insert multiple sensor measurement summary data into the database.
   * @param sensorMeasurementSummaryList Multiple SensorMeasurementSummary data to insert.
   */
  def insertSummary(sensorMeasurementSummaryList: List[BaseSensorMeasurementSummary], sqliteEnable: Boolean) = {
    if (sqliteEnable) {
      SQLiteMeasurementService.bulkInsertSummary(sensorMeasurementSummaryList.map(sensorMeasurementSummaryToMeasurementSummary))
    } else {
      // To Do
      //MeasurementService.bulkInsertSummary(sensorMeasurementSummaryList.map(sensorMeasurementSummaryToMeasurementSummary))
    }
  }

  def insertRecordFromKafka(str: String) = {
    BaseSensorMeasurement.jsonToSensorMeasurement(str) match {
      case Some(sensorMeasurement) => insert(sensorMeasurement, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertRecordFromZMQ(str: String): Unit = {
    // println("insertRecordFromZMQ called. str: " + str + "\n")
    BaseSensorMeasurement.jsonToSensorMeasurement(str) match {
      case Some(sensorMeasurement) => insert(sensorMeasurement, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertCleansedRecordFromZMQ(str: String): Unit = {
    // println("insertCleansedRecordFromZMQ called." + "\n")
    // println("str: " + str + "\n")
    BaseSensorMeasurementCleansed.jsonToSensorMeasurementCleansed(str) match {
      case Some(smc) => insertCleansed(smc, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertSummaryRecordFromZMQ(str: String): Unit = {
    // println("insertSummaryRecordFromZMQ called." + "\n")
    // println("str: " + str + "\n")
    BaseSensorMeasurementSummary.jsonToSensorMeasurementSummary(str) match {
      case Some(sms) => {
        insertSummary(sms, Configs.measDBLite)
      }
      case _ => logger.error("Bad json format!")
    }
  }

  def insertDynamicRecordFromZMQ(str: String): Unit = {
    try {
      BaseSensorMeasurementCleansed.jsonToSensorMeasurementCleansed(str) match {
        case Some(smc: BaseSensorMeasurementCleansed) => {
          // println("inserting dynamic record as measurements cleansed record.\n")
          insertCleansed(smc, Configs.measDBLite)
          return
        }
        case _ => throw new Exception("Dynamic data is not of type MeasurementCleansed.")
      }
    } catch {
      case _: Throwable => {
        try {
          BaseSensorMeasurementSummary.jsonToSensorMeasurementSummary(str) match {
            case Some(sms: BaseSensorMeasurementSummary) => {
              // println("inserting dynamic record as measurements summary record.\n")
              insertSummary(sms, Configs.measDBLite)
              return
            }
            case _ => throw new Exception("Dynamic data is not of type MeasurementSummary.")
          }
        } catch {
          case _: Throwable => {
            logger.error("Bad json format!")
          }
        }
      }
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

    // Two way ingestion NOT supported with SQLite
    if (Configs.twoWaysIngestion && !Configs.measDBLite) {
      insert(sensorMeasurementList, Configs.measDBLite)
    }
  }

  /**
   * Insert a measurement into the ZMQ.
   * @param sensorMeasurement The Measurement to insert.
   */
  def insertToZMQ(sensorMeasurement: BaseSensorMeasurement): Unit = {
    // logger.info("insertToZMQ called. sensorMeasurement: " + sensorMeasurement)
    val key = keyForMeasurementTopic(sensorMeasurement)
    val value = BaseSensorMeasurement.toJson(sensorMeasurement)
    // logger.info("key: " + key + ", value: " + value)

    ZMQInit._ZMQProducer.push(key, value)
    ZMQInit._ZMQProducer.pub(Measurement.zmqTopic, key, value)
  }

  def insertToZMQ(sensorMeasurementList: List[BaseSensorMeasurement]): Unit = {
    // logger.info("Bulk insertToZMQ called. sensorMeasurementList: " + sensorMeasurementList)
    sensorMeasurementList.foreach(m => insertToZMQ(m))

    // Two way ingestion NOT supported with SQLite
    if (Configs.twoWaysIngestion && !Configs.measDBLite) {
      insert(sensorMeasurementList, Configs.measDBLite)
    }
  }

  /** Convert a list of SensorMeasurement to a json representation. */
  def toJson(sensorMeasurements: List[BaseSensorMeasurement]): String = BaseSensorMeasurement.toJson(sensorMeasurements)

  def query(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    size: Int = 1000,
    batch: String = "",
    ordering: Ordering.Value,
    sqliteEnable: Boolean): List[BaseSensorMeasurement] = {
    if (sqliteEnable) {
      SQLiteMeasurementService.query(company, site, station, sensor, beginTime, endTime, size, batch, ordering)
        .map(BaseSensorMeasurement.measurementToSensorMeasurement)
    } else {
      MeasurementService.query(company, site, station, sensor, beginTime, endTime, size, batch, ordering)
        .map(BaseSensorMeasurement.measurementToSensorMeasurement)
    }
  }

  def queryCleansed(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    size: Int = 1000,
    batch: String = "",
    ordering: Ordering.Value,
    sqliteEnable: Boolean): List[BaseSensorMeasurementCleansed] = {
    if (sqliteEnable) {
      SQLiteMeasurementService.queryCleansed(company, site, station, sensor, beginTime, endTime, size, batch, ordering)
        .map(BaseSensorMeasurementCleansed.measurementCleansedToSensorMeasurementCleansed)
    } else {
      MeasurementService.queryCleansed(company, site, station, sensor, beginTime, endTime, size, batch, ordering)
        .map(BaseSensorMeasurementCleansed.measurementCleansedToSensorMeasurementCleansed)
    }
  }

  def querySummary(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    size: Int = 1000,
    batch: String = "",
    ordering: Ordering.Value,
    sqliteEnable: Boolean): List[BaseSensorMeasurementSummary] = {
    if (sqliteEnable) {
      SQLiteMeasurementService.querySummary(company, site, station, sensor, beginTime, endTime, size, batch, ordering)
        .map(BaseSensorMeasurementSummary.measurementSummaryToSensorMeasurementSummary)
    } else {
      MeasurementService.querySummary(company, site, station, sensor, beginTime, endTime, size, batch, ordering)
        .map(BaseSensorMeasurementSummary.measurementSummaryToSensorMeasurementSummary)
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
    .map(BaseSensorMeasurement.measurementToSensorMeasurement)

}
