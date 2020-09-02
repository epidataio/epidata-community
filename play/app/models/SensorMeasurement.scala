/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import com.epidata.lib.models.{ SensorMeasurement => BaseSensorMeasurement, Measurement }
import play.api.Logger
import play.api.libs.json._
import _root_.util.Ordering
import service.{ Configs, KafkaService, DataService }
import scala.collection.convert.WrapAsScala

import java.util.Date
import scala.language.implicitConversions

object SensorMeasurement {

  import com.epidata.lib.models.SensorMeasurement._

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
  def insert(sensorMeasurement: BaseSensorMeasurement, Sqlite_enable: Boolean) = {
    if (Sqlite_enable) {
      SQLiteMeasurementService.insert(sensorMeasurement)
    } else {
      MeasurementService.insert(sensorMeasurement)
    }
  }

  def insert(sensorMeasurementList: List[BaseSensorMeasurement], Sqlite_enable: Boolean) = {
    if (Sqlite_enable) {
      SQLiteMeasurementService.bulkInsert(sensorMeasurementList)
    } else {
      MeasurementService.bulkInsert(sensorMeasurementList)
    }
  }

  def insertRecordFromKafka(str: String) = {
    BaseSensorMeasurement.jsonToSensorMeasurement(str) match {
<<<<<<< Updated upstream
      case Some(sensorMeasurement) => insert(sensorMeasurement)
      case _ => Logger.error("Bad json format!")
=======
      case Some(sensorMeasurement) => insert(sensorMeasurement, Configs.DBMeas)
      case _ => logger.error("Bad json format!")
>>>>>>> Stashed changes
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
      insert(sensorMeasurementList, Configs.DBMeas)
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
    tableName: String = com.epidata.lib.models.Measurement.DBTableName
  ): List[BaseSensorMeasurement] = MeasurementService.find(company, site, station, sensor, beginTime, endTime, ordering, tableName)
    .map(measurementToSensorMeasurement)

  /** Convert a list of SensorMeasurement to a json representation. */
  def toJson(sensorMeasurements: List[BaseSensorMeasurement]): String = BaseSensorMeasurement.toJson(sensorMeasurements)

}
