/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import com.datastax.driver.core.ResultSet
import com.epidata.lib.models.{ SensorMeasurement => BaseSensorMeasurement, MeasurementSummary }
import com.epidata.lib.models.util.JsonHelpers
import play.api.libs.json._
import _root_.util.Ordering
import scala.collection.convert.WrapAsScala

import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import scala.language.implicitConversions
import scala.collection.JavaConverters._

object SensorMeasurement {

  import com.epidata.lib.models.SensorMeasurement._
  import com.epidata.lib.models.MeasurementSummary._

  /**
   * Insert a Double sensor measurement into the database.
   * @param sensorMeasurement The SensorMeasurement to insert.
   */
  def insert(sensorMeasurement: BaseSensorMeasurement): Unit =
    Measurement.insert(sensorMeasurement)

  def insertList(sensorMeasurementList: List[BaseSensorMeasurement]): Unit = Measurement.bulkInsert(sensorMeasurementList)

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
  ): List[BaseSensorMeasurement] = Measurement.find(company, site, station, sensor, beginTime, endTime, ordering, tableName)
    .map(measurementToSensorMeasurement)

  def query(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    size: Int,
    batch: String,
    ordering: Ordering.Value,
    tableName: String = com.epidata.lib.models.Measurement.DBTableName
  ): String = {

    // Get the data from Cassandra
    val rs: ResultSet = Measurement.query(company, site, station, sensor, beginTime, endTime, ordering, tableName, size, batch)

    // Get the next page info
    val nextPage = rs.getExecutionInfo().getPagingState()
    val nextBatch = if (nextPage == null) "" else nextPage.toString

    // only return the available ones by not fetching.
    val rows = 1.to(rs.getAvailableWithoutFetching()).map(_ => rs.one())

    // Convert the model to SensorMeasurements

    val records = new JLinkedList[JLinkedHashMap[String, Object]]()

    tableName match {
      case MeasurementSummary.DBTableName =>
        val measurements = rows
          .map(MeasurementSummary.rowToMeasurementSummary)
          .toList
          .map(measurementSummaryToSensorMeasurementSummary)

        records.addAll(
          measurements
          .map(m => JsonHelpers.toJLinkedHashMap(m))
          .asJavaCollection
        )

      case com.epidata.lib.models.MeasurementCleansed.DBTableName =>
        val measurements = rows
          .map(com.epidata.lib.models.MeasurementCleansed.rowToMeasurementCleansed)
          .toList
          .map(measurementCleansedToSensorMeasurementCleansed)

        records.addAll(
          measurements
          .map(m => JsonHelpers.toJLinkedHashMap(m))
          .asJavaCollection
        )

      case com.epidata.lib.models.Measurement.DBTableName =>
        val measurements = rows
          .map(com.epidata.lib.models.Measurement.rowToMeasurement)
          .toList
          .map(measurementToSensorMeasurement)

        records.addAll(
          measurements
          .map(m => JsonHelpers.toJLinkedHashMap(m))
          .asJavaCollection
        )
    }

    // Return the json object
    JsonHelpers.toJson(records, nextBatch)
  }

  /** Convert a list of SensorMeasurement to a json representation. */
  def toJson(sensorMeasurements: List[BaseSensorMeasurement]): String = JsonHelpers.toJson(sensorMeasurements)

  object JsonFormats {

    def convertNaNToDouble(double: Double): Double = {
      if (java.lang.Double.isNaN(double)) java.lang.Double.valueOf(0) else double
    }

    def convertAnyValToDouble(double: AnyVal): Double = {
      try {
        convertNaNToDouble(double.asInstanceOf[Double])
      } catch {
        case _: Throwable => 0
      }
    }

    def timestampToDate(t: java.sql.Timestamp): Date = new Date(t.getTime)
    def dateToTimestamp(dt: Date): java.sql.Timestamp = new java.sql.Timestamp(dt.getTime)

    implicit val timestampFormat = new Format[java.sql.Timestamp] {
      def writes(t: java.sql.Timestamp): JsValue = Json.toJson(timestampToDate(t))
      def reads(json: JsValue): JsResult[java.sql.Timestamp] = Json.fromJson[Date](json).map(dateToTimestamp)
    }
  }

}
