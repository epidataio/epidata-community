/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models

import java.sql.Timestamp
import java.util.{ Date, Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import java.lang.{ Long => JLong, Double => JDouble }

import com.datastax.driver.core.Row
import com.epidata.lib.models.util.{ Binary, Datatype, TypeUtils, JsonHelpers }
import org.json.simple.{ JSONArray, JSONObject }
import java.sql.ResultSet

/**
 * Specialization of MeasurementSummary representing sensor data.
 *
 * @param meas_lower_limit Lower limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 * @param meas_upper_limit Upper limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 */
case class SensorMeasurementSummary(
    company: String,
    site: String,
    station: String,
    sensor: String,
    start_time: Date,
    stop_time: Date,
    event: String,
    meas_name: String,
    meas_summary_name: String,
    meas_summary_value: String,
    meas_summary_description: Option[String])

object SensorMeasurementSummary {

  val NAME: String = "sensor_measurement_summary"

  // Model Conversions
  def rowToSensorMeasurementSummary(row: Row): SensorMeasurementSummary = MeasurementSummary.rowToMeasurementSummary(row)

  // Model Conversions for SQLite
  def rowToSensorMeasurementSummary(row: ResultSet): SensorMeasurementSummary = MeasurementSummary.rowToMeasurementSummary(row)

  implicit def measurementSummaryToSensorMeasurementSummary(ms: MeasurementSummary): SensorMeasurementSummary =
    SensorMeasurementSummary(
      ms.customer,
      ms.customer_site,
      ms.collection,
      ms.dataset,
      ms.start_time,
      ms.stop_time,
      ms.key1.get,
      ms.key2.get,
      ms.meas_summary_name,
      ms.meas_summary_value,
      ms.meas_summary_description)

  implicit def sensorMeasurementSummaryToMeasurementSummary(sms: SensorMeasurementSummary): MeasurementSummary =
    MeasurementSummary(
      sms.customer,
      sms.customer_site,
      sms.collection,
      sms.dataset,
      sms.start_time,
      sms.stop_time,
      Some(sms.event),
      Some(sms.meas_name),
      None,
      sms.meas_summary_name,
      sms.meas_summary_value,
      sms.meas_summary_description)

  implicit def sensorMeasurementSummaryToMeasurementSummary(sensorMeasurementsSummary: List[SensorMeasurementSummary]): List[MeasurementSummary] =
    sensorMeasurementsSummary.map(sensorMeasurementSummary => sensorMeasurementSummaryToMeasurementSummary(sensorMeasurementSummary))

  implicit def measurementSummaryToSensorMeasurementSummary(measurementsSummary: List[MeasurementSummary]): List[SensorMeasurementSummary] =
    measurementsSummary.map(measurementSummary => measurementSummaryToSensorMeasurementSummary(measurementSummary))

  // JSON Helpers
  def rowToJLinkedHashMap(rowSummary: Row, tableName: String): JLinkedHashMap[String, Object] = {
    tableName match {
      case MeasurementSummary.DBTableName =>
        val ms = rowToSensorMeasurementSummary(rowSummary)
        toJLinkedHashMap(ms)
    }
  }

  // JSON Helpers for SQLite
  def rowToJLinkedHashMap(rowSummary: ResultSet, tableName: String): JLinkedHashMap[String, Object] = {
    tableName match {
      case MeasurementSummary.DBTableName =>
        val ms = rowToSensorMeasurementSummary(rowSummary)
        toJLinkedHashMap(ms)
    }
  }

  import com.epidata.lib.models.util.JsonHelpers._

  def toJson(ms: SensorMeasurementSummary): String = {
    val map = toJLinkedHashMap(ms)
    JSONObject.toJSONString(map)
  }

  def toJson(sensorMeasurementsSummary: List[SensorMeasurementSummary]): String = {
    import scala.collection.JavaConverters._
    val arr = new JLinkedList[JLinkedHashMap[String, Object]]()
    arr.addAll(
      sensorMeasurementsSummary
        .map(ms => toJLinkedHashMap(ms))
        .asJavaCollection)
    JSONArray.toJSONString(arr)
  }

  def toJLinkedHashMap(m: SensorMeasurementSummary): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    putToMap(map, "company", m.company)
    putToMap(map, "site", m.site)
    putToMap(map, "station", m.station)
    putToMap(map, "sensor", m.sensor)

    if (m.start_time != null)
      putToMap(map, "start_time", convertToJLong(m.start_time.getTime))

    if (m.stop_time != null)
      putToMap(map, "stop_time", convertToJLong(m.stop_time.getTime))

    putToMap(map, "event", m.event)
    putToMap(map, "meas_name", m.meas_name)
    putToMap(map, "meas_summary_name", m.meas_summary_name)
    putToMap(map, "meas_summary_value", m.meas_summary_value)
    putToMap(map, "meas_summary_description", m.meas_summary_description)

    map
  }

  def fromJLinkedHashMap(map: JLinkedHashMap[String, Object]): SensorMeasurementSummary = {
    val company: String = map.get("company").asInstanceOf[String]
    val site: String = map.get("site").asInstanceOf[String]
    val station: String = map.get("station").asInstanceOf[String]
    val sensor: String = map.get("sensor").asInstanceOf[String]
    val start_time: Date = new Date(map.get("start_time").asInstanceOf[Long])
    val stop_time: Date = new Date(map.get("stop_time").asInstanceOf[Long])
    val event: String = map.get("event").asInstanceOf[String]
    val meas_name: String = map.get("meas_name").asInstanceOf[String]
    val meas_summary_name: String = map.get("meas_summary_name").asInstanceOf[String]
    val meas_summary_value: String = map.get("meas_summary_value").asInstanceOf[String]
    val meas_summary_description: Option[String] = TypeUtils.blankToNone(map.get("meas_summary_description").asInstanceOf[String])

    SensorMeasurementSummary(
      company,
      site,
      station,
      sensor,
      start_time,
      stop_time,
      event,
      meas_name,
      meas_summary_name,
      meas_summary_value,
      meas_summary_description)
  }

  def jsonToSensorMeasurementSummary(str: String): Option[SensorMeasurementSummary] = {
    fromJson(str) match {
      case Some(jSONObject) => Some(jsonToSensorMeasurementSummary(jSONObject))
      case _ => None
    }
  }

  def jsonToSensorMeasurementsSummary(str: String): List[Option[SensorMeasurementSummary]] = {
    fromJsonArray(str) match {
      case Some(jSONArray) => jSONArray.toArray.toList.map(
        x =>
          try {
            Some(jsonToSensorMeasurementSummary(x.asInstanceOf[JSONObject]))
          } catch {
            case _: Throwable => None
          })
      case _ => List.empty
    }
  }

  def jsonToSensorMeasurementSummary(jSONObject: JSONObject): SensorMeasurementSummary = {
    val company: String = jSONObject.get("company").asInstanceOf[String]
    val site: String = jSONObject.get("site").asInstanceOf[String]
    val station: String = jSONObject.get("station").asInstanceOf[String]
    val sensor: String = jSONObject.get("sensor").asInstanceOf[String]
    val start_time: Date = new Date(jSONObject.get("start_time").asInstanceOf[Long])
    val stop_time: Date = new Date(jSONObject.get("stop_time").asInstanceOf[Long])
    val event: String = jSONObject.get("event").asInstanceOf[String]
    val meas_name: String = jSONObject.get("meas_name").asInstanceOf[String]
    val meas_summary_name: String = jSONObject.get("meas_summary_name").asInstanceOf[String]
    val meas_summary_value: String = jSONObject.get("meas_summary_value").asInstanceOf[String]
    val meas_summary_description: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_summary_description").asInstanceOf[String])

    SensorMeasurementSummary(
      company,
      site,
      station,
      sensor,
      start_time,
      stop_time,
      event,
      meas_name,
      meas_summary_name,
      meas_summary_value,
      meas_summary_description)
  }

  def jsonToJLinkedHashMap(str: String): JLinkedHashMap[String, Object] = {
    val m = jsonToSensorMeasurementSummary(str).get
    toJLinkedHashMap(m)
  }

  def getColumns: Set[String] = {
    val col_set = Set(
      "company",
      "site",
      "station",
      "sensor",
      "start_time",
      "stop_time",
      "event",
      "meas_name",
      "meas_summary_name",
      "meas_summary_value",
      "meas_summary_description")
    col_set
  }
}
