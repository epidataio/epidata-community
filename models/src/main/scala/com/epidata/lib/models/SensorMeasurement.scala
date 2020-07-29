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

/**
 * Specialization of Measurement representing sensor data.
 *
 * @param meas_lower_limit Lower limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 * @param meas_upper_limit Upper limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 */
case class SensorMeasurement(
    company: String,
    site: String,
    station: String,
    sensor: String,
    ts: Date,
    event: String,
    meas_name: String,
    meas_datatype: Option[String],
    meas_value: Any,
    meas_unit: Option[String],
    meas_status: Option[String],
    meas_lower_limit: Option[AnyVal],
    meas_upper_limit: Option[AnyVal],
    meas_description: Option[String])

case class SensorMeasurementCleansed(
    company: String,
    site: String,
    station: String,
    sensor: String,
    ts: Date,
    event: String,
    meas_name: String,
    meas_datatype: Option[String],
    meas_value: Any,
    meas_unit: Option[String],
    meas_status: Option[String],
    meas_flag: Option[String],
    meas_method: Option[String],
    meas_lower_limit: Option[AnyVal],
    meas_upper_limit: Option[AnyVal],
    meas_description: Option[String])

case class SensorMeasurementSummary(
    company: String,
    site: String,
    station: String,
    sensor: String,
    start_time: Timestamp,
    stop_time: Timestamp,
    event: String,
    meas_name: String,
    meas_summary_name: String,
    meas_summary_value: String,
    meas_summary_description: String)

object SensorMeasurement {

  val NAME: String = "sensor_measurement"

  // Model Conversions
  def rowToSensorMeasurement(row: Row): SensorMeasurement = Measurement.rowToMeasurement(row)
  def rowToSensorMeasurementCleansed(row: Row): SensorMeasurementCleansed = MeasurementCleansed.rowToMeasurementCleansed(row)
  def rowToSensorMeasurementSummary(row: Row): SensorMeasurementSummary = MeasurementSummary.rowToMeasurementSummary(row)

  implicit def measurementToSensorMeasurement(measurement: Measurement): SensorMeasurement =
    SensorMeasurement(
      measurement.customer,
      measurement.customer_site,
      measurement.collection,
      measurement.dataset,
      measurement.ts,
      measurement.key1.get,
      measurement.key2.get,
      measurement.meas_datatype,
      measurement.meas_value,
      measurement.meas_unit,
      measurement.meas_status,
      measurement.meas_lower_limit,
      measurement.meas_upper_limit,
      measurement.meas_description)

  implicit def measurementCleansedToSensorMeasurementCleansed(measurement: MeasurementCleansed): SensorMeasurementCleansed =
    SensorMeasurementCleansed(
      measurement.customer,
      measurement.customer_site,
      measurement.collection,
      measurement.dataset,
      measurement.ts,
      measurement.key1.get,
      measurement.key2.get,
      measurement.meas_datatype,
      measurement.meas_value,
      measurement.meas_unit,
      measurement.meas_status,
      measurement.meas_flag,
      measurement.meas_method,
      measurement.meas_lower_limit,
      measurement.meas_upper_limit,
      measurement.meas_description)

  implicit def measurementSummaryToSensorMeasurementSummary(ms: MeasurementSummary): SensorMeasurementSummary =
    SensorMeasurementSummary(
      ms.customer,
      ms.customer_site,
      ms.collection,
      ms.dataset,
      ms.start_time,
      ms.stop_time,
      ms.key1,
      ms.key2,
      ms.meas_summary_name,
      ms.meas_summary_value,
      ms.meas_summary_description)

  implicit def sensorMeasurementToMeasurement(sensorMeasurement: SensorMeasurement): Measurement =
    Measurement(
      sensorMeasurement.company,
      sensorMeasurement.site,
      sensorMeasurement.station,
      sensorMeasurement.sensor,
      sensorMeasurement.ts,
      Some(sensorMeasurement.event),
      Some(sensorMeasurement.meas_name),
      None,
      sensorMeasurement.meas_datatype,
      sensorMeasurement.meas_value,
      sensorMeasurement.meas_unit,
      sensorMeasurement.meas_status,
      sensorMeasurement.meas_lower_limit,
      sensorMeasurement.meas_upper_limit,
      sensorMeasurement.meas_description,
      None,
      None)

  implicit def sensorMeasurementToMeasurement(sensorMeasurements: List[SensorMeasurement]): List[Measurement] =
    sensorMeasurements.map(sensorMeasurement => sensorMeasurementToMeasurement(sensorMeasurement))

  implicit def measurementToSensorMeasurement(measurements: List[Measurement]): List[SensorMeasurement] =
    measurements.map(measurement => measurementToSensorMeasurement(measurement))

  // JSON Helpers
  def rowToJLinkedHashMap(row: Row, tableName: String): JLinkedHashMap[String, Object] = {
    tableName match {
      case MeasurementSummary.DBTableName =>
        val m = rowToSensorMeasurementSummary(row)
        toJLinkedHashMap(m)

      case com.epidata.lib.models.MeasurementCleansed.DBTableName =>
        val m = rowToSensorMeasurementCleansed(row)
        toJLinkedHashMap(m)

      case com.epidata.lib.models.Measurement.DBTableName =>
        val m = rowToSensorMeasurement(row)
        toJLinkedHashMap(m)
    }
  }

  import com.epidata.lib.models.util.JsonHelpers._

  def toJson(m: SensorMeasurement): String = {
    val map = toJLinkedHashMap(m)
    JSONObject.toJSONString(map)
  }

  def toJson(sensorMeasurements: List[SensorMeasurement]): String = {
    import scala.collection.JavaConverters._
    val arr = new JLinkedList[JLinkedHashMap[String, Object]]()
    arr.addAll(
      sensorMeasurements
        .map(m => toJLinkedHashMap(m))
        .asJavaCollection)
    JSONArray.toJSONString(arr)
  }

  def toJLinkedHashMap(m: SensorMeasurement): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    putToMap(map, "company", m.company)
    putToMap(map, "site", m.site)
    putToMap(map, "station", m.station)
    putToMap(map, "sensor", m.sensor)
    if (m.ts != null)
      putToMap(map, "ts", convertToJLong(m.ts.getTime))
    putToMap(map, "event", m.event)
    putToMap(map, "meas_name", m.meas_name)
    putOptionToMap(map, "meas_unit", m.meas_unit)
    putOptionToMap(map, "meas_status", m.meas_status)
    putOptionToMap(map, "meas_description", m.meas_description)

    putOptionToMap(map, "meas_datatype", m.meas_datatype)
    if (m.meas_value != null)
      putAnyToMap(map, "meas_value", m.meas_value)
    putOptionAnyValToMap(map, "meas_lower_limit", m.meas_lower_limit)
    putOptionAnyValToMap(map, "meas_upper_limit", m.meas_upper_limit)

    map
  }

  def toJLinkedHashMap(m: SensorMeasurementCleansed): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    putToMap(map, "company", m.company)
    putToMap(map, "site", m.site)
    putToMap(map, "station", m.station)
    putToMap(map, "sensor", m.sensor)
    if (m.ts != null)
      putToMap(map, "ts", convertToJLong(m.ts.getTime))
    putToMap(map, "event", m.event)
    putToMap(map, "meas_name", m.meas_name)
    putOptionToMap(map, "meas_unit", m.meas_unit)
    putOptionToMap(map, "meas_status", m.meas_status)
    putOptionToMap(map, "meas_description", m.meas_description)
    putOptionToMap(map, "meas_flag", m.meas_flag)
    putOptionToMap(map, "meas_method", m.meas_method)

    putOptionToMap(map, "meas_datatype", m.meas_datatype)
    if (m.meas_value != null)
      putAnyToMap(map, "meas_value", m.meas_value)
    putOptionAnyValToMap(map, "meas_lower_limit", m.meas_lower_limit)
    putOptionAnyValToMap(map, "meas_upper_limit", m.meas_upper_limit)

    map
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

  def jsonToSensorMeasurement(str: String): Option[SensorMeasurement] = {
    fromJson(str) match {
      case Some(jSONObject) => Some(jsonToSensorMeasurement(jSONObject))
      case _ => None
    }
  }

  def jsonToSensorMeasurements(str: String): List[Option[SensorMeasurement]] = {
    fromJsonArray(str) match {
      case Some(jSONArray) => jSONArray.toArray.toList.map(
        x =>
          try {
            Some(jsonToSensorMeasurement(x.asInstanceOf[JSONObject]))
          } catch {
            case _: Throwable => None
          })
      case _ => List.empty
    }
  }

  def jsonToSensorMeasurement(jSONObject: JSONObject): SensorMeasurement = {
    val company: String = jSONObject.get("company").asInstanceOf[String]
    val site: String = jSONObject.get("site").asInstanceOf[String]
    val station: String = jSONObject.get("station").asInstanceOf[String]
    val sensor: String = jSONObject.get("sensor").asInstanceOf[String]
    val ts: Date = new Date(jSONObject.get("ts").asInstanceOf[Long])
    val event: String = jSONObject.get("event").asInstanceOf[String]
    val meas_name: String = jSONObject.get("meas_name").asInstanceOf[String]

    val meas_unit: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_unit").asInstanceOf[String])
    val meas_status: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_status").asInstanceOf[String])
    val meas_description: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_description").asInstanceOf[String])

    val meas_value_jsonObject = jSONObject.get("meas_value")
    val meas_lower_limit_jsonObject = jSONObject.get("meas_lower_limit")
    val meas_upper_limit_jsonObject = jSONObject.get("meas_upper_limit")

    val datatype_str = jSONObject.get("meas_datatype") match {
      case x: String if (x != null) => Some(x)
      case _ => None
    }

    val datatype = datatype_str match {
      case Some(x) if Datatype.isValidName(x) => Datatype.byName(x)
      case _ => null
    }

    val (meas_value, meas_lower_limit, meas_upper_limit, isInvalid) = TypeUtils.getMeasValues(datatype, meas_value_jsonObject, meas_lower_limit_jsonObject, meas_upper_limit_jsonObject)

    if (isInvalid)
      throw new Exception("invalid json format!")

    SensorMeasurement(
      company,
      site,
      station,
      sensor,
      ts,
      event,
      meas_name,
      datatype_str,
      meas_value,
      meas_unit,
      meas_status,
      meas_lower_limit,
      meas_upper_limit,
      meas_description)

  }

}
