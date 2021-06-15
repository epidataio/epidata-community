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
 * Specialization of MeasurementCleansed representing sensor data.
 *
 * @param meas_lower_limit Lower limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 * @param meas_upper_limit Upper limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 */
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

object SensorMeasurementCleansed {

  val NAME: String = "sensor_measurement_cleansed"

  // Model Conversions
  def rowToSensorMeasurementCleansed(row: Row): SensorMeasurementCleansed = MeasurementCleansed.rowToMeasurementCleansed(row)

  // Model Conversions for SQLite
  def rowToSensorMeasurementCleansed(row: ResultSet): SensorMeasurementCleansed = MeasurementCleansed.rowToMeasurementCleansed(row)

  implicit def measurementCleansedToSensorMeasurementCleansed(measurementCleansed: MeasurementCleansed): SensorMeasurementCleansed =
    SensorMeasurementCleansed(
      measurementCleansed.customer,
      measurementCleansed.customer_site,
      measurementCleansed.collection,
      measurementCleansed.dataset,
      measurementCleansed.ts,
      measurementCleansed.key1.get,
      measurementCleansed.key2.get,
      measurementCleansed.meas_datatype,
      measurementCleansed.meas_value,
      measurementCleansed.meas_unit,
      measurementCleansed.meas_status,
      measurementCleansed.meas_flag,
      measurementCleansed.meas_method,
      measurementCleansed.meas_lower_limit,
      measurementCleansed.meas_upper_limit,
      measurementCleansed.meas_description)

  implicit def sensorMeasurementCleansedToMeasurementCleansed(sensorMeasurementCleansed: SensorMeasurementCleansed): MeasurementCleansed =
    MeasurementCleansed(
      sensorMeasurementCleansed.company,
      sensorMeasurementCleansed.site,
      sensorMeasurementCleansed.station,
      sensorMeasurementCleansed.sensor,
      sensorMeasurementCleansed.ts,
      Some(sensorMeasurementCleansed.event),
      Some(sensorMeasurementCleansed.meas_name),
      None,
      sensorMeasurementCleansed.meas_datatype,
      sensorMeasurementCleansed.meas_value,
      sensorMeasurementCleansed.meas_unit,
      sensorMeasurementCleansed.meas_status,
      sensorMeasurementCleansed.meas_flag,
      sensorMeasurementCleansed.meas_method,
      sensorMeasurementCleansed.meas_lower_limit,
      sensorMeasurementCleansed.meas_upper_limit,
      sensorMeasurementCleansed.meas_description,
      None,
      None)

  implicit def sensorMeasurementCleansedToMeasurementCleansed(sensorMeasurementsCleansed: List[SensorMeasurementCleansed]): List[MeasurementCleansed] =
    sensorMeasurementsCleansed.map(sensorMeasurementCleansed => sensorMeasurementCleansedToMeasurementCleansed(sensorMeasurementCleansed))

  implicit def measurementCleansedToSensorMeasurementCleansed(measurementsCleansed: List[MeasurementCleansed]): List[SensorMeasurementCleansed] =
    measurementsCleansed.map(measurementCleansed => measurementCleansedToSensorMeasurementCleansed(measurementCleansed))

  // JSON Helpers
  def rowToJLinkedHashMap(rowCleansed: Row, tableName: String): JLinkedHashMap[String, Object] = {
    tableName match {
      case com.epidata.lib.models.MeasurementCleansed.DBTableName =>
        val mc = rowToSensorMeasurementCleansed(rowCleansed)
        toJLinkedHashMap(mc)
    }
  }

  // JSON Helpers for SQLite
  def rowToJLinkedHashMap(rowCleansed: ResultSet, tableName: String): JLinkedHashMap[String, Object] = {
    tableName match {
      case com.epidata.lib.models.MeasurementCleansed.DBTableName =>
        val mc = rowToSensorMeasurementCleansed(rowCleansed)
        toJLinkedHashMap(mc)
    }
  }

  import com.epidata.lib.models.util.JsonHelpers._

  def toJson(mc: SensorMeasurementCleansed): String = {
    val map = toJLinkedHashMap(mc)
    JSONObject.toJSONString(map)
  }

  def toJson(sensorMeasurementsCleansed: List[SensorMeasurementCleansed]): String = {
    import scala.collection.JavaConverters._
    val arr = new JLinkedList[JLinkedHashMap[String, Object]]()
    arr.addAll(
      sensorMeasurementsCleansed
        .map(mc => toJLinkedHashMap(mc))
        .asJavaCollection)
    JSONArray.toJSONString(arr)
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

  def jsonToSensorMeasurementCleansed(str: String): Option[SensorMeasurementCleansed] = {
    fromJson(str) match {
      case Some(jSONObject) => Some(jsonToSensorMeasurementCleansed(jSONObject))
      case _ => None
    }
  }

  def jsonToSensorMeasurementsCleansed(str: String): List[Option[SensorMeasurementCleansed]] = {
    fromJsonArray(str) match {
      case Some(jSONArray) => jSONArray.toArray.toList.map(
        x =>
          try {
            Some(jsonToSensorMeasurementCleansed(x.asInstanceOf[JSONObject]))
          } catch {
            case _: Throwable => None
          })
      case _ => List.empty
    }
  }

  def jsonToSensorMeasurementCleansed(jSONObject: JSONObject): SensorMeasurementCleansed = {
    val company: String = jSONObject.get("company").asInstanceOf[String]
    val site: String = jSONObject.get("site").asInstanceOf[String]
    val station: String = jSONObject.get("station").asInstanceOf[String]
    val sensor: String = jSONObject.get("sensor").asInstanceOf[String]
    val ts: Date = new Date(jSONObject.get("ts").asInstanceOf[Long])
    val event: String = jSONObject.get("event").asInstanceOf[String]
    val meas_name: String = jSONObject.get("meas_name").asInstanceOf[String]

    val meas_unit: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_unit").asInstanceOf[String])
    val meas_status: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_status").asInstanceOf[String])

    val meas_flag: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_flag").asInstanceOf[String])
    val meas_method: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_method").asInstanceOf[String])

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

    SensorMeasurementCleansed(
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
      meas_flag,
      meas_method,
      meas_lower_limit,
      meas_upper_limit,
      meas_description)
  }

  def getColumns: Set[String] = {
    val col_set = Set(
      "company",
      "site",
      "station",
      "sensor",
      "ts",
      "event",
      "meas_name",
      "datatype_str",
      "meas_value",
      "meas_unit",
      "meas_status",
      "meas_flag",
      "meas_method",
      "meas_lower_limit",
      "meas_upper_limit",
      "meas_description")
    col_set
  }

}
