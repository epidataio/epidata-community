/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark

import java.sql.Timestamp
import com.epidata.spark.utils.ConvertUtils
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import com.epidata.lib.models.util.JsonHelpers._

import org.apache.spark.MeasurementValue

/** Specialization of MeasurementKey representing a sensor measurement key. */
case class SensorMeasurementKey(
    company: String,
    site: String,
    station: String,
    sensor: String)

object SensorMeasurementKey {

  implicit def keyToSensorMeasurement(key: MeasurementKey): SensorMeasurementKey =
    SensorMeasurementKey(
      key.customer,
      key.customer_site,
      key.collection,
      key.dataset)

  implicit def toJLinkedHashMap(key: SensorMeasurementKey): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    putToMap(map, "company", key.company)
    putToMap(map, "site", key.site)
    putToMap(map, "station", key.station)
    putToMap(map, "sensor", key.sensor)

    map
  }
}

/** Specialization of Measurement representing sensor data. */
case class SensorMeasurement(
    company: String,
    site: String,
    station: String,
    sensor: String,
    ts: Timestamp,
    event: String,
    meas_name: String,
    meas_datatype: Option[String],
    meas_value: MeasurementValue,
    meas_unit: Option[String],
    meas_status: Option[String],
    meas_lower_limit: Option[MeasurementValue],
    meas_upper_limit: Option[MeasurementValue],
    meas_description: Option[String])

object SensorMeasurement {

  implicit def measurementToSensorMeasurement(measurement: Measurement): SensorMeasurement =
    SensorMeasurement(
      measurement.customer,
      measurement.customer_site,
      measurement.collection,
      measurement.dataset,
      measurement.ts,
      ConvertUtils.optionNoneToString(measurement.key1),
      ConvertUtils.optionNoneToString(measurement.key2),
      measurement.meas_datatype,
      measurement.meas_value,
      measurement.meas_unit,
      measurement.meas_status,
      measurement.meas_lower_limit,
      measurement.meas_upper_limit,
      measurement.meas_description)
}

case class SensorMeasurementCleansed(
    company: String,
    site: String,
    station: String,
    sensor: String,
    ts: Timestamp,
    event: String,
    meas_name: String,
    meas_datatype: Option[String],
    meas_value: MeasurementValue,
    meas_unit: Option[String],
    meas_status: Option[String],
    meas_flag: Option[String],
    meas_method: Option[String],
    meas_lower_limit: Option[MeasurementValue],
    meas_upper_limit: Option[MeasurementValue],
    meas_description: Option[String])

object SensorMeasurementCleansed {
  implicit def measurementCleansedToSensorMeasurementCleansed(measurement: MeasurementCleansed): SensorMeasurementCleansed =
    SensorMeasurementCleansed(
      measurement.customer,
      measurement.customer_site,
      measurement.collection,
      measurement.dataset,
      measurement.ts,
      ConvertUtils.optionNoneToString(measurement.key1),
      ConvertUtils.optionNoneToString(measurement.key2),
      measurement.meas_datatype,
      measurement.meas_value,
      measurement.meas_unit,
      measurement.meas_status,
      measurement.meas_flag,
      measurement.meas_method,
      measurement.meas_lower_limit,
      measurement.meas_upper_limit,
      measurement.meas_description)
}
