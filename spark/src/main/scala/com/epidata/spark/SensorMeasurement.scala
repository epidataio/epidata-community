/*
 * Copyright (c) 2015-2020 EpiData, Inc.
*/

package com.epidata.spark

import java.sql.Timestamp
import com.epidata.spark.utils.ConvertUtils
import com.epidata.spark.models.MeasurementSummary

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

  implicit def keyToSensorMeasurement(key: MeasurementLiteKey): SensorMeasurementKey =
    SensorMeasurementKey(
      key.customer,
      key.customer_site,
      key.collection,
      key.dataset)
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

  implicit def measureliteToSensorMeasurement(measurementlite: MeasurementLite): SensorMeasurement =
    SensorMeasurement(
      measurementlite.customer,
      measurementlite.customer_site,
      measurementlite.collection,
      measurementlite.dataset,
      measurementlite.ts,
      ConvertUtils.optionNoneToString(measurementlite.key1),
      ConvertUtils.optionNoneToString(measurementlite.key2),
      measurementlite.meas_datatype,
      measurementlite.meas_value,
      measurementlite.meas_unit,
      measurementlite.meas_status,
      measurementlite.meas_lower_limit,
      measurementlite.meas_upper_limit,
      measurementlite.meas_description)

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

  implicit def measureliteCleansedToSensorMeasurementCleansed(measureliteCleansed: MeasurementLiteCleansed): SensorMeasurementCleansed =
    SensorMeasurementCleansed(
      measureliteCleansed.customer,
      measureliteCleansed.customer_site,
      measureliteCleansed.collection,
      measureliteCleansed.dataset,
      measureliteCleansed.ts,
      measureliteCleansed.key1.get,
      measureliteCleansed.key2.get,
      measureliteCleansed.meas_datatype,
      measureliteCleansed.meas_value,
      measureliteCleansed.meas_unit,
      measureliteCleansed.meas_status,
      measureliteCleansed.meas_flag,
      measureliteCleansed.meas_method,
      measureliteCleansed.meas_lower_limit,
      measureliteCleansed.meas_upper_limit,
      measureliteCleansed.meas_description)
}
