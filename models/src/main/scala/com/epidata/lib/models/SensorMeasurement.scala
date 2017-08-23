/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models

import java.util.Date

/**
 * Specialization of Measurement representing sensor data.
 *
 * @param TODO
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
  meas_description: Option[String]
)

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
  meas_description: Option[String]
)

object SensorMeasurement {

  def convertSensorMeasurementCleansedToSensorMeasurement(m: SensorMeasurementCleansed): SensorMeasurement =
    SensorMeasurement(
      m.company,
      m.site,
      m.station,
      m.sensor,
      m.ts,
      m.event,
      m.meas_name,
      m.meas_datatype,
      m.meas_value,
      m.meas_unit,
      m.meas_status,
      m.meas_lower_limit,
      m.meas_upper_limit,
      m.meas_description
    )

  def convertSensorMeasurementToMeasurement(
    sensorMeasurement: SensorMeasurement
  ): Measurement =
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
      None
    )

  def convertMeasurementToSensorMeasurement(measurement: Measurement): SensorMeasurement =
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
      measurement.meas_description
    )

  def convertMeasurementCleansedToSensorMeasurementCleansed(measurement: MeasurementCleansed): SensorMeasurementCleansed =
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
      measurement.meas_description
    )

  implicit def sensorMeasurementToMeasurement(
    sensorMeasurement: SensorMeasurement
  ): Measurement =
    convertSensorMeasurementToMeasurement(sensorMeasurement)

  implicit def measurementToSensorMeasurement(measurement: Measurement): SensorMeasurement =
    convertMeasurementToSensorMeasurement(measurement)

  implicit def measurementCleansedToSensorMeasurementCleansed(measurement: MeasurementCleansed): SensorMeasurementCleansed =
    convertMeasurementCleansedToSensorMeasurementCleansed(measurement)

  implicit def sensorMeasurementToMeasurement(
    sensorMeasurements: List[SensorMeasurement]
  ): List[Measurement] =
    sensorMeasurements.map(sensorMeasurement => convertSensorMeasurementToMeasurement(sensorMeasurement))

  implicit def measurementToSensorMeasurement(measurements: List[Measurement]): List[SensorMeasurement] =
    measurements.map(measurement => convertMeasurementToSensorMeasurement(measurement))

}
