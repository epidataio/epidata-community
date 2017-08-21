/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark

import com.datastax.driver.core.{ Row => CassandraRow }
import com.epidata.lib.models.{ Measurement => BaseMeasurement, MeasurementCleansed => BaseMeasurementCleansed }
import com.epidata.lib.models.util.{ Binary }
import java.sql.Timestamp
import java.util.Date
import com.epidata.spark.utils.ConvertUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BaseGenericInternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._

import org.apache.spark.MeasurementValue

/**
 * Model representing a measurement key stored in the database's
 * measurement_keys table. Each measurement key is a partition key value from
 * the database's measurements table.
 */
case class MeasurementKey(
  customer: String,
  customer_site: String,
  collection: String,
  dataset: String
)

/** Specialization of MeasurementKey representing an automated test key. */
case class AutomatedTestKey(
  company: String,
  site: String,
  device_group: String,
  tester: String
)

object AutomatedTestKey {

  implicit def keyToAutomatedTest(key: MeasurementKey): AutomatedTestKey =
    AutomatedTestKey(
      key.customer,
      key.customer_site,
      key.collection,
      key.dataset
    )
}

/** Specialization of MeasurementKey representing a sensor measurement key. */
case class SensorMeasurementKey(
  company: String,
  site: String,
  station: String,
  sensor: String
)

object SensorMeasurementKey {

  implicit def keyToSensorMeasurement(key: MeasurementKey): SensorMeasurementKey =
    SensorMeasurementKey(
      key.customer,
      key.customer_site,
      key.collection,
      key.dataset
    )
}

/**
 * Model representing a customer measurement stored in the database. The minor
 * differences from BaseMeasurement allow for integration with Spark SQL.
 */
case class Measurement(
  customer: String,
  customer_site: String,
  collection: String,
  dataset: String,
  ts: Timestamp,
  key1: Option[String],
  key2: Option[String],
  key3: Option[String],
  meas_datatype: Option[String],
  meas_value: MeasurementValue,
  meas_unit: Option[String],
  meas_status: Option[String],
  meas_lower_limit: Option[MeasurementValue],
  meas_upper_limit: Option[MeasurementValue],
  meas_description: Option[String],
  val1: Option[String],
  val2: Option[String]
)

object Measurement {

  // Splitting timeseries by epoch keeps partitions from growing beyond
  // capacity. The epoch is computed directly from the timestamp.
  def epochForTs(ts: Timestamp): Int = BaseMeasurement.epochForTs(new Date(ts.getTime))

  implicit def baseMeasurementToMeasurement(base: BaseMeasurement): Measurement =
    Measurement(
      base.customer,
      base.customer_site,
      base.collection,
      base.dataset,
      new Timestamp(base.ts.getTime),
      base.key1,
      base.key2,
      base.key3,
      base.meas_datatype,
      base.meas_value match {
        case b: Binary => MeasurementValue(b.backing)
        case v => MeasurementValue(v)
      },
      base.meas_unit,
      base.meas_status,
      base.meas_lower_limit.map(MeasurementValue(_)),
      base.meas_upper_limit.map(MeasurementValue(_)),
      base.meas_description,
      base.val1,
      base.val2
    )

  implicit def rowToMeasurement(row: CassandraRow): Measurement =
    baseMeasurementToMeasurement(row)
}

/** Specialization of Measurement representing automated test data. */
case class AutomatedTest(
  company: String,
  site: String,
  device_group: String,
  tester: String,
  ts: Timestamp,
  device_name: String,
  test_name: String,
  meas_name: String,
  meas_value: MeasurementValue,
  meas_unit: Option[String],
  meas_status: Option[String],
  meas_lower_limit: Option[MeasurementValue],
  meas_upper_limit: Option[MeasurementValue],
  meas_description: Option[String],
  device_status: Option[String],
  test_status: Option[String]
)

object AutomatedTest {

  implicit def measurementToAutomatedTest(measurement: Measurement): AutomatedTest =
    AutomatedTest(
      measurement.customer,
      measurement.customer_site,
      measurement.collection,
      measurement.dataset,
      measurement.ts,
      measurement.key1.get,
      measurement.key2.get,
      measurement.key3.get,
      measurement.meas_value,
      measurement.meas_unit,
      measurement.meas_status,
      measurement.meas_lower_limit,
      measurement.meas_upper_limit,
      measurement.meas_description,
      measurement.val1,
      measurement.val2
    )
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
  meas_description: Option[String]
)

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
      measurement.meas_description
    )
}

case class MeasurementCleansed(
  customer: String,
  customer_site: String,
  collection: String,
  dataset: String,
  ts: Timestamp,
  key1: Option[String],
  key2: Option[String],
  key3: Option[String],
  meas_datatype: Option[String],
  meas_value: MeasurementValue,
  meas_unit: Option[String],
  meas_status: Option[String],
  meas_flag: Option[String],
  meas_method: Option[String],
  meas_lower_limit: Option[MeasurementValue],
  meas_upper_limit: Option[MeasurementValue],
  meas_description: Option[String],
  val1: Option[String],
  val2: Option[String]
)

object MeasurementCleansed {

  // Splitting timeseries by epoch keeps partitions from growing beyond
  // capacity. The epoch is computed directly from the timestamp.
  def epochForTs(ts: Timestamp): Int = BaseMeasurement.epochForTs(new Date(ts.getTime))

  implicit def baseMeasurementCleansedToMeasurementCleansed(base: BaseMeasurementCleansed): MeasurementCleansed =
    MeasurementCleansed(
      base.customer,
      base.customer_site,
      base.collection,
      base.dataset,
      new Timestamp(base.ts.getTime),
      base.key1,
      base.key2,
      base.key3,
      base.meas_datatype,
      base.meas_value match {
        case b: Binary => MeasurementValue(b.backing)
        case v => MeasurementValue(v)
      },
      base.meas_unit,
      base.meas_status,
      base.meas_flag,
      base.meas_method,
      base.meas_lower_limit.map(MeasurementValue(_)),
      base.meas_upper_limit.map(MeasurementValue(_)),
      base.meas_description,
      base.val1,
      base.val2
    )

  implicit def rowToMeasurementCleansed(row: CassandraRow): MeasurementCleansed =
    baseMeasurementCleansedToMeasurementCleansed(row)
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
  meas_description: Option[String]
)

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
      measurement.meas_description
    )
}
