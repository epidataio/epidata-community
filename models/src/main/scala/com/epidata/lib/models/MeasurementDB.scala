/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models

import java.sql.Timestamp
import java.util.Date

import com.datastax.driver.core.Row

case class MeasurementDB(
  customer: String,
  customer_site: String,
  collection: String,
  dataset: String,
  epoch: Int,
  ts: Timestamp,
  key1: String,
  key2: String,
  key3: String,
  meas_value: Option[Double],
  meas_value_l: Option[Long],
  meas_value_s: Option[String],
  meas_value_b: Option[Array[Byte]],
  meas_unit: Option[String],
  meas_status: Option[String],
  meas_lower_limit: Option[Double],
  meas_lower_limit_l: Option[Long],
  meas_upper_limit: Option[Double],
  meas_upper_limit_l: Option[Long],
  meas_description: Option[String],
  val1: Option[String],
  val2: Option[String]
)

object MeasurementDB {
  val FieldNames: List[String] =
    List(
      "customer",
      "customer_site",
      "collection",
      "dataset",
      "epoch",
      "ts",
      "key1",
      "key2",
      "key3",
      "meas_value",
      "meas_value_l",
      "meas_value_s",
      "meas_value_b",
      "meas_unit",
      "meas_status",
      "meas_lower_limit",
      "meas_lower_limit_l",
      "meas_upper_limit",
      "meas_upper_limit_l",
      "meas_description",
      "val1",
      "val2"
    )
}

case class MeasurementCleansed(
    customer: String,
    customer_site: String,
    collection: String,
    dataset: String,
    ts: Date,
    key1: Option[String],
    key2: Option[String],
    key3: Option[String],
    meas_value: Any,
    meas_unit: Option[String],
    meas_status: Option[String],
    meas_flag: Option[String],
    meas_method: Option[String],
    meas_lower_limit: Option[AnyVal],
    meas_upper_limit: Option[AnyVal],
    meas_description: Option[String],
    val1: Option[String],
    val2: Option[String]
) {

  // Splitting timeseries by epoch keeps partitions from growing beyond
  // capacity. The epoch is computed directly from the timestamp.
  lazy val epoch = Measurement.epochForTs(ts)
}

object MeasurementCleansed {

  val DBTableName: String = "measurements_cleansed"
  val KafkaTopic: String = DBTableName

  val FieldNames: List[String] =
    List(
      "customer",
      "customer_site",
      "collection",
      "dataset",
      "ts",
      "key1",
      "key2",
      "key3",
      "meas_value",
      "meas_value_l",
      "meas_value_s",
      "meas_value_b",
      "meas_unit",
      "meas_status",
      "meas_flag",
      "meas_method",
      "meas_lower_limit",
      "meas_lower_limit_l",
      "meas_upper_limit",
      "meas_upper_limit_l",
      "meas_description",
      "val1",
      "val2"
    )

  implicit def rowToMeasurementCleansed(row: Row): MeasurementCleansed = {
    val m = Measurement.rowToMeasurement(row)
    MeasurementCleansed(
      m.customer,
      m.customer_site,
      m.collection,
      m.dataset,
      m.ts,
      m.key1,
      m.key2,
      m.key3,
      m.meas_value,
      m.meas_unit,
      m.meas_status,
      Measurement.stringToOption(row.getString("meas_flag")),
      Measurement.stringToOption(row.getString("meas_method")),
      m.meas_lower_limit,
      m.meas_upper_limit,
      m.meas_description,
      m.val1,
      m.val2
    )

  }
}
