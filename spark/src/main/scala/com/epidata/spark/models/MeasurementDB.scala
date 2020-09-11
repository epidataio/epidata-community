package com.epidata.spark.models

import java.sql.Timestamp

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
    meas_datatype: Option[String],
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
    val2: Option[String])

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
      "meas_datatype",
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
      "val2")
}
