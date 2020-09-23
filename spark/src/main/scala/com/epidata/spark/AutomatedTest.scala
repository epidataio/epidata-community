/*
 * Copyright (c) 2015-2020 EpiData, Inc.
*/

package com.epidata.spark

import java.sql.Timestamp

import org.apache.spark.MeasurementValue
import com.epidata.spark.models.MeasurementSummary

// Spark-specific class adapted from epidata.models
/** Specialization of MeasurementKey representing an automated test key. */
case class AutomatedTestKey(
    company: String,
    site: String,
    device_group: String,
    tester: String)

object AutomatedTestKey {

  implicit def keyToAutomatedTest(key: MeasurementKey): AutomatedTestKey =
    AutomatedTestKey(
      key.customer,
      key.customer_site,
      key.collection,
      key.dataset)

  implicit def keyToAutomatedTest(key: MeasurementLiteKey): AutomatedTestKey =
    AutomatedTestKey(
      key.customer,
      key.customer_site,
      key.collection,
      key.dataset)
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
    meas_datatype: Option[String],
    meas_value: MeasurementValue,
    meas_unit: Option[String],
    meas_status: Option[String],
    meas_lower_limit: Option[MeasurementValue],
    meas_upper_limit: Option[MeasurementValue],
    meas_description: Option[String],
    device_status: Option[String],
    test_status: Option[String])

case class AutomatedTestSummary(
    company: String,
    site: String,
    device_group: String,
    tester: String,
    start_time: Timestamp,
    stop_time: Timestamp,
    device_name: String,
    test_name: String,
    meas_name: String,
    meas_summary_name: String,
    meas_summary_value: String,
    meas_summary_description: String)

object AutomatedTest {
  val NAME: String = "automated_test"
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
      measurement.meas_datatype,
      measurement.meas_value,
      measurement.meas_unit,
      measurement.meas_status,
      measurement.meas_lower_limit,
      measurement.meas_upper_limit,
      measurement.meas_description,
      measurement.val1,
      measurement.val2)

  implicit def measliteToAutomatedTest(measurement: MeasurementLite): AutomatedTest =
    AutomatedTest(
      measurement.customer,
      measurement.customer_site,
      measurement.collection,
      measurement.dataset,
      measurement.ts,
      measurement.key1.get,
      measurement.key2.get,
      measurement.key3.get,
      measurement.meas_datatype,
      measurement.meas_value,
      measurement.meas_unit,
      measurement.meas_status,
      measurement.meas_lower_limit,
      measurement.meas_upper_limit,
      measurement.meas_description,
      measurement.val1,
      measurement.val2)

  implicit def measurementSummaryToAutomatedTestSummary(ms: MeasurementSummary): AutomatedTestSummary =
    AutomatedTestSummary(
      ms.customer,
      ms.customer_site,
      ms.collection,
      ms.dataset,
      ms.start_time,
      ms.stop_time,
      ms.key1,
      ms.key2,
      ms.key3,
      ms.meas_summary_name,
      ms.meas_summary_value,
      ms.meas_summary_description)
}

case class AutomatedTestCleansed(
    company: String,
    site: String,
    device_group: String,
    tester: String,
    ts: Timestamp,
    device_name: String,
    test_name: String,
    meas_name: String,
    meas_datatype: Option[String],
    meas_value: MeasurementValue,
    meas_unit: Option[String],
    meas_status: Option[String],
    meas_flag: Option[String],
    meas_method: Option[String],
    meas_lower_limit: Option[MeasurementValue],
    meas_upper_limit: Option[MeasurementValue],
    meas_description: Option[String],
    device_status: Option[String],
    test_status: Option[String])

object AutomatedTestCleansed {

  implicit def measurementCleansedToAutomatedTestCleansed(measurement: MeasurementCleansed): AutomatedTestCleansed =
    AutomatedTestCleansed(
      measurement.customer,
      measurement.customer_site,
      measurement.collection,
      measurement.dataset,
      measurement.ts,
      measurement.key1.get,
      measurement.key2.get,
      measurement.key3.get,
      measurement.meas_datatype,
      measurement.meas_value,
      measurement.meas_unit,
      measurement.meas_status,
      measurement.meas_flag,
      measurement.meas_method,
      measurement.meas_lower_limit,
      measurement.meas_upper_limit,
      measurement.meas_description,
      measurement.val1,
      measurement.val2)

  implicit def measureliteCleansedToAutomatedTestCleansed(measurement: MeasurementLiteCleansed): AutomatedTestCleansed =
    AutomatedTestCleansed(
      measurement.customer,
      measurement.customer_site,
      measurement.collection,
      measurement.dataset,
      measurement.ts,
      measurement.key1.get,
      measurement.key2.get,
      measurement.key3.get,
      measurement.meas_datatype,
      measurement.meas_value,
      measurement.meas_unit,
      measurement.meas_status,
      measurement.meas_flag,
      measurement.meas_method,
      measurement.meas_lower_limit,
      measurement.meas_upper_limit,
      measurement.meas_description,
      measurement.val1,
      measurement.val2)
}
