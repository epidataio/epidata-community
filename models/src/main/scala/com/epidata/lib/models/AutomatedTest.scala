/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models

import com.epidata.lib.models.util.Binary
import java.util.Date

/**
 * Specialization of Measurement representing automated test data.
 *
 * @param TODO
 * @param meas_lower_limit Lower limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 * @param meas_upper_limit Upper limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 */
case class AutomatedTest(
  company: String,
  site: String,
  device_group: String,
  tester: String,
  ts: Date,
  device_name: String,
  test_name: String,
  meas_name: String,
  meas_value: Any,
  meas_unit: Option[String],
  meas_status: Option[String],
  meas_lower_limit: Option[AnyVal],
  meas_upper_limit: Option[AnyVal],
  meas_description: Option[String],
  device_status: Option[String],
  test_status: Option[String]
)

object AutomatedTest {

  implicit def automatedTestToMeasurement(automatedTest: AutomatedTest): Measurement =
    Measurement(
      automatedTest.company,
      automatedTest.site,
      automatedTest.device_group,
      automatedTest.tester,
      automatedTest.ts,
      Some(automatedTest.device_name),
      Some(automatedTest.test_name),
      Some(automatedTest.meas_name),
      automatedTest.meas_value,
      automatedTest.meas_unit,
      automatedTest.meas_status,
      automatedTest.meas_lower_limit,
      automatedTest.meas_upper_limit,
      automatedTest.meas_description,
      automatedTest.device_status,
      automatedTest.test_status
    )

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
