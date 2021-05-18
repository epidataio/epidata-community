package com.epidata.spark

import java.sql.Timestamp
import com.epidata.spark.utils.ConvertUtils
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import com.epidata.lib.models.util.JsonHelpers._
import org.apache.spark.MeasurementValue

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

  implicit def toJLinkedHashMap(key: AutomatedTestKey): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    putToMap(map, "company", key.company)
    putToMap(map, "site", key.site)
    putToMap(map, "device_group", key.device_group)
    putToMap(map, "tester", key.tester)

    map
  }
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
      measurement.meas_datatype,
      measurement.meas_value,
      measurement.meas_unit,
      measurement.meas_status,
      measurement.meas_lower_limit,
      measurement.meas_upper_limit,
      measurement.meas_description,
      measurement.val1,
      measurement.val2)
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
}
