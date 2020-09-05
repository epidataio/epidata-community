/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark.models

import java.sql.Timestamp

import org.apache.spark.sql.Row
import com.epidata.lib.models.util.{ Datatype, TypeUtils, JsonHelpers, Binary }
import java.util.{ Date, Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import java.util.Date

import org.json.simple.{ JSONArray, JSONObject }

/**
 * Specialization of Measurement representing automated test data.
 *
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
    meas_datatype: Option[String],
    meas_value: Any,
    meas_unit: Option[String],
    meas_status: Option[String],
    meas_lower_limit: Option[AnyVal],
    meas_upper_limit: Option[AnyVal],
    meas_description: Option[String],
    device_status: Option[String],
    test_status: Option[String])

case class AutomatedTestCleansed(
    company: String,
    site: String,
    device_group: String,
    tester: String,
    ts: Date,
    device_name: String,
    test_name: String,
    meas_name: String,
    meas_datatype: Option[String],
    meas_value: Any,
    meas_unit: Option[String],
    meas_status: Option[String],
    meas_flag: Option[String],
    meas_method: Option[String],
    meas_lower_limit: Option[AnyVal],
    meas_upper_limit: Option[AnyVal],
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

  def rowToAutomatedTest(row: Row): AutomatedTest = MeasureLite.rowToMeasurement(row)
  def rowToAutomatedTestCleansed(row: Row): AutomatedTestCleansed = MeasureLiteCleansed.rowToMeasurementCleansed(row)
  def rowToAutomatedTestSummary(row: Row): AutomatedTestSummary = MeasurementSummary.rowToMeasurementSummary(row)

  implicit def measurementToAutomatedTest(measurement: MeasureLite): AutomatedTest =
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

  implicit def measurementCleansedToAutomatedTestCleansed(measurement: MeasureLiteCleansed): AutomatedTestCleansed =
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

  implicit def automatedTestToMeasurement(automatedTest: AutomatedTest): MeasureLite =
    Measurement(
      automatedTest.company,
      automatedTest.site,
      automatedTest.device_group,
      automatedTest.tester,
      automatedTest.ts,
      Some(automatedTest.device_name),
      Some(automatedTest.test_name),
      Some(automatedTest.meas_name),
      automatedTest.meas_datatype,
      automatedTest.meas_value,
      automatedTest.meas_unit,
      automatedTest.meas_status,
      automatedTest.meas_lower_limit,
      automatedTest.meas_upper_limit,
      automatedTest.meas_description,
      automatedTest.device_status,
      automatedTest.test_status)

  // JSON Helpers
  def rowToJLinkedHashMap(row: Row, tableName: String): JLinkedHashMap[String, Object] = {
    tableName match {
      case MeasurementSummary.DBTableName =>
        val m = rowToAutomatedTestSummary(row)
        toJLinkedHashMap(m)

      case com.epidata.lib.models.MeasurementCleansed.DBTableName =>
        val m = rowToAutomatedTestCleansed(row)
        toJLinkedHashMap(m)

      case com.epidata.lib.models.Measurement.DBTableName =>
        val m = rowToAutomatedTest(row)
        toJLinkedHashMap(m)
    }
  }

  // JSON Helpers
  def rowToJLinkedHashMap(row: ResultSet, tableName: String): JLinkedHashMap[String, Object] = {
    tableName match {
      case MeasurementSummary.DBTableName =>
        val m = rowToAutomatedTestSummary(row)
        toJLinkedHashMap(m)

      case com.epidata.lib.models.MeasurementCleansed.DBTableName =>
        val m = rowToAutomatedTestCleansed(row)
        toJLinkedHashMap(m)

      case com.epidata.lib.models.Measurement.DBTableName =>
        val m = rowToAutomatedTest(row)
        toJLinkedHashMap(m)
    }
  }

  import com.epidata.lib.models.util.JsonHelpers._

  def toJLinkedHashMap(m: AutomatedTest): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    putToMap(map, "company", m.company)
    putToMap(map, "site", m.site)
    putToMap(map, "device_group", m.device_group)
    putToMap(map, "tester", m.tester)
    if (m.ts != null)
      putToMap(map, "ts", convertToJLong(m.ts.getTime))
    putToMap(map, "device_name", m.device_name)
    putToMap(map, "test_name", m.test_name)
    putToMap(map, "meas_name", m.meas_name)
    putOptionToMap(map, "meas_datatype", m.meas_datatype)

    if (m.meas_value != null)
      putAnyToMap(map, "meas_value", m.meas_value)

    putOptionToMap(map, "meas_unit", m.meas_unit)
    putOptionToMap(map, "meas_status", m.meas_status)
    putOptionAnyValToMap(map, "meas_lower_limit", m.meas_lower_limit)
    putOptionAnyValToMap(map, "meas_upper_limit", m.meas_upper_limit)

    putOptionToMap(map, "meas_description", m.meas_description)
    putOptionToMap(map, "device_status", m.device_status)
    putOptionToMap(map, "test_status", m.test_status)

    map
  }

  def toJLinkedHashMap(m: AutomatedTestCleansed): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    putToMap(map, "company", m.company)
    putToMap(map, "site", m.site)
    putToMap(map, "device_group", m.device_group)
    putToMap(map, "tester", m.tester)
    if (m.ts != null)
      putToMap(map, "ts", convertToJLong(m.ts.getTime))
    putToMap(map, "device_name", m.device_name)
    putToMap(map, "test_name", m.test_name)
    putToMap(map, "meas_name", m.meas_name)
    putOptionToMap(map, "meas_datatype", m.meas_datatype)

    if (m.meas_value != null)
      putAnyToMap(map, "meas_value", m.meas_value)

    putOptionToMap(map, "meas_unit", m.meas_unit)
    putOptionToMap(map, "meas_status", m.meas_status)

    putOptionToMap(map, "meas_flag", m.meas_flag)
    putOptionToMap(map, "meas_method", m.meas_method)

    putOptionAnyValToMap(map, "meas_lower_limit", m.meas_lower_limit)
    putOptionAnyValToMap(map, "meas_upper_limit", m.meas_upper_limit)

    putOptionToMap(map, "meas_description", m.meas_description)
    putOptionToMap(map, "device_status", m.device_status)
    putOptionToMap(map, "test_status", m.test_status)

    map
  }

  def toJLinkedHashMap(m: AutomatedTestSummary): JLinkedHashMap[String, Object] = {
    val map = new JLinkedHashMap[String, Object]()

    putToMap(map, "company", m.company)
    putToMap(map, "site", m.site)
    putToMap(map, "device_group", m.device_group)
    putToMap(map, "tester", m.tester)

    if (m.start_time != null)
      putToMap(map, "start_time", convertToJLong(m.start_time.getTime))

    if (m.stop_time != null)
      putToMap(map, "stop_time", convertToJLong(m.stop_time.getTime))

    putToMap(map, "device_name", m.device_name)
    putToMap(map, "test_name", m.test_name)
    putToMap(map, "meas_name", m.meas_name)
    putToMap(map, "meas_summary_name", m.meas_summary_name)
    putToMap(map, "meas_summary_value", m.meas_summary_value)
    putToMap(map, "meas_summary_description", m.meas_summary_description)

    map
  }

  def jsonToAutomatedTest(str: String): Option[AutomatedTest] = {
    fromJson(str) match {
      case Some(jSONObject) => Some(jsonToAutomatedTest(jSONObject))
      case _ => None
    }
  }

  def jsonToAutomatedTests(str: String): List[Option[AutomatedTest]] = {
    fromJsonArray(str) match {
      case Some(jSONArray) => jSONArray.toArray.toList.map(
        x =>
          try {
            Some(jsonToAutomatedTest(x.asInstanceOf[JSONObject]))
          } catch {
            case _: Throwable => None
          })
      case _ => List.empty
    }
  }

  def toJson(m: AutomatedTest): String = {
    val map = toJLinkedHashMap(m)
    JSONObject.toJSONString(map)
  }

  def toJson(list: List[AutomatedTest]): String = {
    import scala.collection.JavaConverters._
    val arr = new JLinkedList[JLinkedHashMap[String, Object]]()
    arr.addAll(
      list
        .map(m => toJLinkedHashMap(m))
        .asJavaCollection)
    JSONArray.toJSONString(arr)
  }

  def jsonToAutomatedTest(jSONObject: JSONObject): AutomatedTest = {

    val company: String = jSONObject.get("company").asInstanceOf[String]
    val site: String = jSONObject.get("site").asInstanceOf[String]
    val device_group: String = jSONObject.get("device_group").asInstanceOf[String]
    val tester: String = jSONObject.get("tester").asInstanceOf[String]
    val ts: Date = new Date(jSONObject.get("ts").asInstanceOf[Long])
    val device_name: String = jSONObject.get("device_name").asInstanceOf[String]
    val test_name: String = jSONObject.get("test_name").asInstanceOf[String]
    val meas_name: String = jSONObject.get("meas_name").asInstanceOf[String]

    val meas_unit: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_unit").asInstanceOf[String])
    val meas_status: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_status").asInstanceOf[String])
    val meas_description: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_description").asInstanceOf[String])
    val device_status: Option[String] = TypeUtils.blankToNone(jSONObject.get("device_status").asInstanceOf[String])
    val test_status: Option[String] = TypeUtils.blankToNone(jSONObject.get("test_status").asInstanceOf[String])

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

    AutomatedTest(
      company,
      site,
      device_group,
      tester,
      ts,
      device_name,
      test_name,
      meas_name,
      datatype_str,
      meas_value,
      meas_unit,
      meas_status,
      meas_lower_limit,
      meas_upper_limit,
      meas_description,
      device_status,
      test_status)

  }

}
