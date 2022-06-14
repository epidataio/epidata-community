/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models

import java.sql.Timestamp

import com.datastax.driver.core.Row
import java.sql.ResultSet
import com.epidata.lib.models.util.{ Datatype, TypeUtils, JsonHelpers, Binary }
import java.util.{ Date, Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import java.util.Date

import org.json.simple.{ JSONArray, JSONObject }

/**
 * Specialization of MeasurementCleansed representing automated test cleansed data.
 *
 * @param meas_lower_limit Lower limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 * @param meas_upper_limit Upper limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 */
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

object AutomatedTestCleansed {

  val NAME: String = "automated_test_cleansed"

  def rowToAutomatedTestCleansed(row: Row): AutomatedTestCleansed = MeasurementCleansed.rowToMeasurementCleansed(row)

  def rowToAutomatedTestCleansed(row: ResultSet): AutomatedTestCleansed = MeasurementCleansed.rowToMeasurementCleansed(row)

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

  implicit def automatedTestCleansedToMeasurementCleansed(automatedTestCleansed: AutomatedTestCleansed): MeasurementCleansed =
    MeasurementCleansed(
      automatedTestCleansed.company,
      automatedTestCleansed.site,
      automatedTestCleansed.device_group,
      automatedTestCleansed.tester,
      automatedTestCleansed.ts,
      Some(automatedTestCleansed.device_name),
      Some(automatedTestCleansed.test_name),
      Some(automatedTestCleansed.meas_name),
      automatedTestCleansed.meas_datatype,
      automatedTestCleansed.meas_value,
      automatedTestCleansed.meas_unit,
      automatedTestCleansed.meas_status,
      automatedTestCleansed.meas_flag,
      automatedTestCleansed.meas_method,
      automatedTestCleansed.meas_lower_limit,
      automatedTestCleansed.meas_upper_limit,
      automatedTestCleansed.meas_description,
      automatedTestCleansed.device_status,
      automatedTestCleansed.test_status)

  implicit def automatedTestCleansedToMeasurementCleansed(automatedTestsCleansed: List[AutomatedTestCleansed]): List[MeasurementCleansed] =
    automatedTestsCleansed.map(automatedTestCleansed => automatedTestCleansedToMeasurementCleansed(automatedTestCleansed))

  implicit def measurementToAutomatedTest(measurementsCleansed: List[MeasurementCleansed]): List[AutomatedTestCleansed] =
    measurementsCleansed.map(measurementCleansed => measurementCleansedToAutomatedTestCleansed(measurementCleansed))

  // JSON Helpers
  def rowToJLinkedHashMap(rowCleansed: Row, tableName: String): JLinkedHashMap[String, Object] = {
    tableName match {
      case com.epidata.lib.models.MeasurementCleansed.DBTableName =>
        val mc = rowToAutomatedTestCleansed(rowCleansed)
        toJLinkedHashMap(mc)
    }
  }

  // JSON Helpers
  def rowToJLinkedHashMap(rowCleansed: ResultSet, tableName: String): JLinkedHashMap[String, Object] = {
    tableName match {
      case com.epidata.lib.models.MeasurementCleansed.DBTableName =>
        val mc = rowToAutomatedTestCleansed(rowCleansed)
        toJLinkedHashMap(mc)
    }
  }

  import com.epidata.lib.models.util.JsonHelpers._

  def toJson(mc: AutomatedTestCleansed): String = {
    val map = toJLinkedHashMap(mc)
    JSONObject.toJSONString(map)
  }

  def toJson(list: List[AutomatedTestCleansed]): String = {
    import scala.collection.JavaConverters._
    val arr = new JLinkedList[JLinkedHashMap[String, Object]]()
    arr.addAll(
      list
        .map(mc => toJLinkedHashMap(mc))
        .asJavaCollection)
    JSONArray.toJSONString(arr)
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

  def fromJLinkedHashMap(map: JLinkedHashMap[String, Object]): AutomatedTestCleansed = {
    val company: String = map.get("company").asInstanceOf[String]
    val site: String = map.get("site").asInstanceOf[String]
    val device_group: String = map.get("device_group").asInstanceOf[String]
    val tester: String = map.get("tester").asInstanceOf[String]
    val ts: Date = new Date(map.get("ts").asInstanceOf[Long])
    val device_name: String = map.get("device_name").asInstanceOf[String]
    val test_name: String = map.get("test_name").asInstanceOf[String]
    val meas_name: String = map.get("meas_name").asInstanceOf[String]

    val meas_unit: Option[String] = TypeUtils.blankToNone(map.get("meas_unit").asInstanceOf[String])
    val meas_status: Option[String] = TypeUtils.blankToNone(map.get("meas_status").asInstanceOf[String])

    val meas_flag: Option[String] = TypeUtils.blankToNone(map.get("meas_flag").asInstanceOf[String])
    val meas_method: Option[String] = TypeUtils.blankToNone(map.get("meas_method").asInstanceOf[String])

    val meas_description: Option[String] = TypeUtils.blankToNone(map.get("meas_description").asInstanceOf[String])
    val device_status: Option[String] = TypeUtils.blankToNone(map.get("device_status").asInstanceOf[String])
    val test_status: Option[String] = TypeUtils.blankToNone(map.get("test_status").asInstanceOf[String])

    val meas_value_map = map.get("meas_value")
    val meas_lower_limit_map = map.get("meas_lower_limit")
    val meas_upper_limit_map = map.get("meas_upper_limit")

    val datatype_str = map.get("meas_datatype") match {
      case x: String if (x != null) => Some(x)
      case _ => None
    }

    val datatype = datatype_str match {
      case Some(x) if Datatype.isValidName(x) => Datatype.byName(x)
      case _ => null
    }

    val (meas_value, meas_lower_limit, meas_upper_limit, isInvalid) = TypeUtils.getMeasValues(datatype, meas_value_map, meas_lower_limit_map, meas_upper_limit_map)

    if (isInvalid)
      throw new Exception("invalid json format!")

    AutomatedTestCleansed(
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
      meas_flag,
      meas_method,
      meas_lower_limit,
      meas_upper_limit,
      meas_description,
      device_status,
      test_status)
  }

  def jsonToAutomatedTestCleansed(str: String): Option[AutomatedTestCleansed] = {
    fromJson(str) match {
      case Some(jSONObject) => Some(jsonToAutomatedTestCleansed(jSONObject))
      case _ => None
    }
  }

  def jsonToAutomatedTestsCleansed(str: String): List[Option[AutomatedTestCleansed]] = {
    fromJsonArray(str) match {
      case Some(jSONArray) => jSONArray.toArray.toList.map(
        x =>
          try {
            Some(jsonToAutomatedTestCleansed(x.asInstanceOf[JSONObject]))
          } catch {
            case _: Throwable => None
          })
      case _ => List.empty
    }
  }

  def jsonToAutomatedTestCleansed(jSONObject: JSONObject): AutomatedTestCleansed = {

    val company: String = jSONObject.get("company").asInstanceOf[String]
    val site: String = jSONObject.get("site").asInstanceOf[String]
    val device_group: String = jSONObject.get("device_group").asInstanceOf[String]
    val tester: String = jSONObject.get("tester").asInstanceOf[String]

    if (jSONObject.get("ts") == null)
      throw new Exception("invalid json format!")

    val ts: Date = new Date(jSONObject.get("ts").asInstanceOf[Long])

    val device_name: String = jSONObject.get("device_name").asInstanceOf[String]
    val test_name: String = jSONObject.get("test_name").asInstanceOf[String]
    val meas_name: String = jSONObject.get("meas_name").asInstanceOf[String]

    val meas_unit: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_unit").asInstanceOf[String])
    val meas_status: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_status").asInstanceOf[String])

    val meas_flag: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_flag").asInstanceOf[String])
    val meas_method: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_method").asInstanceOf[String])

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

    AutomatedTestCleansed(
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
      meas_flag,
      meas_method,
      meas_lower_limit,
      meas_upper_limit,
      meas_description,
      device_status,
      test_status)
  }

  def jsonToJLinkedHashMap(str: String): JLinkedHashMap[String, Object] = {
    val m = jsonToAutomatedTestCleansed(str).get
    toJLinkedHashMap(m)
  }

  def getColumns: Set[String] = {
    val col_set = Set(
      "company",
      "site",
      "device_group",
      "tester",
      "ts",
      "device_name",
      "test_name",
      "meas_name",
      "datatype_str",
      "meas_value",
      "meas_unit",
      "meas_status",
      "meas_flag",
      "meas_method",
      "meas_lower_limit",
      "meas_upper_limit",
      "meas_description",
      "device_status",
      "test_status")

    col_set
  }
}
