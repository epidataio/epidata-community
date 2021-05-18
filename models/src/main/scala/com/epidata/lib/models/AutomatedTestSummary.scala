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
 * Specialization of MeasurementSummary representing automated test summary data.
 *
 * @param meas_lower_limit Lower limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 * @param meas_upper_limit Upper limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 */
case class AutomatedTestSummary(
    company: String,
    site: String,
    device_group: String,
    tester: String,
    start_time: Date,
    stop_time: Date,
    device_name: String,
    test_name: String,
    meas_name: String,
    meas_summary_name: String,
    meas_summary_value: String,
    meas_summary_description: Option[String])

object AutomatedTestSummary {

  val NAME: String = "automated_test_summary"

  def rowToAutomatedTestSummary(row: Row): AutomatedTestSummary = MeasurementSummary.rowToMeasurementSummary(row)

  def rowToAutomatedTestSummary(row: ResultSet): AutomatedTestSummary = MeasurementSummary.rowToMeasurementSummary(row)

  implicit def measurementSummaryToAutomatedTestSummary(ms: MeasurementSummary): AutomatedTestSummary =
    AutomatedTestSummary(
      ms.customer,
      ms.customer_site,
      ms.collection,
      ms.dataset,
      ms.start_time,
      ms.stop_time,
      ms.key1.get,
      ms.key2.get,
      ms.key3.get,
      ms.meas_summary_name,
      ms.meas_summary_value,
      ms.meas_summary_description)

  implicit def automatedTestSummaryToMeasurementSummary(automatedTestSummary: AutomatedTestSummary): MeasurementSummary =
    MeasurementSummary(
      automatedTestSummary.company,
      automatedTestSummary.site,
      automatedTestSummary.device_group,
      automatedTestSummary.tester,
      automatedTestSummary.start_time,
      automatedTestSummary.stop_time,
      Some(automatedTestSummary.device_name),
      Some(automatedTestSummary.test_name),
      Some(automatedTestSummary.meas_name),
      automatedTestSummary.meas_summary_name,
      automatedTestSummary.meas_summary_value,
      automatedTestSummary.meas_summary_description)

  // JSON Helpers
  def rowToJLinkedHashMap(rowSummary: Row, tableName: String): JLinkedHashMap[String, Object] = {
    tableName match {
      case MeasurementSummary.DBTableName =>
        val ms = rowToAutomatedTestSummary(rowSummary)
        toJLinkedHashMap(ms)
    }
  }

  // JSON Helpers
  def rowToJLinkedHashMap(rowSummary: ResultSet, tableName: String): JLinkedHashMap[String, Object] = {
    tableName match {
      case MeasurementSummary.DBTableName =>
        val ms = rowToAutomatedTestSummary(rowSummary)
        toJLinkedHashMap(ms)
    }
  }

  import com.epidata.lib.models.util.JsonHelpers._

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

  def jsonToAutomatedTestSummary(str: String): Option[AutomatedTestSummary] = {
    fromJson(str) match {
      case Some(jSONObject) => Some(jsonToAutomatedTestSummary(jSONObject))
      case _ => None
    }
  }

  def jsonToAutomatedTestsSummary(str: String): List[Option[AutomatedTestSummary]] = {
    fromJsonArray(str) match {
      case Some(jSONArray) => jSONArray.toArray.toList.map(
        x =>
          try {
            Some(jsonToAutomatedTestSummary(x.asInstanceOf[JSONObject]))
          } catch {
            case _: Throwable => None
          })
      case _ => List.empty
    }
  }

  def toJson(ms: AutomatedTestSummary): String = {
    val map = toJLinkedHashMap(ms)
    JSONObject.toJSONString(map)
  }

  def toJson(list: List[AutomatedTestSummary]): String = {
    import scala.collection.JavaConverters._
    val arr = new JLinkedList[JLinkedHashMap[String, Object]]()
    arr.addAll(
      list
        .map(ms => toJLinkedHashMap(ms))
        .asJavaCollection)
    JSONArray.toJSONString(arr)
  }

  def jsonToAutomatedTestSummary(jSONObject: JSONObject): AutomatedTestSummary = {
    val company: String = jSONObject.get("company").asInstanceOf[String]
    val site: String = jSONObject.get("site").asInstanceOf[String]
    val device_group: String = jSONObject.get("device_group").asInstanceOf[String]
    val tester: String = jSONObject.get("tester").asInstanceOf[String]
    val start_time: Date = new Date(jSONObject.get("start_time").asInstanceOf[Long])
    val stop_time: Date = new Date(jSONObject.get("stop_time").asInstanceOf[Long])

    val device_name: String = jSONObject.get("device_name").asInstanceOf[String]
    val test_name: String = jSONObject.get("test_name").asInstanceOf[String]
    val meas_name: String = jSONObject.get("meas_name").asInstanceOf[String]

    val meas_summary_name: String = jSONObject.get("meas_summary_name").asInstanceOf[String]
    val meas_summary_value: String = jSONObject.get("meas_summary_value").asInstanceOf[String]
    val meas_summary_description: Option[String] = TypeUtils.blankToNone(jSONObject.get("meas_summary_description").asInstanceOf[String])

    AutomatedTestSummary(
      company,
      site,
      device_group,
      tester,
      start_time,
      stop_time,
      device_name,
      test_name,
      meas_name,
      meas_summary_name,
      meas_summary_value,
      meas_summary_description)
  }

}
