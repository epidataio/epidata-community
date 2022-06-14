/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.lib.models

import java.sql.Timestamp
import com.datastax.driver.core.Row
import java.sql.ResultSet

import com.epidata.lib.models.util.TypeUtils._
import com.epidata.lib.models.util.Binary
import java.lang.{ Double => JDouble, Long => JLong }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }

case class MeasurementSummary(
    customer: String,
    customer_site: String,
    collection: String,
    dataset: String,
    start_time: Date,
    stop_time: Date,
    key1: Option[String],
    key2: Option[String],
    key3: Option[String],
    meas_summary_name: String,
    meas_summary_value: String,
    meas_summary_description: Option[String])

object MeasurementSummary {

  val DBTableName: String = "measurements_summary"
  val KafkaTopic: String = DBTableName
  val zmqTopic: String = DBTableName

  /** Map a cassandra Row to a MeasurementSummary */
  implicit def rowToMeasurementSummary(row: Row): MeasurementSummary = {
    val customer = row.getString("customer")
    val customer_site = row.getString("customer_site")
    val collection = row.getString("collection")
    val dataset = row.getString("dataset")

    val start_time_timeStamp = row.getTimestamp("start_time")
    val stop_time_timeStamp = row.getTimestamp("stop_time")

    val start_time_date = new Date(start_time_timeStamp.getTime)
    val stop_time_date = new Date(stop_time_timeStamp.getTime)

    val start_time = new Timestamp(start_time_date.getTime)
    val stop_time = new Timestamp(stop_time_date.getTime)

    val key1 = blankToNone(Option(row.getString("key1")).get)
    val key2 = blankToNone(Option(row.getString("key2")).get)
    val key3 = blankToNone(Option(row.getString("key3")).get)

    val meas_summary_name = row.getString("meas_summary_name")
    val meas_summary_value = row.getString("meas_summary_value")
    val meas_summary_description = optionBlankToNone(Option(row.getString("meas_summary_description")))

    MeasurementSummary(
      customer,
      customer_site,
      collection,
      dataset,
      start_time,
      stop_time,
      key1,
      key2,
      key3,
      meas_summary_name,
      meas_summary_value,
      meas_summary_description)

  }

  implicit def rowToMeasurementSummary(row: ResultSet): MeasurementSummary = {
    val customer = row.getString("customer")
    val customer_site = row.getString("customer_site")
    val collection = row.getString("collection")
    val dataset = row.getString("dataset")

    val start_time_timeStamp = row.getTimestamp("start_time")
    val stop_time_timeStamp = row.getTimestamp("stop_time")

    val start_time_date = new Date(start_time_timeStamp.getTime)
    val stop_time_date = new Date(stop_time_timeStamp.getTime)

    val start_time = new Timestamp(start_time_date.getTime)
    val stop_time = new Timestamp(stop_time_date.getTime)

    val key1 = blankToNone(Option(row.getString("key1")).get)
    val key2 = blankToNone(Option(row.getString("key2")).get)
    val key3 = blankToNone(Option(row.getString("key3")).get)

    val meas_summary_name = row.getString("meas_summary_name")
    val meas_summary_value = row.getString("meas_summary_value")
    val meas_summary_description = optionBlankToNone(Option(row.getString("meas_summary_description")))

    MeasurementSummary(
      customer,
      customer_site,
      collection,
      dataset,
      start_time,
      stop_time,
      key1,
      key2,
      key3,
      meas_summary_name,
      meas_summary_value,
      meas_summary_description)

  }

  def rowToJLinkedHashMap(row: Row, tableName: String, modelName: String): JLinkedHashMap[String, Object] = {
    modelName match {
      case SensorMeasurement.NAME => SensorMeasurementSummary.rowToJLinkedHashMap(row, tableName)
      case AutomatedTest.NAME => AutomatedTestSummary.rowToJLinkedHashMap(row, tableName)
      case _ => new JLinkedHashMap[String, Object]()
    }
  }

  def rowToJLinkedHashMap(row: ResultSet, tableName: String, modelName: String): JLinkedHashMap[String, Object] = {
    modelName match {
      case SensorMeasurement.NAME => SensorMeasurementSummary.rowToJLinkedHashMap(row, tableName)
      case AutomatedTest.NAME => AutomatedTestSummary.rowToJLinkedHashMap(row, tableName)
      case _ => new JLinkedHashMap[String, Object]()
    }
  }

  def getColumns: Set[String] = {
    val col_set = Set(
      "customer",
      "customer_site",
      "collection",
      "dataset",
      "start_time",
      "stop_time",
      "key1",
      "key2",
      "key3",
      "meas_summary_name",
      "meas_summary_value",
      "meas_summary_description")
    col_set
  }
}
