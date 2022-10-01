/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.lib.models

import com.datastax.driver.core.Row
import java.sql.ResultSet

import java.nio.ByteBuffer
import java.lang.{ Double => JDouble, Long => JLong }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import com.epidata.lib.models.util.TypeUtils
import com.epidata.lib.models.util.Binary

import scala.collection.mutable
//import scala.collection.mutable.Set

case class MeasurementCleansed(
    customer: String,
    customer_site: String,
    collection: String,
    dataset: String,
    ts: Date,
    key1: Option[String],
    key2: Option[String],
    key3: Option[String],
    meas_datatype: Option[String],
    meas_value: Any,
    meas_unit: Option[String],
    meas_status: Option[String],
    meas_flag: Option[String],
    meas_method: Option[String],
    meas_lower_limit: Option[AnyVal],
    meas_upper_limit: Option[AnyVal],
    meas_description: Option[String],
    val1: Option[String],
    val2: Option[String]) {

  // Splitting timeseries by epoch keeps partitions from growing beyond
  // capacity. The epoch is computed directly from the timestamp.
  lazy val epoch = TypeUtils.epochForTs(ts)
}

object MeasurementCleansed {

  val DBTableName: String = "measurements_cleansed"
  val KafkaTopic: String = DBTableName
  val zmqTopic: String = DBTableName

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
      "meas_datatype",
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
      "val2")

  implicit def measurementToMeasurementCleansed(m: Measurement, measFlag: Option[String], measMethod: Option[String]): MeasurementCleansed = {
    MeasurementCleansed(
      m.customer,
      m.customer_site,
      m.collection,
      m.dataset,
      m.ts,
      m.key1,
      m.key2,
      m.key3,
      m.meas_datatype,
      m.meas_value,
      m.meas_unit,
      m.meas_status,
      measFlag,
      measMethod,
      m.meas_lower_limit,
      m.meas_upper_limit,
      m.meas_description,
      m.val1,
      m.val2)
  }

  implicit def rowToMeasurementCleansed(row: Row): MeasurementCleansed = {
    val m = Measurement.rowToMeasurement(row)
    measurementToMeasurementCleansed(
      m,
      TypeUtils.stringToOption(row.getString("meas_flag")),
      TypeUtils.stringToOption(row.getString("meas_method")))
  }

  implicit def resultSetToMeasurementCleansed(row: ResultSet): MeasurementCleansed = {
    val m = Measurement.resultSetToMeasurement(row)
    measurementToMeasurementCleansed(
      m,
      TypeUtils.stringToOption(row.getString("meas_flag")),
      TypeUtils.stringToOption(row.getString("meas_method")))
  }

  def rowToJLinkedHashMap(row: Row, tableName: String, modelName: String): JLinkedHashMap[String, Object] = {
    modelName match {
      case SensorMeasurement.NAME => SensorMeasurementCleansed.rowToJLinkedHashMap(row, tableName)
      case AutomatedTest.NAME => AutomatedTestCleansed.rowToJLinkedHashMap(row, tableName)
      case _ => new JLinkedHashMap[String, Object]()
    }
  }

  def resultSetToJLinkedHashMap(row: ResultSet, tableName: String, modelName: String): JLinkedHashMap[String, Object] = {
    modelName match {
      case SensorMeasurement.NAME => SensorMeasurementCleansed.resultSetToJLinkedHashMap(row, tableName)
      case AutomatedTest.NAME => AutomatedTestCleansed.resultSetToJLinkedHashMap(row, tableName)
      case _ => new JLinkedHashMap[String, Object]()
    }
  }

  def getColumns: Set[String] = {
    val col_set = Set(
      "customer",
      "customer_site",
      "collection",
      "dataset",
      "ts",
      "key1",
      "key2",
      "key3",
      "meas_datatype",
      "meas_value",
      "meas_unit",
      "meas_status",
      "meas_flag",
      "meas_method",
      "meas_lower_limit",
      "meas_upper_limit",
      "meas_description",
      "val1",
      "val2")
    col_set
  }

}
