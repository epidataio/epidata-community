/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models

import com.datastax.driver.core.Row

import java.sql.ResultSet
import com.epidata.lib.models.util.Binary

import java.nio.ByteBuffer
import java.util.Date
import java.lang.{ Double => JDouble, Long => JLong }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import com.epidata.lib.models.util.TypeUtils._

import scala.collection.mutable
import scala.collection.mutable.Set

/**
 * Model representing a customer measurement stored in the database. Optional
 * key and val fields may be used for some measurement specializations but not
 * others.
 *
 * @param meas_unit Measurement unit. Must be present for all types except
 *                  string. Must not be present for string.
 * @param meas_lower_limit Lower limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 * @param meas_upper_limit Upper limit of measurement range. May be present for
 *                         numeric types but never for non numeric types.
 */
case class Measurement(
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
    meas_lower_limit: Option[AnyVal],
    meas_upper_limit: Option[AnyVal],
    meas_description: Option[String],
    val1: Option[String],
    val2: Option[String]) {

  // Splitting timeseries by epoch keeps partitions from growing beyond
  // capacity. The epoch is computed directly from the timestamp.
  lazy val epoch = epochForTs(ts)
}

object MeasurementsKeys {
  val DBTableName: String = "measurements_keys"
}

object Measurement {

  val DBTableName: String = "measurements_original"
  val KafkaTopic: String = "measurements"

  /** Map a cassandra Row to a Measurement of the proper type. */
  implicit def rowToMeasurement(row: Row): Measurement = {

    // First get the fields that are common across all measurement types.
    val customer = Option(row.getString("customer")).get
    val customer_site = Option(row.getString("customer_site")).get
    val collection = Option(row.getString("collection")).get
    val dataset = Option(row.getString("dataset")).get
    // The stored epoch is ignored. Epoch may be computed directly from ts.
    val tsts = Option(row.getTimestamp("ts")).get

    val ts = new Date(tsts.getTime)
    // "Missing" values stored as empty strings are converted to None.
    val key1 = blankToNone(Option(row.getString("key1")).get)
    val key2 = blankToNone(Option(row.getString("key2")).get)
    val key3 = blankToNone(Option(row.getString("key3")).get)
    val meas_datatype = stringToOption(row.getString("meas_datatype"))
    val meas_unit = optionBlankToNone(Option(row.getString("meas_unit")))
    val meas_status = optionBlankToNone(Option(row.getString("meas_status")))
    val meas_description = optionBlankToNone(Option(row.getString("meas_description")))
    val val1 = optionBlankToNone(Option(row.getString("val1")))
    val val2 = optionBlankToNone(Option(row.getString("val2")))

    // Construct Measurements using the fields found above along with type
    // specific fields. Measurements of different types are stored using
    // different field names. The presence of these field names is used to test
    // for values of the corresponing type.

    val meas_value_d = getOptionDouble(row, "meas_value")
    val meas_value_l = getOptionLong(row, "meas_value_l")
    val meas_value_s = getOptionString(row, "meas_value_s")

    val meas_lower_limit_d = getOptionDouble(row, "meas_lower_limit")
    val meas_upper_limit_d = getOptionDouble(row, "meas_upper_limit")

    val meas_lower_limit_l = getOptionLong(row, "meas_lower_limit_l")
    val meas_upper_limit_l = getOptionLong(row, "meas_upper_limit_l")

    val meas_value =
      if (!meas_value_d.isEmpty)
        meas_value_d.get
      else if (!meas_value_s.isEmpty)
        meas_value_s.get
      else if (!meas_value_l.isEmpty)
        meas_value_l.get
      else {
        val meas_value_b = getOptionBinary(row, "meas_value_b")
        if (!meas_value_b.isEmpty) meas_value_b.get else None
      }

    val meas_lower_limit =
      if (!meas_lower_limit_d.isEmpty)
        meas_lower_limit_d
      else if (!meas_lower_limit_l.isEmpty)
        meas_lower_limit_l
      else
        None

    val meas_upper_limit =
      if (!meas_upper_limit_d.isEmpty)
        meas_upper_limit_d
      else if (!meas_upper_limit_l.isEmpty)
        meas_upper_limit_l
      else
        None

    Measurement(
      customer,
      customer_site,
      collection,
      dataset,
      ts,
      key1,
      key2,
      key3,
      meas_datatype,
      meas_value,
      meas_unit,
      meas_status,
      meas_lower_limit,
      meas_upper_limit,
      meas_description,
      val1,
      val2)

  }

  /** Map a SQLite Row to a Measurement of the proper type. */
  implicit def rowToMeasurement(row: ResultSet): Measurement = {

    // First get the fields that are common across all measurement types.
    val customer = Option(row.getString("customer")).get
    val customer_site = Option(row.getString("customer_site")).get
    val collection = Option(row.getString("collection")).get
    val dataset = Option(row.getString("dataset")).get
    // The stored epoch is ignored. Epoch may be computed directly from ts.
    val tsts = Option(row.getTimestamp("ts")).get

    val ts = new Date(tsts.getTime)
    // "Missing" values stored as empty strings are converted to None.
    val key1 = blankToNone(Option(row.getString("key1")).get)
    val key2 = blankToNone(Option(row.getString("key2")).get)
    val key3 = blankToNone(Option(row.getString("key3")).get)
    val meas_datatype = stringToOption(row.getString("meas_datatype"))
    val meas_unit = optionBlankToNone(Option(row.getString("meas_unit")))
    val meas_status = optionBlankToNone(Option(row.getString("meas_status")))
    val meas_description = optionBlankToNone(Option(row.getString("meas_description")))
    val val1 = optionBlankToNone(Option(row.getString("val1")))
    val val2 = optionBlankToNone(Option(row.getString("val2")))

    // Construct Measurements using the fields found above along with type
    // specific fields. Measurements of different types are stored using
    // different field names. The presence of these field names is used to test
    // for values of the corresponing type.

    val meas_value_d = getOptionDouble(row, "meas_value")
    val meas_value_l = getOptionLong(row, "meas_value_l")
    val meas_value_s = getOptionString(row, "meas_value_s")

    val meas_lower_limit_d = getOptionDouble(row, "meas_lower_limit")
    val meas_upper_limit_d = getOptionDouble(row, "meas_upper_limit")

    val meas_lower_limit_l = getOptionLong(row, "meas_lower_limit_l")
    val meas_upper_limit_l = getOptionLong(row, "meas_upper_limit_l")

    val meas_value =
      if (!meas_value_d.isEmpty)
        meas_value_d.get
      else if (!meas_value_s.isEmpty)
        meas_value_s.get
      else if (!meas_value_l.isEmpty)
        meas_value_l.get
      else {
        val meas_value_b = getOptionBinary(row, "meas_value_b")
        if (!meas_value_b.isEmpty) meas_value_b.get else None
      }

    val meas_lower_limit =
      if (!meas_lower_limit_d.isEmpty)
        meas_lower_limit_d
      else if (!meas_lower_limit_l.isEmpty)
        meas_lower_limit_l
      else
        None

    val meas_upper_limit =
      if (!meas_upper_limit_d.isEmpty)
        meas_upper_limit_d
      else if (!meas_upper_limit_l.isEmpty)
        meas_upper_limit_l
      else
        None

    Measurement(
      customer,
      customer_site,
      collection,
      dataset,
      ts,
      key1,
      key2,
      key3,
      meas_datatype,
      meas_value,
      meas_unit,
      meas_status,
      meas_lower_limit,
      meas_upper_limit,
      meas_description,
      val1,
      val2)

  }

  def rowToJLinkedHashMap(row: Row, tableName: String, modelName: String): JLinkedHashMap[String, Object] = {
    modelName match {
      case SensorMeasurement.NAME => SensorMeasurement.rowToJLinkedHashMap(row, tableName)
      case AutomatedTest.NAME => AutomatedTest.rowToJLinkedHashMap(row, tableName)
      case _ => new JLinkedHashMap[String, Object]()
    }
  }

  def rowToJLinkedHashMap(row: ResultSet, tableName: String, modelName: String): JLinkedHashMap[String, Object] = {
    modelName match {
      case SensorMeasurement.NAME => SensorMeasurement.rowToJLinkedHashMap(row, tableName)
      case AutomatedTest.NAME => AutomatedTest.rowToJLinkedHashMap(row, tableName)
      case _ => new JLinkedHashMap[String, Object]()
    }
  }

  def getColumns: mutable.Set[String] = {
    val col_set = mutable.Set(
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
      "meas_lower_limit",
      "meas_upper_limit",
      "meas_description",
      "val1",
      "val2")
    col_set
  }

}
