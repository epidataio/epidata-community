/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark.models

import com.epidata.spark.models.util._
import org.apache.spark.sql.Row
//import com.epidata.lib.models.util.Binary
//import java.nio.ByteBuffer
import java.util.Date
import java.lang.{ Double => JDouble, Long => JLong }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import com.epidata.lib.models.util.TypeUtils._

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
case class MeasureLite(
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

object MeasureLite {

  val DBTableName: String = "measurements_original"
  val KafkaTopic: String = "measurements"

  /** Map a Spark SQL Row to a Measurement of the proper type. */
  implicit def rowToMeasurement(row: Row): MeasureLite = {

    // First get the fields that are common across all measurement types.
    val customer = Option(row.getString(0)).get
    val customer_site = Option(row.getString(1)).get
    val collection = Option(row.getString(2)).get
    val dataset = Option(row.getString(3)).get
    // The stored epoch is ignored. Epoch may be computed directly from ts.
    val tsts = Option(row.getTimestamp(0)).get

    val ts = new Date(tsts.getTime)
    // "Missing" values stored as empty strings are converted to None.
    val key1 = blankToNone(Option(row.getString(4)).get)
    val key2 = blankToNone(Option(row.getString(5)).get)
    val key3 = blankToNone(Option(row.getString(6)).get)
    val meas_datatype = stringToOption(row.getString(7))
    val meas_unit = optionBlankToNone(Option(row.getString(8)))
    val meas_status = optionBlankToNone(Option(row.getString(9)))
    val meas_description = optionBlankToNone(Option(row.getString(10)))
    val val1 = optionBlankToNone(Option(row.getString(11)))
    val val2 = optionBlankToNone(Option(row.getString(12)))

    // Construct Measurements using the fields found above along with type
    // specific fields. Measurements of different types are stored using
    // different field names. The presence of these field names is used to test
    // for values of the corresponing type.

    val meas_value_d = Option(row.getDouble(0))
    val meas_value_l = Option(row.getLong(0))
    val meas_value_s = Option(row.getString(13))

    val meas_lower_limit_d = Option(row.getDouble(2))
    val meas_upper_limit_d = Option(row.getDouble(3))

    val meas_lower_limit_l = Option(row.getLong(1))
    val meas_upper_limit_l = Option(row.getLong(2))

    val meas_value =
      if (!meas_value_d.isEmpty)
        meas_value_d.get
      else if (!meas_value_s.isEmpty)
        meas_value_s.get
      else if (!meas_value_l.isEmpty)
        meas_value_l.get
      else {
        val meas_value_b = SQLiteTypeUtils.getOptionBinary(row, 0)
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

    MeasureLite(
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

}
