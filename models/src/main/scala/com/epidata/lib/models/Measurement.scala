/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models

import com.datastax.driver.core.Row
import com.epidata.lib.models.util.Binary
import java.nio.ByteBuffer
import java.util.Date

/**
 * Model representing a customer measurement stored in the database. Optional
 * key and val fields may be used for some measurement specializations but not
 * others.
 *
 * @param TODO
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
    meas_value: Any,
    meas_unit: Option[String],
    meas_status: Option[String],
    meas_lower_limit: Option[AnyVal],
    meas_upper_limit: Option[AnyVal],
    meas_description: Option[String],
    val1: Option[String],
    val2: Option[String]
) {

  // Splitting timeseries by epoch keeps partitions from growing beyond
  // capacity. The epoch is computed directly from the timestamp.
  lazy val epoch = Measurement.epochForTs(ts)
}

object MeasurementsKeys {
  val DBTableName: String = "measurements_keys"
}

object Measurement {

  val DBTableName: String = "measurements_original"
  val KafkaTopic: String = "measurements"

  def epochForTs(ts: Date): Int =
    // Divide the timeline into epochs approximately 12 days in duration.
    (ts.getTime() / (1000L * 1000L * 1000L)).toInt

  /** Helpers for converting empty strings to None. */
  def blankToNone(string: String): Option[String] = string match {
    case "" => None
    case string => Some(string)
  }
  def optionBlankToNone(string: Option[String]): Option[String] = string match {
    case Some("") => None
    case _ => string
  }

  def stringToOption(string: String): Option[String] = string match {
    case s if (s != null) => Some(s)
    case _ => None
  }

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
    val meas_unit = optionBlankToNone(Option(row.getString("meas_unit")))
    val meas_status = optionBlankToNone(Option(row.getString("meas_status")))
    val meas_description = optionBlankToNone(Option(row.getString("meas_description")))
    val val1 = optionBlankToNone(Option(row.getString("val1")))
    val val2 = optionBlankToNone(Option(row.getString("val2")))

    // Construct Measurements using the fields found above along with type
    // specific fields. Measurements of different types are stored using
    // different field names. The presence of these field names is used to test
    // for values of the corresponing type.
    row.getDouble("meas_value")
    if (!row.isNull("meas_value") && !java.lang.Double.isNaN(row.getDouble("meas_value"))) {
      // Get a Double measurement from row.
      Measurement(
        customer,
        customer_site,
        collection,
        dataset,
        ts,
        key1,
        key2,
        key3,
        Option(row.getDouble("meas_value")).get,
        meas_unit,
        meas_status,
        Option(row.getDouble("meas_lower_limit")),
        Option(row.getDouble("meas_upper_limit")),
        meas_description,
        val1,
        val2
      )
    } else if (!row.isNull("meas_value_l") && row.getLong("meas_value_l") != 0) {
      // Get an Int measurement from row.
      Measurement(
        customer,
        customer_site,
        collection,
        dataset,
        ts,
        key1,
        key2,
        key3,
        Option(row.getLong("meas_value_l")).get,
        meas_unit,
        meas_status,
        Option(row.getLong("meas_lower_limit_l")),
        Option(row.getLong("meas_upper_limit_l")),
        meas_description,
        val1,
        val2
      )
    } else if (!row.isNull("meas_value_s")) {
      // Get a String measurement from row.
      Measurement(
        customer,
        customer_site,
        collection,
        dataset,
        ts,
        key1,
        key2,
        key3,
        Option(row.getString("meas_value_s")).get,
        meas_unit,
        meas_status,
        // Upper and lower limits are not saved with non numeric types.
        None,
        None,
        meas_description,
        val1,
        val2
      )
    } else {
      // Otherwise, get a binary measurement from row.
      val binaryBuf = row.getBytes("meas_value_b")

      binaryBuf match {
        case null =>
          Measurement(
            customer,
            customer_site,
            collection,
            dataset,
            ts,
            key1,
            key2,
            key3,
            "",
            meas_unit,
            meas_status,
            None,
            None,
            meas_description,
            val1,
            val2
          )
        case _ =>
          val measValue = new Array[Byte](binaryBuf.limit - binaryBuf.position)
          binaryBuf.get(measValue)
          val binary = new Binary(measValue)

          Measurement(
            customer,
            customer_site,
            collection,
            dataset,
            ts,
            key1,
            key2,
            key3,
            binary,
            meas_unit,
            meas_status,
            // Upper and lower limits are not saved with non numeric types.
            None,
            None,
            meas_description,
            val1,
            val2
          )
      }

    }
  }
}
