/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark.models

import java.sql.Timestamp
import java.util.Date

import org.apache.spark.sql.Row

case class MeasurementSummary(
    customer: String,
    customer_site: String,
    collection: String,
    dataset: String,
    start_time: Timestamp,
    stop_time: Timestamp,
    key1: String,
    key2: String,
    key3: String,
    meas_summary_name: String,
    meas_summary_value: String,
    meas_summary_description: String)

object MeasurementSummary {

  val DBTableName: String = "measurements_summary"

  /** Map a cassandra Row to a MeasurementSummary */
  implicit def rowToMeasurementSummary(row: Row): MeasurementSummary = {
    val customer = row.getString(0)
    val customer_site = row.getString(1)
    val collection = row.getString(2)
    val dataset = row.getString(3)

    val start_time_timeStamp = row.getTimestamp(0)
    val stop_time_timeStamp = row.getTimestamp(1)

    val start_time_date = new Date(start_time_timeStamp.getTime)
    val stop_time_date = new Date(stop_time_timeStamp.getTime)

    val start_time = new Timestamp(start_time_date.getTime)
    val stop_time = new Timestamp(stop_time_date.getTime)

    val key1 = row.getString(4)
    val key2 = row.getString(5)
    val key3 = row.getString(6)
    val meas_summary_name = row.getString(7)
    val meas_summary_value = row.getString(8)
    val meas_summary_description = row.getString(9)

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
}

