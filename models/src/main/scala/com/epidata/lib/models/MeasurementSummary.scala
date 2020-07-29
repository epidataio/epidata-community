/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models

import java.sql.Timestamp
import java.util.Date

import com.datastax.driver.core.Row

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

    val key1 = row.getString("key1")
    val key2 = row.getString("key2")
    val key3 = row.getString("key3")
    val meas_summary_name = row.getString("meas_summary_name")
    val meas_summary_value = row.getString("meas_summary_value")
    val meas_summary_description = row.getString("meas_summary_description")

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

