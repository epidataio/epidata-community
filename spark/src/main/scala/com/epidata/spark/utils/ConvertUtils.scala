/*
 * Copyright (c) 2015-2020 EpiData, Inc.
*/

package com.epidata.spark.utils

import java.sql.Timestamp

import com.epidata.lib.models._
import com.epidata.lib.models.util.{ TypeUtils, JsonHelpers, Binary }
import com.epidata.spark.models.MeasurementDB

object ConvertUtils {

  def optionNoneToString(string: Option[String]): String = string match {
    case Some(value) => value
    case _ => ""
  }

  def nullToOption(value: Double): Option[Double] = value match {
    case x: Double if (x != null) => Some(value)
    case _ => None
  }

  def nullToOption(value: Long): Option[Long] = value match {
    case x: Long if (x != null) => Some(value)
    case _ => None
  }

  def nullToOption(value: String): Option[String] = value match {
    case x: String if (x != null && !x.isEmpty) => Some(value)
    case _ => None
  }

  def nullToOption(value: Array[Byte]): Option[Array[Byte]] = value match {
    case x: Array[Byte] if !x.isEmpty => Some(value)
    case _ => None
  }

  def convertDoubleMeasurementToMeasurementDB(meas: Measurement): MeasurementDB = {
    MeasurementDB(
      meas.customer,
      meas.customer_site,
      meas.collection,
      meas.dataset,
      TypeUtils.epochForTs(meas.ts),
      new Timestamp(meas.ts.getTime),
      optionNoneToString(meas.key1),
      optionNoneToString(meas.key2),
      optionNoneToString(meas.key3),
      meas.meas_datatype,
      Some(meas.meas_value.asInstanceOf[Double]),
      None,
      None,
      None,
      meas.meas_unit,
      meas.meas_status,
      meas.meas_lower_limit.map(_.asInstanceOf[Double]),
      None,
      meas.meas_upper_limit.map(_.asInstanceOf[Double]),
      None,
      meas.meas_description,
      meas.val1,
      meas.val2)
  }

  def convertLongMeasurementToMeasurementDB(meas: Measurement): MeasurementDB = {
    MeasurementDB(
      meas.customer,
      meas.customer_site,
      meas.collection,
      meas.dataset,
      TypeUtils.epochForTs(meas.ts),
      new Timestamp(meas.ts.getTime),
      optionNoneToString(meas.key1),
      optionNoneToString(meas.key2),
      optionNoneToString(meas.key3),
      meas.meas_datatype,
      None,
      Some(meas.meas_value.asInstanceOf[Long]),
      None,
      None,
      meas.meas_unit,
      meas.meas_status,
      None,
      meas.meas_lower_limit.map(_.asInstanceOf[Long]),
      None,
      meas.meas_upper_limit.map(_.asInstanceOf[Long]),
      meas.meas_description,
      meas.val1,
      meas.val2)
  }

  def convertStringMeasurementToMeasurementDB(meas: Measurement): MeasurementDB = {
    MeasurementDB(
      meas.customer,
      meas.customer_site,
      meas.collection,
      meas.dataset,
      TypeUtils.epochForTs(meas.ts),
      new Timestamp(meas.ts.getTime),
      optionNoneToString(meas.key1),
      optionNoneToString(meas.key2),
      optionNoneToString(meas.key3),
      meas.meas_datatype,
      None,
      None,
      Some(meas.meas_value.asInstanceOf[String]),
      None,
      meas.meas_unit,
      meas.meas_status,
      None,
      None,
      None,
      None,
      meas.meas_description,
      meas.val1,
      meas.val2)
  }

  def convertBinaryMeasurementToMeasurementDB(meas: Measurement): MeasurementDB = {
    MeasurementDB(
      meas.customer,
      meas.customer_site,
      meas.collection,
      meas.dataset,
      TypeUtils.epochForTs(meas.ts),
      new Timestamp(meas.ts.getTime),
      optionNoneToString(meas.key1),
      optionNoneToString(meas.key2),
      optionNoneToString(meas.key3),
      meas.meas_datatype,
      None,
      None,
      None,
      Some(meas.meas_value.asInstanceOf[Binary].backing),
      meas.meas_unit,
      meas.meas_status,
      None,
      None,
      None,
      None,
      meas.meas_description,
      meas.val1,
      meas.val2)
  }

  def convertJsonStringToMeasurementDB(str: String): MeasurementDB = {
    val meas = com.epidata.lib.models.SensorMeasurement.jsonToSensorMeasurement(str).get
    val m = SensorMeasurement.sensorMeasurementToMeasurement(meas)
    val mdb = ConvertUtils.convertMeasurementToMeasurementDB(m)
    mdb
  }

  def convertMeasurementToMeasurementDB(meas: Measurement): MeasurementDB = {
    meas.meas_value match {
      case meas_value: Double => convertDoubleMeasurementToMeasurementDB(meas)
      case meas_value: Long => convertLongMeasurementToMeasurementDB(meas)
      case meas_value: String => convertStringMeasurementToMeasurementDB(meas)
      case meas_value: Binary => convertBinaryMeasurementToMeasurementDB(meas)
    }
  }

}

