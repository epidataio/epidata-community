/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark.ops

import com.epidata.lib.models.{ Measurement => BaseMeasurement, SensorMeasurement => BaseSensorMeasurement, AutomatedTest => BaseAutomatedTest }
import org.apache.spark.sql.{ SQLContext, DataFrame }

trait Transformation {
  def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame
  def apply(measurements: List[BaseMeasurement]): List[BaseMeasurement] = {
    // default - return input measurements
    measurements
  }
  def destination: String
}
