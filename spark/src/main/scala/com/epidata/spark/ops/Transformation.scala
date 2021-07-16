/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark.ops

import com.epidata.lib.models.{ Measurement => BaseMeasurement, SensorMeasurement => BaseSensorMeasurement, AutomatedTest => BaseAutomatedTest }
import org.apache.spark.sql.{ SQLContext, DataFrame }

trait Transformation {
  def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame
  def apply(measurements: List[String]): List[String] = {
    //takes in list of basemeasurements as string converts to json applies transformation and returns new List of stringyfied basemeasuremnts
    // default - return input measurements
    measurements
  }
  def destination: String
}
