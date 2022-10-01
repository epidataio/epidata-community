/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.spark.ops

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.epidata.spark.{ Measurement, MeasurementCleansed }
//import com.epidata.lib.models.{ Measurement => BaseMeasurement, SensorMeasurement => BaseSensorMeasurement, AutomatedTest => BaseAutomatedTest }

class Identity extends Transformation {

  override val name: String = "Identity"
  override def destination: String = "measurements_cleansed"
}
