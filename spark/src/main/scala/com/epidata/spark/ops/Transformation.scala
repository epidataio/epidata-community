/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.spark.ops

import com.epidata.spark.{ Measurement, MeasurementCleansed }
import com.epidata.lib.models.{ Measurement => BaseMeasurement, MeasurementCleansed => BaseMeasurementCleansed, MeasurementSummary => BaseMeasurementSummary }
import com.epidata.lib.models.{ AutomatedTest => BaseAutomatedTest, AutomatedTestCleansed => BaseAutomatedTestCleansed, AutomatedTestSummary => BaseAutomatedTestSummary }
import com.epidata.lib.models.{ SensorMeasurement => BaseSensorMeasurement, SensorMeasurementCleansed => BaseSensorMeasurementCleansed, SensorMeasurementSummary => BaseSensorMeasurementSummary }
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SQLContext, DataFrame }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import scala.collection.mutable.{ Map => MutableMap, ListBuffer }

trait Transformation {
  def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame =
    dataFrame
      .withColumn("meas_flag", lit(null: String))
      .withColumn("meas_method", lit(null: String))

  def apply(measurements: ListBuffer[java.util.Map[String, Object]]): ListBuffer[java.util.Map[String, Object]] = {
    for (index <- measurements.indices) {
      measurements(index).put("meas_flag", null)
      measurements(index).put("meas_method", null)
    }
    measurements
  }

  def apply(measurements: java.util.List[java.util.Map[String, Object]]): java.util.List[java.util.Map[String, Object]] = {
    import scala.collection.JavaConverters._
    val sBuffer = measurements.asScala.to[ListBuffer]
    println("List1: " + sBuffer)
    val applyBuffer = apply(sBuffer)
    println("List2: " + applyBuffer)
    val retList = new java.util.ArrayList[java.util.Map[String, Object]]()
    for (map <- applyBuffer) {
      retList.add(map)
    }
    println("List3: " + retList)
    retList
  }

  val name: String = "Default"
  def destination: String
}
