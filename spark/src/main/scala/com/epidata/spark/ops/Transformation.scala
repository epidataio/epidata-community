/*
* Copyright (c) 2015-2017 EpiData, Inc.
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

  def apply(measurements: ListBuffer[JLinkedHashMap[String, Object]]): ListBuffer[JLinkedHashMap[String, Object]] = {
    // To Be Implemented - Add meas_flag and meas_method columns
    measurements
  }

  /* Interface for Python and Java */
  /*  def apply(measurements: JList[JLinkedHashMap[String, Object]]): JList[JLinkedHashMap[String, Object]] = {
    import scala.collection.JavaConversions._
    // To Be Implemented - perform JList[JLinkedHashMap[String, Object]] -> List[Map[String, Object]] conversion and call Scala native apply method
    measurements
  }
*/
  val name: String = "Default"
  def destination: String
}
