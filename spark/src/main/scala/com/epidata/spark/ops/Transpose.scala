/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.spark.ops

import com.epidata.spark.utils.{ ConvertUtils, DataFrameUtils }
import com.epidata.lib.models.{ Measurement => BaseMeasurement }
import com.epidata.spark.{ Measurement, MeasurementCleansed }
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.functions._

import scala.collection.mutable.{ ListBuffer, Map => MutableMap }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import java.time._
import java.time.format.DateTimeFormatter
import scala.util.control.Breaks.break

class Transpose(
    val fields: List[String]) extends Transformation {

  override def apply(measurements: ListBuffer[JLinkedHashMap[String, Object]]): ListBuffer[JLinkedHashMap[String, Object]] = {

    var new_DF = new ListBuffer[JLinkedHashMap[String, Object]]()

    var tempSeries = scala.collection.mutable.Set[String]()
    for (indice <- measurements.indices) {
      var curr_value = measurements(indice)
      var founded = 0
      for (new_indice <- new_DF.indices) {
        if (curr_value.get("company").asInstanceOf[String].equals(new_DF(new_indice).get("company").asInstanceOf[String])
          && curr_value.get("site").asInstanceOf[String].equals(new_DF(new_indice).get("site").asInstanceOf[String])
          && curr_value.get("station").asInstanceOf[String].equals(new_DF(new_indice).get("station").asInstanceOf[String])
          && curr_value.get("ts").asInstanceOf[String].equals(new_DF(new_indice).get("ts").asInstanceOf[String])) {
          founded = 1
          new_DF(indice).put(curr_value.get("meas_name").asInstanceOf[String], curr_value.get("meas_value"))
          break
        }
      }
      if (founded == 0) {
        var newHashmap = new JLinkedHashMap[String, Object]()
        newHashmap.put("company", curr_value.get("company").asInstanceOf[String])
        newHashmap.put("site", curr_value.get("site").asInstanceOf[String])
        newHashmap.put("station", curr_value.get("station").asInstanceOf[String])
        newHashmap.put("ts", curr_value.get("ts").asInstanceOf[String])
        newHashmap.put(curr_value.get("meas_name").asInstanceOf[String], curr_value.get("meas_value").asInstanceOf[String])
        new_DF += newHashmap
      }

    }
    new_DF
  }
  override def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {

    dataFrame
  }

  override val name: String = "Transpose"

  override def destination: String = "measurements_cleansed"
}
