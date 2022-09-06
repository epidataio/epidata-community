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

class InverseTranspose(
    val fields: List[String]) extends Transformation {

  override def apply(measurements: ListBuffer[java.util.Map[String, Object]]): ListBuffer[java.util.Map[String, Object]] = {
    var new_DF = new ListBuffer[java.util.Map[String, Object]]()
    for (mfield <- fields) {
      var newHashmap = new JLinkedHashMap[String, Object]()
      newHashmap.put("company", measurements(0).get("company").asInstanceOf[String])
      newHashmap.put("site", measurements(0).get("site").asInstanceOf[String])
      newHashmap.put("station", measurements(0).get("station").asInstanceOf[String])
      newHashmap.put("ts", measurements(0).get("ts"))
      newHashmap.put("meas_name", mfield)
      newHashmap.put("meas_value", measurements(0).get(mfield))
    }

    new_DF
  }
  override def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {

    dataFrame
  }

  override val name: String = "InverseTranspose"

  override def destination: String = "measurements_cleansed"
}
