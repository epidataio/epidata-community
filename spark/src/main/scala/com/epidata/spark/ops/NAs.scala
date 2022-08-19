/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.spark.ops

import com.epidata.spark.utils.{ ConvertUtils, DataFrameUtils }
import com.epidata.lib.models.{ Measurement => BaseMeasurement }
import com.epidata.spark.{ Measurement, MeasurementCleansed }
import org.apache.spark.sql.{ SQLContext, Row, DataFrame }
import org.apache.spark.sql.functions._
import scala.collection.mutable.{ Map => MutableMap, ListBuffer }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import java.util

class NAs extends Transformation {

  override def apply(measurements: ListBuffer[JLinkedHashMap[String, Object]]): ListBuffer[JLinkedHashMap[String, Object]] = {

    var nonEmpty = new JLinkedHashMap[String, Int]()

    for (index <- measurements.indices) {
      measurements(index).forEach {
        case (key, value) =>
          var curr = 0
          if (nonEmpty.containsKey(key)) {
            curr = nonEmpty.get(key).asInstanceOf[Int]

          }
          nonEmpty.put(key, curr)
      }

    }

    var data_ratio: Map[String, Int] = Map()
    var measurement_size = measurements.size.asInstanceOf[Int]
    nonEmpty.forEach {
      case (key, value) =>
        var ratio = data_ratio.get(key).asInstanceOf[Int] / measurement_size
        data_ratio + (key -> ratio)
    }
    for ((k, v) <- data_ratio) {
      if (v < 0.2) {
        for (index <- measurements.indices) {
          if (v.equals((measurements(index).get("meas_name").asInstanceOf[String]))) {
            measurements.remove(index)
          }
        }
      }

    }
    measurements
  }

  override def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {

    dataFrame
  }

  override val name: String = "NAs"

  override def destination: String = "measurements_cleansed"
}
