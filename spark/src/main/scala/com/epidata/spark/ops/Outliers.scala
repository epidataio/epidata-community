/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.spark.ops

import com.epidata.spark.utils.{ ConvertUtils, DataFrameUtils }
import com.epidata.lib.models.{ Measurement => BaseMeasurement }
import com.epidata.spark.{ Measurement, MeasurementCleansed }
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.functions._
import com.google.common.math.Quantiles._

import java.util
import collection.immutable._
import scala.collection.JavaConverters._
import scala.collection.mutable.{ ListBuffer, Map => MutableMap }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
//import com.google.common.math.Quantiles.Scale

class Outliers(
    val fields: List[String],
    val mpercentage: Int,
    val method: String) extends Transformation {

  override def apply(measurements: ListBuffer[JLinkedHashMap[String, Object]]): ListBuffer[JLinkedHashMap[String, Object]] = {
    var dataset = collection.immutable.Map[String, List[Double]]()
    for (mfield <- fields) {
      if (!dataset.contains(mfield)) {
        val empty: List[Object] = List()
        dataset + (mfield -> empty)
      }
      for (index <- measurements.indices) {
        //      q_low = measurements(index).get("meter_reading").quantile(0.01)
        //      q_hi = measurements(index).get("meter_reading").quantile(0.99)
        if (mfield.equals(measurements(index).get("meas_name").asInstanceOf[String])) {
          dataset.get(mfield) + (measurements(index).get("meas_value").asInstanceOf[String])
        }
      }

    }
    var list = new java.util.ArrayList[Int]()
    var outliers = new ListBuffer[Int]()
    for (index <- measurements.indices) {

      if (dataset.contains(measurements(index).get("meas_name").asInstanceOf[String])) {
        val fieldName = measurements(index).get("meas_name").asInstanceOf[String]
        var q_low: Double = 0.0
        var q_high: Double = 0.0
        val mdataset: List[Double] = dataset.get(fieldName) match {
          //case None => None //Or handle the lack of a value another way: throw an error, etc.
          case Some(s: List[Double]) => s //return the string to set your value
        }
        //        q_low = percentiles().index(mpercentage).compute(list)
        //        q_high = percentiles().index(1 - mpercentage).compute(mdataset.asJava)
        val currData = measurements(index).get("meas_value").asInstanceOf[Double]
        if (currData < q_low || currData > q_high) {
          outliers += index
        }
      }
    }
    if (method.equals("delete")) {
      for (item <- outliers) {
        measurements.remove(item)
      }

    }

    measurements
  }

  override def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {
    dataFrame
  }

  override val name: String = "Outliers"

  override def destination: String = "measurements_cleansed"
}
