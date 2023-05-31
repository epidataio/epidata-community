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

class Resample(
    val fields: List[String],
    val time_interval: Int,
    val timeunit: String) extends Transformation {
  //https://users.scala-lang.org/t/how-to-create-date-ranges-in-scala/6566/2
  //  def dateRange(start: LocalDateTime, end: LocalDateTime, increment: LocalDateTime => LocalDateTime) =
  //    Iterator.unfold(start)(next => Option(increment(next)).filter(_.isBefore(end)))

  override def apply(measurements: ListBuffer[java.util.Map[String, Object]]): ListBuffer[java.util.Map[String, Object]] = {

    //    for (index <- measurements.indices) {
    //
    //      var newDatetime = measurements(index).get("ts")
    //      measurements(index).put("ts", newDatetime)
    //
    //    }

    var filtered = measurements
      .filter(m => fields.contains(m.get("meas_name").asInstanceOf[String]))

    var notfiltered = measurements
      .filterNot(m => fields.contains(m.get("meas_name").asInstanceOf[String]))

    var tempSeries = scala.collection.mutable.Set[Long]()
    //    var freq = time_interval + timeunit

    var min = measurements(0).get("ts").asInstanceOf[Long]
    var max = measurements(0).get("ts").asInstanceOf[Long]

    for (index <- measurements.indices) {
      var temp = measurements(index).get("ts").asInstanceOf[Long]
      if (temp < min) {
        min = temp
      } else if (temp > max) {
        max = temp
      }
    }

    var temp = min
    tempSeries += temp
    while (temp <= max) {
      if (timeunit.equals("sec")) {
        temp += time_interval
      } else if (timeunit.equals("min")) {
        temp += time_interval * 60
      } else if (timeunit.equals("hour")) {
        temp += time_interval * 60 * 60
      } else if (timeunit.equals("day")) {
        temp += time_interval * 60 * 60 * 24
      }
      tempSeries += temp
    }

    var newfiltered = new ListBuffer[java.util.Map[String, Object]]()
    for (index <- filtered.indices) {
      if (tempSeries.contains(filtered(index).get("ts").asInstanceOf[Long])) {
        // print("Timestamp: ", filtered(index).get("ts").asInstanceOf[Long])

        newfiltered += filtered(index)
      }
    }
    for (index <- notfiltered.indices) {
      newfiltered += notfiltered(index)
    }

    newfiltered
  }
  override def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {

    dataFrame
  }

  override val name: String = "Resample"

  override def destination: String = "measurements_cleansed"
}
