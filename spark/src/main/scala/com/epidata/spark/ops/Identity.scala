/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.spark.ops

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.epidata.spark.{ Measurement, MeasurementCleansed }
//import com.epidata.lib.models.{ Measurement => BaseMeasurement, SensorMeasurement => BaseSensorMeasurement, AutomatedTest => BaseAutomatedTest }
import scala.collection.mutable.{ Map => MutableMap, ListBuffer }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import java.util.logging._

class Identity(
    val meas_names: Option[List[String]]) extends Transformation {

  def this() = this(None)

  override def apply(measurements: ListBuffer[java.util.Map[String, Object]]): ListBuffer[java.util.Map[String, Object]] = {
    logger.log(Level.FINE, "Identity transformation apply method invoked.")
    // logger.log(Level.INFO, "meas_names: " + meas_names)

    if (meas_names.isDefined) {
      val filteredMeasurementsCollection = new ListBuffer[java.util.Map[String, Object]]()
      // logger.log(Level.INFO, "filteredMeasurementsCollection: " + filteredMeasurementsCollection)

      for (meas_name <- meas_names.get) {
        val filteredMeasurements = measurements
          .filter(m => meas_name.equals(m.get("meas_name").asInstanceOf[String]))

        // logger.log(Level.INFO, "filteredMeasurements: " + filteredMeasurements)

        for (index <- filteredMeasurements.indices) {
          if ((filteredMeasurements(index).get("meas_flag") == null) || (filteredMeasurements(index).get("meas_flag") == "")) {
            filteredMeasurements(index).put("meas_flag", "")
          }
          if ((filteredMeasurements(index).get("meas_method") == null) || (filteredMeasurements(index).get("meas_method") == "")) {
            filteredMeasurements(index).put("meas_method", "")
          }
        }

        filteredMeasurementsCollection ++= filteredMeasurements
      }

      // logger.log(Level.INFO, "filteredMeasurementsCollection: " + filteredMeasurementsCollection)

      filteredMeasurementsCollection
    } else {
      for (index <- measurements.indices) {
        if (measurements(index).get("meas_flag") == null) {
          measurements(index).put("meas_flag", null)
        }
        if (measurements(index).get("meas_method") == null) {
          measurements(index).put("meas_method", null)
        }
      }

      measurements
    }
  }

  override val name: String = "Identity"
  override def destination: String = "measurements_cleansed"
}
