/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.lib.models.util

import java.util.Date

import com.epidata.lib.models.{ Measurement, SensorMeasurement }
import org.json.simple.JSONObject
import org.json.simple.parser.{ ParseException, JSONParser }

object JsonHelpers {

  def toJson(str: String): Option[JSONObject] = {
    val parser = new JSONParser()
    try {
      val jSONObject = parser.parse(str).asInstanceOf[JSONObject]
      Some(jSONObject)
    } catch {
      case e: ParseException => None
      case _: Throwable => None
    }
  }

  def toSensorMeasurement(str: String): Option[SensorMeasurement] = {
    toJson(str) match {
      case Some(jSONObject) => Some(toSensorMeasurement(jSONObject))
      case _ => None
    }
  }

  def toSensorMeasurement(jSONObject: JSONObject): SensorMeasurement = {
    val company: String = jSONObject.get("company").asInstanceOf[String]
    val site: String = jSONObject.get("site").asInstanceOf[String]
    val station: String = jSONObject.get("station").asInstanceOf[String]
    val sensor: String = jSONObject.get("sensor").asInstanceOf[String]
    val ts: Date = new Date(jSONObject.get("ts").asInstanceOf[Long])
    val event: String = jSONObject.get("event").asInstanceOf[String]
    val meas_name: String = jSONObject.get("meas_name").asInstanceOf[String]

    val meas_unit: Option[String] = Measurement.blankToNone(jSONObject.get("meas_unit").asInstanceOf[String])
    val meas_status: Option[String] = Measurement.blankToNone(jSONObject.get("meas_status").asInstanceOf[String])
    val meas_description: Option[String] = Measurement.blankToNone(jSONObject.get("meas_description").asInstanceOf[String])

    val meas_value_jsonObject = jSONObject.get("meas_value")
    val meas_lower_limit_jsonObject = jSONObject.get("meas_lower_limit")
    val meas_upper_limit_jsonObject = jSONObject.get("meas_upper_limit")

    val datatype = Datatype.byName(jSONObject.get("meas_datatype").asInstanceOf[String])
    val meas_value = datatype match {
      case Datatype.Double => meas_value_jsonObject.asInstanceOf[Double]
      case Datatype.Long => meas_value_jsonObject.asInstanceOf[Long]
      case Datatype.String => Measurement.blankToNone(meas_value_jsonObject.asInstanceOf[String])
      case _ => Binary.fromBase64(datatype, meas_value_jsonObject.asInstanceOf[String])
    }
    val meas_lower_limit = datatype match {
      case Datatype.Double => Some(meas_lower_limit_jsonObject.asInstanceOf[Double])
      case Datatype.Long => Some(meas_lower_limit_jsonObject.asInstanceOf[Long])
      case _ => None
    }
    val meas_upper_limit = datatype match {
      case Datatype.Double => Some(meas_upper_limit_jsonObject.asInstanceOf[Double])
      case Datatype.Long => Some(meas_upper_limit_jsonObject.asInstanceOf[Long])
      case _ => None
    }

    SensorMeasurement(
      company,
      site,
      station,
      sensor,
      ts,
      event,
      meas_name,
      meas_value,
      meas_unit,
      meas_status,
      meas_lower_limit,
      meas_upper_limit,
      meas_description
    )
  }

}
