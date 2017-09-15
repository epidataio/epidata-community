/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import java.util.Date
import com.epidata.lib.models.util.JsonHelpers
import com.epidata.lib.models.MeasurementCleansed
import models.SensorMeasurement
import play.api.libs.json.Json
import play.api.mvc._
import securesocial.core.SecureSocial
import util.{ EpidataMetrics, Ordering }

/** Controller for sensor measurement data. */
object SensorMeasurements extends Controller with SecureSocial {

  def create = SecuredAction(parse.json) { implicit request =>
    JsonHelpers.toSensorMeasurement(request.body.toString()) match {
      case Some(sensorMeasurement) =>
        SensorMeasurement.insert(sensorMeasurement)
        Created
      case _ => BadRequest(Json.obj("status" -> "ERROR", "message" -> "Bad Json Format!"))
    }
  }

  def createList = SecuredAction(parse.json) { implicit request =>
    val sensorMeasurements = JsonHelpers.toSensorMeasurements(request.body.toString())
    SensorMeasurement.insertList(sensorMeasurements.flatMap(x => x))

    val failedIndexes = sensorMeasurements.zipWithIndex.filter(_._1 == None).map(_._2)
    if (failedIndexes.isEmpty)
      Created
    else {
      val message = "Failed objects: " + failedIndexes.mkString(",")
      BadRequest(Json.obj("status" -> "ERROR", "message" -> message))
    }
  }

  def query(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    ordering: Ordering.Value = Ordering.Unspecified
  ) = SecuredAction {
    Ok(SensorMeasurement.toJson(SensorMeasurement.find(
      company,
      site,
      station,
      sensor,
      beginTime,
      endTime,
      ordering
    )))
  }

  def find(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    size: Int = 10000,
    batch: String = "",
    ordering: Ordering.Value = Ordering.Unspecified,
    table: String = MeasurementCleansed.DBTableName
  ) = Action {
    Ok(SensorMeasurement.query(
      company,
      site,
      station,
      sensor,
      beginTime,
      endTime,
      size,
      batch,
      ordering,
      table
    ))
  }

}
