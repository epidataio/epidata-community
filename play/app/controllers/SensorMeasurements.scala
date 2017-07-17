/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import java.util.Date
import com.epidata.lib.models.{ MeasurementSummary, MeasurementCleansed }
import models.SensorMeasurement
import play.api.libs.json.JsError
import play.api.libs.json.Json
import play.api.mvc._
import securesocial.core.SecureSocial
import util.Ordering

/** Controller for sensor measurement data. */
object SensorMeasurements extends Controller with SecureSocial {

  def create = SecuredAction(parse.json) { implicit request =>
    SensorMeasurement.parseJson(request.body).fold(
      errors => {
        BadRequest(Json.obj("status" -> "ERROR", "message" -> JsError.toFlatJson(errors)))
      },
      sensorMeasurement => {
        SensorMeasurement.insert(sensorMeasurement)
        Created
      }
    )
  }

  def createList = SecuredAction(parse.json) { implicit request =>
    SensorMeasurement.parseJsonList(request.body).fold(
      errors => {
        BadRequest(Json.obj("status" -> "ERROR", "message" -> JsError.toFlatJson(errors)))
      },
      sensorMeasurementList => {
        SensorMeasurement.insertList(sensorMeasurementList)
        Created
      }
    )
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
