/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import models.SensorMeasurement
import play.api.libs.json.{ JsError, Json }
import play.api.mvc._
import service.{ DataService, KafkaService }

object KafkaController extends Controller with securesocial.core.SecureSocial {

  def create = SecuredAction(parse.json) { implicit request =>
    SensorMeasurement.parseJson(request.body).fold(
      errors => {
        BadRequest(Json.obj("status" -> "ERROR", "message" -> JsError.toFlatJson(errors)))
      },
      sensorMeasurement => {
        DataService.insert(sensorMeasurement)
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
        DataService.insert(sensorMeasurementList)
        Created
      }
    )
  }

}
