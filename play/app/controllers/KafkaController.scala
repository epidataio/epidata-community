/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import com.epidata.lib.models.util.JsonHelpers
import play.api.libs.json.Json
import play.api.mvc._
import service.DataService

object KafkaController extends Controller with securesocial.core.SecureSocial {

  def create = SecuredAction(parse.json) { implicit request =>
    JsonHelpers.toSensorMeasurement(request.body.toString()) match {
      case Some(sensorMeasurement) =>
        DataService.insert(sensorMeasurement)
        Created
      case _ => BadRequest(Json.obj("status" -> "ERROR", "message" -> "Bad Json Format!"))
    }
  }

  def createList = Action(parse.json) { implicit request =>
    val sensorMeasurements = JsonHelpers.toSensorMeasurements(request.body.toString())
    DataService.insert(sensorMeasurements.flatMap(x => x))
    val failedIndexes = sensorMeasurements.zipWithIndex.filter(_._1 == None).map(_._2)
    if (failedIndexes.isEmpty)
      Created
    else {
      val message = "Failed objects: " + failedIndexes.mkString(",")
      BadRequest(Json.obj("status" -> "ERROR", "message" -> message))
    }
  }

}
