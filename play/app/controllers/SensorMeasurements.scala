/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import java.util.Date
import com.epidata.lib.models.util.JsonHelpers
import com.epidata.lib.models.MeasurementCleansed
<<<<<<< Updated upstream
import models.{ MeasurementService, SensorMeasurement }
import play.api.libs.json.Json
import play.api.mvc._
import securesocial.core.SecureSocial
import util.{ EpidataMetrics, Ordering }
=======
import models.{ MeasurementService, SensorMeasurement, SQLiteMeasurementService }
import util.{ EpidataMetrics, Ordering }
import play.api.libs.json.Json
import play.api.mvc._
import play.api.i18n.{ I18nSupport, Messages }
import securesocial.core.{ IdentityProvider, RuntimeEnvironment, SecureSocial }
import service.Configs
>>>>>>> Stashed changes

/** Controller for sensor measurement data. */
object SensorMeasurements extends Controller with SecureSocial {

  def create = SecuredAction(parse.json) { implicit request =>
<<<<<<< Updated upstream
    val sensorMeasurements = com.epidata.lib.models.SensorMeasurement.jsonToSensorMeasurements(request.body.toString())
    SensorMeasurement.insert(sensorMeasurements.flatMap(x => x))
=======
    val sensorMeasurements = com.epidata.lib.models.SensorMeasurement.jsonToSensorMeasurements(request.body.toString)
    SensorMeasurement.insert(sensorMeasurements.flatMap(x => x), Configs.DBMeas)
>>>>>>> Stashed changes

    val failedIndexes = sensorMeasurements.zipWithIndex.filter(_._1 == None).map(_._2)
    if (failedIndexes.isEmpty)
      Created
    else {
      val message = "Failed objects: " + failedIndexes.mkString(",")
      BadRequest(Json.obj("status" -> "ERROR", "message" -> message))
    }
  }

  def insertKafka = SecuredAction(parse.json) { implicit request =>

    val sensorMeasurements = com.epidata.lib.models.SensorMeasurement.jsonToSensorMeasurements(request.body.toString())
    models.SensorMeasurement.insertToKafka(sensorMeasurements.flatMap(x => x))

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
<<<<<<< Updated upstream
    table: String = MeasurementCleansed.DBTableName
  ) = Action {
    Ok(MeasurementService.query(
      company,
      site,
      station,
      sensor,
      beginTime,
      endTime,
      size,
      batch,
      ordering,
      table,
      com.epidata.lib.models.SensorMeasurement.NAME
    ))
=======
    table: String = MeasurementCleansed.DBTableName) = Action {
    if (Configs.DBMeas) {
      Ok(SQLiteMeasurementService.query(
        company,
        site,
        station,
        sensor,
        beginTime,
        endTime,
        size,
        batch,
        ordering,
        table,
        com.epidata.lib.models.SensorMeasurement.NAME))
    } else {
      Ok(MeasurementService.query(
        company,
        site,
        station,
        sensor,
        beginTime,
        endTime,
        size,
        batch,
        ordering,
        table,
        com.epidata.lib.models.SensorMeasurement.NAME))
    }
>>>>>>> Stashed changes
  }
}
