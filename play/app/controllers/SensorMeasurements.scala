/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package controllers

import javax.inject._
import java.util.Date

import com.epidata.lib.models.util.JsonHelpers
import com.epidata.lib.models.{ Measurement, MeasurementCleansed, MeasurementSummary }
import models.{ MeasurementService, SQLiteMeasurementService, SensorMeasurement, Device }
import util.{ EpidataMetrics, Ordering }
import play.api.libs.json._
import play.api.mvc._
import play.api.i18n.{ I18nSupport, Messages }
import play.api.{ Configuration, Environment, Logger }
import service.{ AppEnvironment, Configs }
import securesocial.core.{ IdentityProvider, RuntimeEnvironment, SecureSocial }
import service.Configs
import service._
import actions.ValidAction

/** Controller for sensor measurement data. */
@Singleton
class SensorMeasurements @Inject() (val cc: ControllerComponents, validAction: ValidAction)(
  override implicit val env: RuntimeEnvironment) extends AbstractController(cc)
  with SecureSocial {

  override def messagesApi = env.messagesApi

  val logger: Logger = Logger(this.getClass())

  def create = validAction(parse.json) { implicit request: Request[JsValue] =>
    val sensorMeasurements = com.epidata.lib.models.SensorMeasurement.jsonToSensorMeasurements(request.body.toString)
    SensorMeasurement.insert(sensorMeasurements.flatMap(x => x), Configs.measDBLite)

    val failedIndexes = sensorMeasurements.zipWithIndex.filter(_._1 == None).map(_._2)
    if (failedIndexes.isEmpty)
      Created
    else {
      val message = "Failed objects: " + failedIndexes.mkString(",")
      BadRequest(Json.obj("status" -> "ERROR", "message" -> message))
    }
  }

  //  def insertKafka = SecuredAction(parse.json) { implicit request =>
  //    val sensorMeasurements = com.epidata.lib.models.SensorMeasurement.jsonToSensorMeasurements(request.body.toString)
  //    models.SensorMeasurement.insertToKafka(sensorMeasurements.flatMap(x => x))
  //
  //    val failedIndexes = sensorMeasurements.zipWithIndex.filter(_._1 == None).map(_._2)
  //    if (failedIndexes.isEmpty)
  //      Created
  //    else {
  //      val message = "Failed objects: " + failedIndexes.mkString(",")
  //      BadRequest(Json.obj("status" -> "ERROR", "message" -> message))
  //    }
  //  }

  def insertQueue = validAction(parse.json) { implicit request =>
    val sensorMeasurements = com.epidata.lib.models.SensorMeasurement.jsonToSensorMeasurements(request.body.toString)
    if (Configs.queueService.equalsIgnoreCase("Kafka")) {
      models.SensorMeasurement.insertToKafka(sensorMeasurements.flatMap(x => x))
    } else if (Configs.queueService.equalsIgnoreCase("ZMQ")) {
      models.SensorMeasurement.insertToZMQ(sensorMeasurements.flatMap(x => x))
    } else {
      logger.error("queueService not recognized. Data not written to queue.")
    }

    val failedIndexes = sensorMeasurements.zipWithIndex.filter(_._1 == None).map(_._2)
    if (failedIndexes.isEmpty)
      Created
    else {
      val message = "Failed objects: " + failedIndexes.mkString(",")
      BadRequest(Json.obj("status" -> "ERROR", "message" -> message))
    }
  }

  @Deprecated
  def query(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    ordering: Ordering.Value = Ordering.Unspecified) = SecuredAction {
    Ok(SensorMeasurement.toJson(SensorMeasurement.find(
      company,
      site,
      station,
      sensor,
      beginTime,
      endTime,
      ordering)))
  }

  def find(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    size: Int = 1000,
    batch: String = "",
    ordering: Ordering.Value = Ordering.Unspecified,
    table: String = MeasurementCleansed.DBTableName) = validAction(parse.json) {
    table match {
      case MeasurementCleansed.DBTableName =>
        Ok(com.epidata.lib.models.SensorMeasurementCleansed.toJson(SensorMeasurement.queryCleansed(
          company,
          site,
          station,
          sensor,
          beginTime,
          endTime,
          size,
          batch,
          ordering,
          Configs.measDBLite)))
      case MeasurementSummary.DBTableName =>
        Ok(com.epidata.lib.models.SensorMeasurementSummary.toJson(SensorMeasurement.querySummary(
          company,
          site,
          station,
          sensor,
          beginTime,
          endTime,
          size,
          batch,
          ordering,
          Configs.measDBLite)))
      case _ =>
        Ok(com.epidata.lib.models.SensorMeasurement.toJson(SensorMeasurement.query(
          company,
          site,
          station,
          sensor,
          beginTime,
          endTime,
          size,
          batch,
          ordering,
          Configs.measDBLite)))
    }
  }

}
