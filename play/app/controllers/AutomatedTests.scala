/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package controllers

import java.util.Date
import com.epidata.lib.models.{ Measurement, MeasurementCleansed, MeasurementSummary }
import com.epidata.lib.models.util.JsonHelpers
import models.{ MeasurementService, AutomatedTest, SQLiteMeasurementService }
import play.api.libs.json.JsError
import play.api.libs.json.Json
import play.api.mvc._
import play.api.{ Configuration, Environment, Logger }
import play.api.mvc.{ AnyContent, Request }
import service.DataService
import providers.DemoProvider
import util.Ordering
import javax.inject._
import play.api.i18n.{ I18nSupport, Messages, Lang }
import securesocial.core.{ IdentityProvider, RuntimeEnvironment, SecureSocial }
import service.Configs

/** Controller for automated test data. */
@Singleton
class AutomatedTests @Inject() (val cc: ControllerComponents)(
  override implicit val env: RuntimeEnvironment) extends AbstractController(cc)
  with SecureSocial {

  override def messagesApi = env.messagesApi

  val logger: Logger = Logger(this.getClass())

  def create = SecuredAction(parse.json) { implicit request =>
    val automatedTests = com.epidata.lib.models.AutomatedTest.jsonToAutomatedTests(request.body.toString)
    AutomatedTest.insert(automatedTests.flatMap(x => x), Configs.measDBLite)

    val failedIndexes = automatedTests.zipWithIndex.filter(_._1 == None).map(_._2)
    if (failedIndexes.isEmpty)
      Created
    else {
      val message = "Failed objects: " + failedIndexes.mkString(",")
      BadRequest(Json.obj("status" -> "ERROR", "message" -> message))
    }
  }

  //  def insertKafka = SecuredAction(parse.json) { implicit request =>
  //    val list = com.epidata.lib.models.AutomatedTest.jsonToAutomatedTests(request.body.toString)
  //    models.AutomatedTest.insertToKafka(list.flatMap(x => x))
  //
  //    val failedIndexes = list.zipWithIndex.filter(_._1 == None).map(_._2)
  //    if (failedIndexes.isEmpty)
  //      Created
  //    else {
  //      val message = "Failed objects: " + failedIndexes.mkString(",")
  //      BadRequest(Json.obj("status" -> "ERROR", "message" -> message))
  //    }
  //  }

  def insertQueue = SecuredAction(parse.json) { implicit request =>
    val automatedTests = com.epidata.lib.models.AutomatedTest.jsonToAutomatedTests(request.body.toString)
    if (Configs.queueService.equalsIgnoreCase("Kafka")) {
      models.AutomatedTest.insertToKafka(automatedTests.flatMap(x => x))
    } else if (Configs.queueService.equalsIgnoreCase("ZMQ")) {
      models.AutomatedTest.insertToZMQ(automatedTests.flatMap(x => x))
    } else {
      logger.error("queueService not recognized. Data not written to queue.")
    }

    val failedIndexes = automatedTests.zipWithIndex.filter(_._1 == None).map(_._2)
    if (failedIndexes.isEmpty)
      Created
    else {
      val message = "Failed objects: " + failedIndexes.mkString(",")
      BadRequest(Json.obj("status" -> "ERROR", "message" -> message))
    }
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
    table: String = Measurement.DBTableName) = SecuredAction {
    table match {
      case MeasurementCleansed.DBTableName =>
        Ok(com.epidata.lib.models.AutomatedTestCleansed.toJson(AutomatedTest.queryCleansed(
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
        Ok(com.epidata.lib.models.AutomatedTestSummary.toJson(AutomatedTest.querySummary(
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
        Ok(com.epidata.lib.models.AutomatedTest.toJson(AutomatedTest.query(
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

  @Deprecated
  def query(
    company: String,
    site: String,
    device_group: String,
    tester: String,
    beginTime: Date,
    endTime: Date,
    ordering: Ordering.Value = Ordering.Unspecified) = SecuredAction {
    Ok(com.epidata.lib.models.AutomatedTest.toJson(AutomatedTest.find(
      company,
      site,
      device_group,
      tester,
      beginTime,
      endTime,
      ordering)))
  }
}
