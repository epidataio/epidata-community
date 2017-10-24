/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import java.util.Date
import com.epidata.lib.models.MeasurementCleansed
import com.epidata.lib.models.util.JsonHelpers
import models.{ MeasurementService, AutomatedTest }
import play.api.libs.json.JsError
import play.api.libs.json.Json
import play.api.mvc._
import securesocial.core.SecureSocial
import service.DataService
import util.Ordering

/** Controller for automated test data. */
object AutomatedTests extends Controller with SecureSocial {

  def create = SecuredAction(parse.json) { implicit request =>
    com.epidata.lib.models.AutomatedTest.jsonToAutomatedTest(request.body.toString()) match {
      case Some(m) =>
        AutomatedTest.insert(m)
        Created
      case _ => BadRequest(Json.obj("status" -> "ERROR", "message" -> "Bad Json Format!"))
    }
  }

  def createList = SecuredAction(parse.json) { implicit request =>
    val automatedTests = com.epidata.lib.models.AutomatedTest.jsonToAutomatedTests(request.body.toString())
    AutomatedTest.insertList(automatedTests.flatMap(x => x))

    val failedIndexes = automatedTests.zipWithIndex.filter(_._1 == None).map(_._2)
    if (failedIndexes.isEmpty)
      Created
    else {
      val message = "Failed objects: " + failedIndexes.mkString(",")
      BadRequest(Json.obj("status" -> "ERROR", "message" -> message))
    }
  }

  def insertKafka = SecuredAction(parse.json) { implicit request =>

    val list = com.epidata.lib.models.AutomatedTest.jsonToAutomatedTests(request.body.toString())
    models.AutomatedTest.insertToKafka(list.flatMap(x => x))

    val failedIndexes = list.zipWithIndex.filter(_._1 == None).map(_._2)
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
    device_group: String,
    tester: String,
    beginTime: Date,
    endTime: Date,
    ordering: Ordering.Value = Ordering.Unspecified
  ) = SecuredAction {
    Ok(com.epidata.lib.models.AutomatedTest.toJson(AutomatedTest.find(
      company,
      site,
      device_group,
      tester,
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
  ) = SecuredAction {
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
      com.epidata.lib.models.AutomatedTest.NAME
    ))
  }
}
