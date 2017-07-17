/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import java.util.Date
import models.AutomatedTest
import play.api.libs.json.JsError
import play.api.libs.json.Json
import play.api.mvc._
import securesocial.core.SecureSocial
import util.Ordering

/** Controller for automated test data. */
object AutomatedTests extends Controller with SecureSocial {

  def create = SecuredAction(parse.json) { implicit request =>
    AutomatedTest.parseJson(request.body).fold(
      errors => {
        BadRequest(Json.obj("status" -> "ERROR", "message" -> JsError.toFlatJson(errors)))
      },
      automatedTest => {
        AutomatedTest.insert(automatedTest)
        Created
      }
    )
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
    Ok(AutomatedTest.toJson(AutomatedTest.find(
      company,
      site,
      device_group,
      tester,
      beginTime,
      endTime,
      ordering
    )))
  }
}
