/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package controllers

import javax.inject._
import service.{ DBUserService, AppEnvironment, DataService }
import play.api.mvc._
import scala.concurrent.Future
import play.api.Logger
import play.api.libs.json.{ JsValue, Json }
import play.api.{ Environment, Configuration }
import play.api.i18n.{ I18nSupport, MessagesApi, Messages }
import play.api.libs.ws.WSResponse
import scala.collection.immutable.ListMap
import scala.util.{ Success, Failure }
import models._
@Singleton
class DeviceAuth @Inject() (val cc: ControllerComponents)(
  implicit
  val env: AppEnvironment) extends AbstractController(cc) {

  // authentication for app/command line that utilizes headers
  def authenticateApp = Action.async { implicit request =>
    var deviceID = ""
    var deviceToken = ""
    try {
      // retrieves id and token from header
      deviceID = request.headers.get("device_id").get
      deviceToken = request.headers.get("device_token").get
      try {
        // authenticates and returns jwt in header
        val deviceJWT = Device.authenticate(deviceID, deviceToken)
        Future.successful(Ok(Json.obj("device_jwt" -> deviceJWT)).withHeaders("device_jwt" -> deviceJWT))
      } catch {
        // gives this status if invalid token/id pair is given
        case _: Throwable => Future.successful(BadRequest(Json.obj("status" -> "ERROR", "message" -> "incorrect id or token")))
      }
      // only catches when id or token are None
    } catch {
      case _: Throwable => Future.successful(BadRequest(Json.obj("status" -> "Internal Server Error", "message" -> "empty id or token")))
    }
  }

  // authentication for web interface that utilizes body
  def authenticateWeb = Action.async { implicit request =>
    var deviceID = ""
    var deviceToken = ""
    var content: AnyContent = request.body
    // converst the body to a string
    val deviceString: String = content.asText match {
      case None => ""
      case Some(s: String) => s
    }
    // gives error if body is empty
    if (deviceString == "") {
      Future.successful(BadRequest(Json.obj("status" -> "Internal Server Error", "message" -> "empty id or token")))
    } else {
      // turns the body into a JSValue object
      val res: JsValue = Json.parse(deviceString)
      try {
        // retrieves id and token from body
        deviceID = (res \ "device_id").as[String]
        deviceToken = (res \ "device_token").as[String]
        try {
          // authenticates and returns jwt in header
          val deviceJWT = Device.authenticate(deviceID, deviceToken)
          Future.successful(Ok(Json.obj("device_jwt" -> deviceJWT)).withHeaders("device_jwt" -> deviceJWT))
        } catch {
          // gives this status if invalid token/id pair is given
          case _: Throwable => Future.successful(BadRequest(Json.obj("status" -> "ERROR", "message" -> "incorrect id or token")))
        }
        // only catches when id or token are None
      } catch {
        case _: Throwable => Future.successful(BadRequest(Json.obj("status" -> "Internal Server Error", "message" -> "empty id or token")))
      }
    }
  }
}
