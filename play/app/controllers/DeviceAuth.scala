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
  val env: AppEnvironment,
  implicit val conf: Configuration) extends AbstractController(cc) {

  def authenticate = Action.async { implicit request =>
    var deviceID = ""
    var deviceToken = ""
    println("DeviceAuth: " + request.headers)
    try {
      deviceID = request.headers.get("device_id").get
      deviceToken = request.headers.get("device_token").get
      println("DeviceAuthAfter: " + deviceID, deviceToken)
      try {
        val deviceJWT = Device.authenticate(deviceID, deviceToken)
        Future.successful(Ok(Json.obj("device_jwt" -> deviceJWT)).withHeaders("device_jwt" -> deviceJWT))
      } catch {
        case _: Throwable => Future.successful(BadRequest(Json.obj("status" -> "ERROR", "message" -> "incorrect id or token")))
      }
    } catch {
      case _: Throwable => Future.successful(BadRequest(Json.obj("status" -> "Internal Server Error", "message" -> "empty id or token")))
    }
  }
}
