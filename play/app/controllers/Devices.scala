/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import play.api.mvc._

import play.api.i18n.{ I18nSupport, MessagesApi }
import models._
import javax.inject._
import play.api.mvc._

import play.api.data._
import play.api.data.Forms._

import play.api._
import play.api.data._
import models.Device
import play.api.libs.ws._
import scala.concurrent._
import scala.concurrent.duration._
import play.api.inject.guice.GuiceApplicationBuilder

case class DeviceRequestInfo(device_id: String, device_token: String)

class Devices @Inject() (val cc: ControllerComponents, ws: WSClient)(implicit assets: AssetsFinder, implicit val conf: Configuration) extends AbstractController(cc) {
  val deviceForm = Form(
    mapping(
      "device_id" -> text,
      "device_token" -> text)(DeviceRequestInfo.apply)(DeviceRequestInfo.unapply))
  override def messagesApi: MessagesApi = super.messagesApi

  // val msg: Option[String] = None
  // form: Form[(String, String)],
  // msg: Option[String] = None)(implicit request: RequestHeader): Html = {

  def show = Action { implicit request =>
    Ok(views.html.Device.auth(deviceForm))
  }

  /*
  def devicePost() = Action { implicit request =>
    deviceForm.bindFromRequest.fold(
      formWithErrors => {
        // binding failure, you retrieve the form containing errors:
        BadRequest(views.html.Device.auth(formWithErrors))
      },
      deviceRequestInfo => {
        /* binding success, you get the actual value. */
        /* flashing uses a short lived cookie */
        println("Form: " + deviceRequestInfo.device_id, deviceRequestInfo.device_token)
        // val result = Redirect("/authenticate/device").withHeaders("device_id" -> deviceRequestInfo.device_id, "device_token" -> deviceRequestInfo.device_token)
        // result
        val response = Await.result(ws.url("https://localhost:9443/authenticate/device").withHttpHeaders(("device_id", deviceRequestInfo.device_id), ("device_token", deviceRequestInfo.device_token)).get(), 1000 millis)
        val status = response.status
        if (status == OK) {
          Ok(views.html.Device.authorized())
        } else {
          Ok(views.html.unauthorized.unauthorized())
        }
      })
  }
  */

}
