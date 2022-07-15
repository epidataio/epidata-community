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

case class DeviceRequestInfo(device_id: String, device_token: String)

class Devices @Inject() (val cc: ControllerComponents)(implicit assets: AssetsFinder, implicit val conf: Configuration) extends AbstractController(cc) {
  val deviceForm = Form(
    mapping(
      "device_id" -> text,
      "device_token" -> text)(DeviceRequestInfo.apply)(DeviceRequestInfo.unapply))
  override def messagesApi: MessagesApi = super.messagesApi

  val msg: Option[String] = None
  // form: Form[(String, String)],
  // msg: Option[String] = None)(implicit request: RequestHeader): Html = {

  def show = Action { implicit request =>
    Ok(views.html.Device.auth(deviceForm, msg))
  }

}
