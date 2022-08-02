/*
 * Copyright (c) 2015-2022 EpiData, Inc.
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

class Devices @Inject() (val cc: ControllerComponents)(implicit assets: AssetsFinder, implicit val conf: Configuration) extends AbstractController(cc) {

  // form that is used in webpage
  val deviceForm = Form(
    mapping(
      "device_id" -> text,
      "device_token" -> text)(DeviceRequestInfo.apply)(DeviceRequestInfo.unapply))
  override def messagesApi: MessagesApi = super.messagesApi

  // shows webpage
  def show = Action { implicit request =>
    Ok(views.html.Device.auth(deviceForm))
  }

}

// class used to store id/token from form
case class DeviceRequestInfo(device_id: String, device_token: String)
