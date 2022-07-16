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
    val deviceID = request.getQueryString("device_id").get
    val deviceToken = request.getQueryString("device_token").get
    //    println("DeviceAuth: ", deviceID, deviceToken)
    try {
      val deviceJWT = Device.authenticate(deviceID, deviceToken)
      Future.successful(Ok(Json.obj("device_jwt" -> deviceJWT)))
    } catch {
      case _: Throwable => Future.successful(BadRequest(Json.obj("status" -> "ERROR", "message" -> "incorrect id or token")))
    }
  }
}