/*
 * Copyright (c) 2022 EpiData, Inc.
*/

package actions

import play.api.mvc._
import scala.concurrent._
import play.api.Logger
import play.api.libs.json.{ JsValue, Json }
import play.api.mvc._
import play.api.{ Environment, Configuration }
import play.api.i18n.{ I18nSupport, MessagesApi, Messages }
import play.api.libs.ws.WSResponse
import scala.collection.immutable.ListMap
import scala.util.{ Success, Failure }
import play.api.mvc.Results._
import models._
import javax.inject._
import service.{ DBUserService, AppEnvironment, DataService }

@Singleton
class ValidAction @Inject() (override val parser: BodyParsers.Default)(implicit ec: ExecutionContext)
  extends ActionBuilderImpl(parser) {

  // always runs when ValidAction is used to check if JWT token is valid
  override def invokeBlock[A](request: Request[A], block: (Request[A]) => Future[Result]): Future[Result] = {
    var deviceJwt = ""
    // checks if there is a token in the header
    try {
      deviceJwt = request.headers.get("device_jwt").get
    } catch {
      case _: Throwable => Future.successful(BadRequest(Json.toJson("Unauthorized")))
    }
    try {
      // creates a new request with Device.validate and sends it to the block
      val newDeviceJWT = Device.validate(deviceJwt)
      val header = Headers(("device_jwt", newDeviceJWT))
      val newRequest = request.withHeaders(header)
      block(newRequest)
    } catch {
      // only runs if token is not validated
      case _: Throwable => Future.successful(BadRequest(Json.toJson("Unauthorized")))
    }
  }

}
