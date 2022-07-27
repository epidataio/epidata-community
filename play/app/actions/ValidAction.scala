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

  def filter[A](request: Request[A], block: (Request[A]) => Future[Result]) = Future[Result] {
    val deviceJWT = request.headers.get("jwt_token")
    try {
      val newDeviceJWT = Device.validate(deviceJWT.get)
      Ok(Json.obj("jwt_token" -> deviceJWT.get))
    } catch {
      case _: Throwable => BadRequest(Json.toJson("Unauthorized"))
    }
  }

}
// If timeout, reauthenticate. Always update jwt_token and extending the time
