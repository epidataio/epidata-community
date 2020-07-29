/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import java.util.UUID

import service.{ CassandraUserService, AppEnvironment, DataService }
import providers.DemoProvider
import play.api.Logger
import play.api.libs.json.{ JsValue, Json }
import play.api.mvc._
import play.api.{ Environment, Configuration }
import play.api.i18n.{ I18nSupport, MessagesApi, Messages }
import play.api.libs.ws.WSResponse
import securesocial.controllers.BaseProviderController
import securesocial.controllers.ProviderController
import securesocial.controllers._
import securesocial.core._
import securesocial.core.authenticator._
import securesocial.core.utils._
import securesocial.core.authenticator.AuthenticatorBuilder
import securesocial.core.authenticator.CookieAuthenticatorBuilder
import securesocial.core.services.SaveMode
import securesocial.core.services.{ CacheService, RoutesService }
import javax.inject._

import scala.collection.immutable.ListMap
import scala.concurrent.Future
import scala.util.{ Success, Failure }

@Singleton
class DemoAuth @Inject() (val cc: ControllerComponents)(
    implicit
    val env: RuntimeEnvironment,
    implicit val conf: Configuration) extends AbstractController(cc) with SecureSocial {

  private implicit val readsOAuth2Info = Json.reads[OAuth2Info]
  val providerId = DemoProvider.Demo
  val logger: Logger = Logger(this.getClass())
  override def messagesApi: MessagesApi = super.messagesApi

  def authenticate = Action.async { implicit request =>
    conf.get[Boolean]("securesocial.useDefaultLogin") match {
      case true => {
        request.getQueryString("token") match {
          case Some(token) if DataService.isValidToken(token) => {
            val provider = env.providers(providerId)
            val user: Future[BasicProfile] = provider.asInstanceOf[OAuth2Provider].fillProfile(null)
            user.map {
              u =>
                u.isInstanceOf[BasicProfile] match {
                  case true =>
                    logger.debug(s"$user logged in via $providerId provider")
                    completeAuthentication(u, request.session)
                  case false =>
                    InternalServerError(Json.toJson(Map("error" -> "unexpected internal error"))).as("application/json")
                }
            }
          }
          case _ =>
            Future.successful(Redirect(securesocial.controllers.routes.LoginPage.login).flashing("error" -> Messages("Access Denied")))
        }
      }
      case false => {
        logger.error("Unable to log user in. Only Default Login is Supported")
        Future.successful(Redirect(securesocial.controllers.routes.LoginPage.login).flashing("error" -> Messages("Login Mode Not Supported")))
      }
    }
  }

  def authenticateByPost = Action.async { implicit request =>
    conf.get[Boolean]("securesocial.useDefaultLogin") match {
      case true => {
        try {
          val jsonBody: Option[JsValue] = request.body.asJson
          jsonBody.map { json =>
            val token = (json \ "accessToken").as[String]
            if (DataService.isValidToken(token)) {
              val provider = env.providers(providerId)
              val user: Future[BasicProfile] = provider.asInstanceOf[OAuth2Provider].fillProfile(null)
              user.map {
                u =>
                  u.isInstanceOf[BasicProfile] match {
                    case true =>
                      logger.debug(s"$user logged in via $providerId provider")
                      completeAuthenticationByPost(u, request.session)
                    case false =>
                      InternalServerError(Json.toJson(Map("error" -> "unexpected internal error"))).as("application/json")
                  }
              }
            } else {
              Future.successful(Unauthorized("Token is invalid"))
            }
          }.getOrElse {
            Future.successful(Unauthorized("Error parsing request data"))
          }
        } catch {
          case e: Exception => {
            logger.error("Unable to log user in. An exception was thrown")
            Future.successful(InternalServerError(Json.toJson(Map("error" -> "unexpected error"))).as("application/json"))
          }
        }
      }
      case _ => {
        logger.error("User login mode is not supported")
        Future.successful(Unauthorized("Unable to log user in"))
      }
    }
  }

  def completeAuthentication(user: BasicProfile, session: Session)(implicit request: RequestHeader): Result = {
    val withSession = Events.fire(new LoginEvent(user)).getOrElse(session)
    val sessionId = UUID.randomUUID().toString
    Ok(Json.obj("sessionId" -> sessionId))
  }

  def completeAuthenticationByPost(user: BasicProfile, session: Session)(implicit request: RequestHeader): Result = {
    val withSession = Events.fire(new LoginEvent(user)).getOrElse(session)
    val sessionId = UUID.randomUUID().toString
    Ok(Json.obj("sessionId" -> sessionId))
  }

}
