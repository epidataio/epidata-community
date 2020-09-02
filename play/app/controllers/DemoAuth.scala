/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

<<<<<<< Updated upstream
import _root_.providers.DemoProvider
=======
import java.util.UUID

import service.{ DBUserService, AppEnvironment, DataService }
import providers.DemoProvider
>>>>>>> Stashed changes
import play.api.Logger
import play.api.i18n.Messages
import play.api.libs.json.{ JsValue, Json }
import play.api.mvc._
import securesocial.controllers.ProviderController._
import securesocial.core._
import securesocial.core.providers.utils.RoutesHelper
import service.DataService

object DemoAuth extends Controller with securesocial.core.SecureSocial {

  private implicit val readsOAuth2Info = Json.reads[OAuth2Info]
<<<<<<< Updated upstream

  def authenticate = Action { implicit request =>

    Registry.providers.get(DemoProvider.Demo) match {
      case Some(p) => {
        try {
          request.getQueryString("token") match {
            case Some(token) if DataService.isValidToken(token) =>
              val user = p.fillProfile(null)
              completeAuthentication(user, request.session)
            case _ => Redirect(RoutesHelper.login()).flashing("error" -> Messages("Access Denied"))
          }

        } catch {
          case ex: AccessDeniedException => {
            Redirect(RoutesHelper.login()).flashing("error" -> Messages("Access Denied"))
          }

          case other: Throwable => {
            Logger.error("Unable to log user in. An exception was thrown", other)
            Redirect(RoutesHelper.login()).flashing("error" -> Messages("Error LoggingIn"))
=======
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
                    if (env.userService.find(providerId, u.userId) != null) {
                      env.userService.save(u, SaveMode.LoggedIn)
                    }
                    logger.debug(s"$user logged in via $providerId provider")
                    completeAuthentication(u, request.session)
                  case false =>
                    InternalServerError(Json.toJson(Map("error" -> "unexpected internal error"))).as("application/json")
                }
            }
>>>>>>> Stashed changes
          }
        }
      }
      case _ => NotFound
    }

  }

  def authenticateByPost = Action { implicit request =>

    Registry.providers.get(DemoProvider.Demo) match {
      case Some(p) => {
        try {
          val jsonBody: Option[JsValue] = request.body.asJson

          // Expecting json body
          jsonBody.map { json =>
            val token = (json \ "accessToken").as[String]
            if (DataService.isValidToken(token)) {
<<<<<<< Updated upstream
              val user = p.fillProfile(null)
              completeAuthenticationByPost(user, request.session)
=======
              env.authenticatorService.find(CookieAuthenticator.Id) match {
                case Some(builder) => {
                  val provider = env.providers(providerId).asInstanceOf[OAuth2Provider]
                  provider.fillProfile(null).flatMap {
                    user =>
                      user.isInstanceOf[BasicProfile] match {
                        case true =>
                          if (env.userService.find(providerId, user.userId) != null) {
                            env.userService.save(user, SaveMode.LoggedIn)
                          }
                          logger.debug(s"$user logged in via $providerId provider")
                          completeAuthenticationByPost(
                            builder.asInstanceOf[CookieAuthenticatorBuilder[BasicProfile]],
                            user.asInstanceOf[BasicProfile], request.session)
                        case false =>
                          Future.successful(InternalServerError(Json.toJson(Map("error" -> "unexpected internal error"))).as("application/json"))
                      }
                  }
                }
                case None =>
                  Future.successful(InternalServerError(Json.toJson(Map("error" -> "unexpected internal error"))).as("application/json"))
              }
>>>>>>> Stashed changes
            } else {
              NotFound
            }
          }.getOrElse {
            NotFound
          }

        } catch {
          case ex: AccessDeniedException => {
            NotFound
          }

          case other: Throwable => {
            Logger.error("Unable to log user in. An exception was thrown", other)
            NotFound
          }
        }
      }
      case _ => NotFound
    }

  }

  def completeAuthenticationByPost(user: Identity, session: Session)(implicit request: RequestHeader): SimpleResult = {
    val withSession = Events.fire(new LoginEvent(user)).getOrElse(session)
    Authenticator.create(user) match {
      case Right(authenticator) => {
        Ok(Json.obj("sessionId" -> authenticator.toCookie.value))
      }
      case Left(error) => {
        // improve this
        throw new RuntimeException("Error creating authenticator")
      }
    }
  }

}
