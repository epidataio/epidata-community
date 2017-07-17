/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import _root_.providers.DemoProvider
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

  def authenticate = Action { implicit request =>

    Registry.providers.get(DemoProvider.Demo) match {
      case Some(p) => {
        try {
          val user = p.fillProfile(null)
          completeAuthentication(user, request.session)

        } catch {
          case ex: AccessDeniedException => {
            Redirect(RoutesHelper.login()).flashing("error" -> Messages("securesocial.login.accessDenied"))
          }

          case other: Throwable => {
            Logger.error("Unable to log user in. An exception was thrown", other)
            Redirect(RoutesHelper.login()).flashing("error" -> Messages("securesocial.login.errorLoggingIn"))
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
              val user = p.fillProfile(null)
              completeAuthenticationByPost(user, request.session)
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
