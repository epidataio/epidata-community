/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package controllers

import play.api.mvc._
import javax.inject._
import play.api.i18n.{ I18nSupport, Messages }
import securesocial.core.{ IdentityProvider, RuntimeEnvironment, SecureSocial }

case class ErrorRedirectException(msg: String) extends Exception

/**
 * Controller for error redirection. The primary use case is to display the
 * play error page when a jupyterhub error occurs.
 */
class ErrorRedirect @Inject() (val cc: ControllerComponents)(
  override implicit val env: RuntimeEnvironment) extends AbstractController(cc)
  with SecureSocial with I18nSupport {

  override def messagesApi = env.messagesApi

  def show = Action { implicit request =>
    // Trigger an error to display the error page.
    throw new ErrorRedirectException("Error")
  }

}
