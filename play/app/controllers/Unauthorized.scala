/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package controllers

import javax.inject._

import play.api.mvc._
import play.api.i18n.{ I18nSupport, MessagesApi }
import securesocial.core.{ IdentityProvider, RuntimeEnvironment, SecureSocial }

/**
 * Controller for error redirection. The primary use case is to display the
 * play error page when a jupyterhub error occurs.
 */
class Unauthorized @Inject() (val cc: ControllerComponents)(
  override implicit val env: RuntimeEnvironment) extends AbstractController(cc)
  with SecureSocial {

  override def messagesApi: MessagesApi = super.messagesApi

  def show = Action { implicit request =>
    Ok(views.html.unauthorized.unauthorized())
  }

}
