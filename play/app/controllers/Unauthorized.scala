/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import play.api.mvc._
import securesocial.core.SecureSocial

/**
 * Controller for error redirection. The primary use case is to display the
 * play error page when a jupyterhub error occurs.
 */
object Unauthorized extends Controller with securesocial.core.SecureSocial {

  def show = Action { implicit request =>
    Ok(views.html.unauthorized.unauthorized())
  }

}
