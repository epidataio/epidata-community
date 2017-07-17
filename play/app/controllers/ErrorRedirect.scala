/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import play.api.mvc._

case class ErrorRedirectException(msg: String) extends Exception

/**
 * Controller for error redirection. The primary use case is to display the
 * play error page when a jupyterhub error occurs.
 */
object ErrorRedirect extends Controller with securesocial.core.SecureSocial {

  def show = Action { implicit request =>
    // Trigger an error to display the error page.
    throw new ErrorRedirectException("Error")
  }

}
