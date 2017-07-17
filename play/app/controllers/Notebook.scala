/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import play.api.mvc._
import securesocial.core.SecureSocial

/** Controller to display the Notebook. */
object Notebook extends Controller with SecureSocial {

  def show = SecuredAction { implicit request =>
    Ok(views.html.Notebook.show())
  }

}
