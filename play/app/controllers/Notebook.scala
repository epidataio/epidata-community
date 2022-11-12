/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package controllers

import play.api.mvc._
//import securesocial.core.SecureSocial
//import securesocial.core.{ IdentityProvider, RuntimeEnvironment, SecureSocial }
import play.api.i18n.{ I18nSupport, MessagesApi }
import play.api.{ Configuration, Environment }
import javax.inject._

/** Controller to display the Notebook. */
class Notebook @Inject() (val cc: ControllerComponents)(implicit assets: AssetsFinder //  override val env: Environment
//  override val env: RuntimeEnvironment
) extends AbstractController(cc) //  with SecureSocial
{

  override def messagesApi: MessagesApi = super.messagesApi

  def show = Action { implicit request =>
    Ok(views.html.Notebook.show())
  }

}
