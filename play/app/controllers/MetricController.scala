/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package controllers

import _root_.util.EpidataMetrics
//import play.api.mvc.{ Action, AbstractController }
import play.api.mvc._
import play.api.libs.json._
import javax.inject._
import play.api.i18n.{ I18nSupport, Messages }
import play.api.{ Configuration, Environment }
//import securesocial.core.{ IdentityProvider, RuntimeEnvironment, SecureSocial }

@Singleton
class MetricController @Inject() (val cc: ControllerComponents)( //  override implicit val env: Environment
//  override implicit val env: RuntimeEnvironment
) extends AbstractController(cc)
  //  with SecureSocial
  with I18nSupport {

  //  override def messagesApi = env.messagesApi

  def getMetric() = Action {
    Ok(Json.toJson(EpidataMetrics.getMetric))
  }
}
