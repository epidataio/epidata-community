/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package providers

import javax.inject._

import securesocial.core._
import securesocial.core.OAuth2Provider
import play.api.libs.ws.WSResponse
import securesocial.core.services.{ CacheService, RoutesService }
import play.api.mvc._

import scala.concurrent.{ ExecutionContext, Future }

//@Singleton
class DemoProvider(
  routesService: RoutesService,
  client: OAuth2Client,
  cacheService: CacheService) extends OAuth2Provider(
  routesService,
  client,
  cacheService) {

  val defaultAccessToken = "accessTokenDemo"
  override val id = DemoProvider.Demo
  val userId = "DefaultUser"
  val firstName: Option[String] = Some("FirstName")
  val lastName: Option[String] = Some("LastName")
  val fullName: Option[String] = Some(firstName + " " + lastName)
  val email: Option[String] = None
  val avatarUrl: Option[String] = None
  override val authMethod: AuthenticationMethod = AuthenticationMethod("demo token")
  val oAuth1Info: Option[OAuth1Info] = None
  val oAuth2Info: Option[OAuth2Info] = None
  val passwordInfo: Option[PasswordInfo] = None

  override protected def buildInfo(response: WSResponse): OAuth2Info = {
    OAuth2Info(defaultAccessToken, None, None, None)
  }

  override def fillProfile(info: OAuth2Info): Future[BasicProfile] = {
    Future.successful(BasicProfile(id, userId, firstName, lastName, fullName, email, avatarUrl, authMethod, oAuth2Info = Some(info)))
  }
}

object DemoProvider {
  val Demo = "demo"
  val user = "DefaultUser"
  val authMethod = AuthenticationMethod("demo token")
}
