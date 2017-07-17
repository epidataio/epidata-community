/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package providers

import securesocial.core._
import play.api.Application
import play.api.libs.ws.Response

class DemoProvider(application: Application) extends OAuth2Provider(application) {
  val defaultAccessToken = "accessTokenDemo"
  val userId = "DefaultUser"

  override def id = DemoProvider.Demo

  val identityId: IdentityId = IdentityId(userId.toString, id)
  val firstName: String = "FirstName"
  val lastName: String = "LastName"
  val fullName: String = firstName + " " + lastName
  val email: Option[String] = None
  val avatarUrl: Option[String] = None
  val oAuth1Info: Option[OAuth1Info] = None
  val oAuth2Info: Option[OAuth2Info] = None
  val passwordInfo: Option[PasswordInfo] = None

  override protected def buildInfo(response: Response): OAuth2Info = {
    OAuth2Info(
      defaultAccessToken, None, None, None
    )
  }

  def fillProfile(user: SocialUser): SocialUser =
    SocialUser(identityId, firstName, lastName, fullName, email, avatarUrl, authMethod, oAuth1Info, oAuth2Info, passwordInfo)

}

object DemoProvider {
  val Demo = "demo"
}
