/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package service

import models.User
import play.api.{ Logger, Application }
import securesocial.core._
import securesocial.core.providers.Token

/** A user service in Scala using a Cassandra backend. */
class CassandraUserService(application: Application) extends UserServicePlugin(application) {
  val logger = Logger("application.controllers.CassandraUserService")

  def find(id: IdentityId): Option[Identity] = User.find(id)

  def findByEmailAndProvider(email: String, providerId: String): Option[Identity] = {
    // No UsernamePassword provider in use.
    None
  }

  def save(user: Identity): Identity = {

    // Do not allow creation of new user accounts. Accounts are created
    // manually during the restricted invite period.
    if (find(user.identityId).isEmpty) {
      throw new AuthenticationException
    }

    val socialUser = SocialUser(
      user.identityId,
      user.firstName,
      user.lastName,
      user.fullName,
      user.email,
      user.avatarUrl,
      user.authMethod,
      user.oAuth1Info,
      user.oAuth2Info,
      user.passwordInfo
    )

    User.save(socialUser)
    socialUser
  }

  def link(current: Identity, to: Identity) {
    // No UsernamePassword provider in use.
  }

  def save(token: Token) {
    // No UsernamePassword provider in use.
  }

  def findToken(token: String): Option[Token] = {
    // No UsernamePassword provider in use.
    None
  }

  def deleteToken(uuid: String) {
    // No UsernamePassword provider in use.
  }

  def deleteTokens() {
    // No UsernamePassword provider in use.
  }

  def deleteExpiredTokens() {
    // No UsernamePassword provider in use.
  }
}
