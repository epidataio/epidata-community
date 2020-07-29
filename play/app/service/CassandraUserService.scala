/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package service

import models.User
import scala.concurrent.Future
import play.api.libs.ws._
import play.api.{ Logger, Application }
import securesocial.core._
import securesocial.core.{ PasswordInfo, BasicProfile }
import play.api.{ Environment, Configuration }
import securesocial.core.services._
import securesocial.core.providers.MailToken

import javax.inject._

/** A user service in Scala using a Cassandra backend. */
@Singleton
class CassandraUserService extends UserService[BasicProfile] {
  val logger = Logger("application.controllers.CassandraUserService")

  var users = Map[(String, String), BasicProfile]()
  private var tokens = Map[String, MailToken]()

  def find(providerId: String, userId: String): Future[Option[BasicProfile]] = Future.successful(User.find(providerId, userId))

  override def findByEmailAndProvider(email: String, providerId: String): Future[Option[BasicProfile]] = {
    // No UsernamePassword provider in use.
    Future.successful(None)
  }

  override def save(user: BasicProfile, mode: SaveMode): Future[BasicProfile] = {

    // Do not allow creation of new user accounts. Accounts are created
    // manually during the restricted invite period.
    find(user.providerId, user.userId).value match {
      case None => throw new Exception("User Not Found")
      case Some(_) =>
        val basicProfile = BasicProfile(
          user.providerId,
          user.userId,
          user.firstName,
          user.lastName,
          user.fullName,
          user.email,
          user.avatarUrl,
          user.authMethod,
          user.oAuth1Info,
          user.oAuth2Info,
          user.passwordInfo)

        User.save(basicProfile)
        Future.successful(basicProfile)
    }
  }

  override def link(current: BasicProfile, to: BasicProfile): Future[BasicProfile] = {
    // Default implementation as no UsernamePassword provider is in use.
    Future.successful(current)
  }

  override def passwordInfoFor(user: BasicProfile): Future[Option[PasswordInfo]] = {
    // No UsernamePassword provider in use.
    Future.successful(None)
  }

  override def updatePasswordInfo(user: BasicProfile, info: PasswordInfo): Future[Option[BasicProfile]] = {
    // No UsernamePassword provider in use.
    Future.successful(None)
  }

  override def saveToken(token: MailToken): Future[MailToken] = {
    // Defult implementaiton as no UsernamePassword provider in use.
    Future.successful(token)
  }

  override def findToken(token: String): Future[Option[MailToken]] = {
    // No UsernamePassword provider in use.
    Future.successful(None)
  }

  override def deleteToken(uuid: String): Future[Option[MailToken]] = {
    // No UsernamePassword provider in use.
    Future.successful(None)
  }

  override def deleteExpiredTokens() {
    // No UsernamePassword provider in use.
  }
}
