/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

// Based on sample code from securesocial github repository

package service

import providers.DemoProvider
import controllers.Users
import controllers.AssetsFinder
import javax.inject.{ Inject, Singleton }
import akka.actor.ActorSystem
import play.api.cache._
import play.api.{ Configuration, Environment }
import play.api.i18n.{ MessagesApi, MessagesProvider, Lang }
import play.api.libs.mailer.MailerClient
import play.api.libs.ws.WSClient
import play.api.mvc.PlayBodyParsers
import securesocial.core._
import securesocial.core.services._
import securesocial.core.{ IdentityProvider, RuntimeEnvironment, OAuth2Client }
import securesocial.controllers._

import scala.collection.immutable.ListMap
import scala.concurrent.ExecutionContext

@Singleton
class AppEnvironment @Inject() (
  override val configuration: Configuration,
  override val messagesApi: MessagesApi,
  override val environment: Environment,
  override val wsClient: WSClient,
  override val cacheApi: AsyncCacheApi,
  override val mailerClient: MailerClient,
  override val parsers: PlayBodyParsers,
  override val actorSystem: ActorSystem,
  assets: AssetsFinder) extends RuntimeEnvironment.Default {
  override type U = BasicProfile
  override val userService: UserService[U] = new CassandraUserService()
  override implicit val executionContext: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global

  val lang: String = (configuration.get[String]("langs"))
  override lazy val viewTemplates: ViewTemplates = new Users(this)(configuration, Lang(lang), assets, messagesApi)

  override lazy val providers: ListMap[String, IdentityProvider] =
    ListMap(DemoProvider.Demo -> new DemoProvider(routes, new OAuth2Client.Default(httpService, demoOAuth2Settings), cacheService))

  val demoOAuth2Settings = OAuth2Settings(
    configuration.get[Configuration]("securesocial.demo")
      .get[String]("authorizationUrl"),
    configuration.get[Configuration]("securesocial.demo")
      .get[String]("accessTokenUrl"),
    configuration.get[Configuration]("securesocial.demo")
      .get[String]("clientId"),
    configuration.get[Configuration]("securesocial.demo")
      .get[String]("clientSecret"),
    None, Map.empty, Map.empty)
}
