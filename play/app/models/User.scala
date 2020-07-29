/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import cassandra.DB
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.Row
import securesocial.core.AuthenticationMethod
import securesocial.core.OAuth2Info
import securesocial.core.services
import scala.concurrent.Future
import securesocial.core.{ PasswordInfo, BasicProfile }

object User {

  type U = BasicProfile
  //  type User = SocialUser

  def save(user: U): Unit = {
    DB.execute(insertStatement.bind(
      user.providerId,
      user.userId,
      user.firstName,
      user.lastName,
      user.fullName,
      user.email.getOrElse(""),
      user.avatarUrl.getOrElse(""),
      user.oAuth2Info.get.accessToken,
      user.oAuth2Info.get.tokenType.getOrElse(""),
      user.oAuth2Info.get.expiresIn.getOrElse(-1).asInstanceOf[AnyRef],
      user.oAuth2Info.get.refreshToken.getOrElse("")))
  }

  def find(providerId: String, userId: String): Option[U] = {
    val query = QueryBuilder.select()
      .all()
      .from("users")
      .where(QueryBuilder.eq(providerId, userId))
    Option(DB.execute(query).one).map(rowToUser)
  }

  private lazy val insertStatement =
    DB.prepare(
      """#INSERT INTO users (
         #providerId,
         #userId,
         #first_name,
         #last_name,
         #full_name,
         #email,
         #avatar_url,
         #oauth2_token,
         #oauth2_token_type,
         #oauth2_expires_in,
         #oauth2_refresh_token) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#'))

  private implicit def rowToUser(row: Row): U = {
    def blankToNone(string: String): Option[String] = string match {
      case "" => None
      case string => Some(string)
    }

    def negativeToNone(num: Int): Option[Int] = num match {
      case num if num < 0 => None
      case num => Some(num)
    }

    val user = BasicProfile(
      //      IdentityId(
      //        row.getString("id"),
      //        "github"),
      row.getString("providerID"),
      row.getString("userId"),
      Some(row.getString("first_name")),
      Some(row.getString("last_name")),
      Some(row.getString("full_name")),
      blankToNone(row.getString("email")),
      blankToNone(row.getString("avatar_url")),
      AuthenticationMethod.OAuth2,
      None,
      Some(OAuth2Info(
        row.getString("oauth2_token"),
        blankToNone(row.getString("oauth2_token_type")),
        negativeToNone(row.getInt("oauth2_expires_in")),
        blankToNone(row.getString("oauth2_refresh_token")))))

    user
  }
}
