/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import cassandra.DB
import SQLite.{ DB => DBLite }
import service.Configs
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.Row
import securesocial.core.AuthenticationMethod
import securesocial.core.OAuth2Info
import securesocial.core.services
import scala.concurrent.Future
import securesocial.core.{ PasswordInfo, BasicProfile }
import java.sql.ResultSet

object User {

  type U = BasicProfile
  //  type User = SocialUser

  def save(user: U): Unit = {
    if (Configs.userDBLite) {
      DBLite.executeUpdate(
        DBLite.binds(
          DBLite.prepare(sqlInsertStatement),
          user.providerId,
          user.userId,
          user.firstName.isInstanceOf[Option[String]] match {
            case true => user.firstName.get
            case false => user.firstName
          },
          user.lastName.isInstanceOf[Option[String]] match {
            case true => user.lastName.get
            case false => user.lastName
          },
          user.fullName.isInstanceOf[Option[String]] match {
            case true => user.fullName.get
            case false => user.fullName
          },
          user.email.getOrElse(""),
          user.avatarUrl.getOrElse(""),
          user.oAuth2Info.get match {
            case null => null
            case _ => user.oAuth2Info.get.accessToken
          },
          user.oAuth2Info.get match {
            case null => ""
            case _ => user.oAuth2Info.get.tokenType
          },
          user.oAuth2Info.get match {
            case null => -1.asInstanceOf[AnyRef]
            case _ => user.oAuth2Info.get.expiresIn
          },
          user.oAuth2Info.get match {
            case null => ""
            case _ => user.oAuth2Info.get.refreshToken
          }))
    } else {
      DB.execute(
        DB.prepare(cqlInsertStatement).bind(
          user.providerId,
          user.userId,
          user.firstName.getOrElse(""),
          user.lastName.getOrElse(""),
          user.fullName.getOrElse(""),
          user.email.getOrElse(""),
          user.avatarUrl.getOrElse(""),
          user.oAuth2Info.get.accessToken,
          user.oAuth2Info.get.tokenType.getOrElse(""),
          user.oAuth2Info.get.expiresIn.getOrElse(-1).asInstanceOf[AnyRef],
          user.oAuth2Info.get.refreshToken.getOrElse("")))
    }
  }

  def find(providerId: String, userId: String): Option[U] = {
    val query = QueryBuilder.select().all().from("users").where()
      .and(QueryBuilder.eq("userId", userId))
      .and(QueryBuilder.eq("providerId", providerId))
    if (Configs.userDBLite) {
      val rs: ResultSet = DBLite.execute(DBLite.prepare(query.toString()))
      rs.next() match {
        case false => Option(null)
        case true => Option(rowToUser(rs))
      }
    } else {
      Option(DB.execute(query).one).map(rowToUser)
    }
  }

  private lazy val sqlInsertStatement =
    """#INSERT OR REPLACE INTO users (
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
         #oauth2_refresh_token) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#')

  private lazy val cqlInsertStatement =
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
         #oauth2_refresh_token) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#')

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

  private implicit def rowToUser(row: ResultSet): U = {
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
