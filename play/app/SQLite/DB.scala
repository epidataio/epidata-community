/*
* Copyright (c) 2015-2020 EpiData, Inc.
*/

package SQLite

import java.io.File
import java.io.PrintStream
import java.util.Date
import org.joda.time.Instant
import play.api.Logger
import play.api.Application
import play.api.{ Configuration, Environment }
import util.EpidataMetrics
import javax.inject._
import scala.io.Source
import java.sql.{ Connection, CallableStatement, DriverManager, PreparedStatement, ResultSet, SQLException, Statement }

/**
 * Singleton object for managing the server's connection to a Cassandra
 * database and executing queries.
 */

object DB {
  private var connection: Option[ConnectionLite] = None

  /**
   * Connect to SQlite.
   */
  @Inject()
  def connect(url: String, schemaPath: java.io.File) = {
    println("url: " + url + " ")
    connection = Some(new ConnectionLite(url, schemaPath))
    println("connection: " + connection + " ")
  }

  /** Generate a prepared statement. */
  def prepare(statementSpec: String) = connection.get.prepare(statementSpec)

  /** Execute a previously prepared statement which returns a resultset (Get Statements) */
  def execute(statement: Statement): ResultSet = connection.get.execute(statement)

  /** Execute a previously prepared statement which does not return anything (Insert Statements) */
  def executeUpdate(statement: PreparedStatement) = connection.get.executeUpdate(statement)

  /** Binds the values in args to the statement. */
  def binds(statement: PreparedStatement, args: Any*): PreparedStatement = connection.get.binds(statement, args)

  /** Execute a SQL statement by binding ordered attributes. */
  def cql(statement: String, args: Any*): ResultSet = connection.get.cql(statement, args)

  /** Execute a CQL statement by binding named attributes. */
  def cql(statement: String, args: Map[String, Any]): ResultSet = connection.get.cql(statement, args)

  /** Closes a Cassandra connection. */
  def close = connection.get.close

  /** Returns the ongoing session. */
  def session = connection.get.session

}

private class ConnectionLite(url: String, schemaPath: java.io.File) {

  var base: File = new File(".")
  println("base path: " + base)
  val PATH = getClass.getResource("").getPath
  println("Path: " + PATH + " ")

  println("url: " + url)
  Class.forName("org.sqlite.JDBC");
  val session = DriverManager.getConnection(url)
  println("session: " + session)

  //  val original = "play/conf/schema/measurements_original"
  val original = schemaPath + "/measurements_original"
  //  val cleansed = "play/conf/schema//measurements_cleansed"
  val cleansed = schemaPath + "/measurements_cleansed"
  //  val summary = "play/conf/schema/measurements_summary"
  val summary = schemaPath + "/measurements_summary"
  //  val keys = "play/conf/schema/measurements_keys"
  val keys = schemaPath + "/measurements_keys"
  //  val users = "play/conf/schema/users"
  val users = schemaPath + "/users"

  val sql1 = Source.fromFile(original).getLines.mkString
  val sql2 = Source.fromFile(cleansed).getLines.mkString
  val sql3 = Source.fromFile(summary).getLines.mkString
  val sql4 = Source.fromFile(keys).getLines.mkString
  val sql5 = Source.fromFile(users).getLines.mkString
  session.createStatement().executeUpdate(sql1)
  session.createStatement().executeUpdate(sql2)
  session.createStatement().executeUpdate(sql3)
  session.createStatement().executeUpdate(sql4)
  session.createStatement().executeUpdate(sql5)

  def prepare(statement: String): PreparedStatement = session.prepareStatement(statement)

  def execute(statement: Statement) = statement.asInstanceOf[PreparedStatement].executeQuery()

  def executeUpdate(statement: PreparedStatement) = {
    statement.executeUpdate()
  }

  def binds(statement: PreparedStatement, args: Seq[Any]): PreparedStatement = {
    var i = 1
    args.foreach { e =>
      statement.setObject(i, e)
      i += 1
    }
    statement
  }

  def cql(statement: String, args: Seq[Any]): ResultSet = {
    val boundStatement = prepare(statement)
    var i = 1
    for (e <- args) {
      boundStatement.setObject(i, e)
      i += 1
    }
    boundStatement.executeQuery()
  }

  def cql(statement: String, args: Map[String, Any]): ResultSet = {
    val boundStatement = session.prepareCall(statement)
    args.foreach {
      case (key, value: String) => boundStatement.setString(key, value)
      case (key, value: Int) => boundStatement.setInt(key, value)
      case (key, value: Double) => boundStatement.setDouble(key, value)
      case _ => throw new IllegalArgumentException("Unexpected args.")
    }
    boundStatement.executeQuery()
  }

  def close = {
    session.close()
  }

}
