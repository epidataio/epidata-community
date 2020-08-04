/*
 * Copyright (c) 2015-2017 EpiData, Inc.
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

object DBL {
  private var connection: Option[ConnectionLite] = None

  /**
   * Connect to SQlite.
   */
  @Inject()
  def connect(url: String) = {
    connection = Some(new ConnectionLite(url))
  }

  /** Generate a prepared statement. */
  def prepare(statementSpec: String) = connection.get.prepare(statementSpec)

  /** Execute a previously prepared statement. */
  def execute(statement: Statement): ResultSet = connection.get.execute(statement)

  /** Execute a previously prepared statement which returns and empty ResultSet */
  def executeUpdate(statement: Statement): ResultSet = connection.get.executeUpdate(statement)

  /** ResultSet to Json Object. */
  //  def resultToJson(rs : ResultSet) = {
  //    // Get the next page info
  //    val nextPage = rs.getExecutionInfo().getPagingState()
  //    val nextBatch = if (nextPage == null) "" else nextPage.toString
  //
  //    // only return the available ones by not fetching.
  //
  //    val rows = 1.to(rs.getFetchSize()).map(_ => rs.one())
  //    val records = new JLinkedList[JLinkedHashMap[String, Object]]()
  //
  //    rows
  //      .map(Model.rowToJLinkedHashMap(_, tableName, modelName))
  //      .foreach(m => records.add(m))
  //
  //    // Return the json object
  //    JsonHelpers.toJson(records, nextBatch)
  //  }

  /** ResultSet to User. */

  /** Executes a batch of statements individually but reverts back to original state if error */
  def batchExecute(statements: List[PreparedStatement]): ResultSet = connection.get.batchExecute(statements)

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

private class ConnectionLite(url: String) {

  val session = DriverManager.getConnection(url)
  val cleansed = "play/app/conf/schemas/measurements_cleansed.txt"
  val keys = "play/app/conf/schemas/measurements_keys.txt"
  val original = "play/app/conf/schemas/measurements_original.txt"
  val summary = "play/app/conf/schemas/measurements_summary.txt"
  val users = "play/app/conf/schemas/users.txt"
  val sql1 = Source.fromFile(cleansed).getLines.mkString
  val sql2 = Source.fromFile(keys).getLines.mkString
  val sql3 = Source.fromFile(original).getLines.mkString
  val sql4 = Source.fromFile(summary).getLines.mkString
  val sql5 = Source.fromFile(users).getLines.mkString
  session.createStatement().executeUpdate(sql1)
  session.createStatement().executeUpdate(sql2)
  session.createStatement().executeUpdate(sql3)
  session.createStatement().executeUpdate(sql4)
  session.createStatement().executeUpdate(sql5)

  def prepare(statement: String): PreparedStatement = session.prepareStatement(statement)

  def execute(statement: Statement) = statement.asInstanceOf[PreparedStatement].executeQuery()

  def executeUpdate(statement: Statement): ResultSet = {
    statement.asInstanceOf[PreparedStatement].executeUpdate()
    val l: ResultSet = null
    l
  }

  def batchExecute(statements: List[PreparedStatement]): ResultSet = {
    val t0 = EpidataMetrics.getCurrentTime
    // execute the batch
    statements.foreach(s => s.executeUpdate())

    EpidataMetrics.increment("DB.batchExecute", t0)
    val l: ResultSet = null
    l
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

