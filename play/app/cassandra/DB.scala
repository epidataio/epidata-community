/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package cassandra

import com.chrisomeara.pillar.Migration
import com.chrisomeara.pillar.Migrator
import com.chrisomeara.pillar.Registry
import com.chrisomeara.pillar.Reporter
import com.chrisomeara.pillar.ReplicationOptions
import com.datastax.driver.core._
import java.io.File
import java.io.PrintStream
import java.util.Date
import org.joda.time.Instant
import play.api.Logger
import play.api.Play
import util.EpidataMetrics

/**
 * Singleton object for managing the server's connection to a Cassandra
 * database and executing queries.
 */
object DB {
  private var connection: Option[Connection] = None

  /**
   * Connect to cassandra. On connect, the keyspace is created and migrated if
   * necessary.
   */
  def connect(nodeNames: String, keyspace: String, username: String, password: String) = {
    connection = Some(new Connection(nodeNames, keyspace, username, password))
  }

  /** Generate a prepared statement. */
  def prepare(statementSpec: String) = connection.get.prepare(statementSpec)

  /** Execute a previously prepared statement. */
  def execute(statement: Statement): ResultSet = {
    val rs = connection.get.execute(statement)
    rs
  }

  /** Execute a previously prepared statement. */
  def batchExecute(statements: List[Statement]): ResultSet = {
    val t0 = EpidataMetrics.getCurrentTime
    val batch = new BatchStatement()
    statements.foreach(s => batch.add(s))
    // execute the batch
    val rs = connection.get.execute(batch)
    EpidataMetrics.increment("DB.batchExecute", t0)
    rs
  }

  /** Execute a CQL statement by binding ordered attributes. */
  def cql(statement: String, args: AnyRef*): ResultSet = {
    connection.get.execute(new SimpleStatement(statement, args: _*))
  }

  /** Execute a CQL statement by binding named attributes. */
  def cql(statement: String, args: Map[String, Any]): ResultSet = {
    val boundStatement = new BoundStatement(connection.get.prepare(statement))
    args.foreach {
      case (key, value: String) => boundStatement.setString(key, value)
      case (key, value: Int) => boundStatement.setInt(key, value)
      case (key, value: Double) => boundStatement.setDouble(key, value)
      case (key, value: Date) => boundStatement.setDate(key, LocalDate.fromMillisSinceEpoch(value.getTime))
      case _ => throw new IllegalArgumentException("Unexpected args.")
    }
    connection.get.execute(boundStatement)
  }

  def close {
    connection.get.close
  }

  def session = connection.get.session
}

private class TerseMigrationReporter(stream: PrintStream) extends Reporter {
  override def initializing(
    session: Session,
    keyspace: String,
    replicationOptions: ReplicationOptions
  ) {
  }

  override def migrating(session: Session, dateRestriction: Option[Date]) {
  }

  override def applying(migration: Migration) {
    stream.println( // scalastyle:ignore
      s"Applying migration ${migration.authoredAt.getTime}: ${migration.description}"
    )
  }

  override def reversing(migration: Migration) {
    stream.println( // scalastyle:ignore
      s"Reversing migration ${migration.authoredAt.getTime}: ${migration.description}"
    )
  }

  override def destroying(session: Session, keyspace: String) {
  }
}

private class Connection(nodeNames: String, keyspace: String, username: String, password: String) {

  val cluster = nodeNames.split(',').foldLeft(Cluster.builder())({ (builder, nodeName) =>
    try {
      builder.addContactPoint(nodeName).withCredentials(username, password)
    } catch {
      case e: IllegalArgumentException => Logger.warn(e.getMessage); builder
    }
  }).build()

  val session = cluster.connect()

  val reporter = new TerseMigrationReporter(System.out)
  val registry = {
    import play.api.Play.current
    Registry.fromDirectory(
      Play.application.getFile("conf/pillar/migrations/epidata"), reporter
    )
  }

  // Create keyspace if necessary.
  Migrator(registry, reporter).initialize(session, keyspace)

  // Use the specified keyspace.
  session.execute(s"USE ${keyspace}")

  // Perform migrations if necessary.
  Migrator(registry, reporter).migrate(session)

  def prepare(statementSpec: String) = session.prepare(statementSpec)

  def execute(statement: Statement) = session.execute(statement)

  def close = {
    session.close()
    cluster.close()
  }
}
