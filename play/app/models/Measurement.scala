/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import java.util

import cassandra.DB
import com.datastax.driver.core.querybuilder.{ Clause, QueryBuilder }
import com.epidata.lib.models.{ Measurement => Model, MeasurementsKeys, MeasurementSummary }
import com.epidata.lib.models.util.Binary
import java.nio.ByteBuffer
import java.util.Date
import scala.collection.convert.WrapAsScala
import _root_.util.Ordering
import com.datastax.driver.core.{ BoundStatement, ResultSet, PagingState, Statement }

object Measurement {

  import com.epidata.lib.models.Measurement._

  def epochForTs(ts: Date): Int =
    // Divide the timeline into epochs approximately 12 days in duration.
    (ts.getTime() / (1000L * 1000L * 1000L)).toInt

  /**
   * Insert a measurement into the database.
   * @param measurement The Measurement to insert.
   */
  def insert(measurement: Model): Unit = {
    val statements = getInsertStatements(measurement)
    statements.foreach(statement =>
      DB.execute(statement))
  }

  /**
   * Insert a bulk of measurements into the database.
   * @param measurements The Measurement to insert.
   */
  def bulkInsert(measurements: List[Model]): Unit = {
    val statements = measurements.flatMap(measurement => getInsertStatements(measurement))
    DB.batchExecute(statements)
  }

  def getInsertStatements(measurement: Model): List[Statement] = {

    // Insert the measurement itself.
    val measurementInsertStatement = measurement.meas_value match {
      case _: Double | _: Long => insertNumeric(measurement)
      case _: String | _: Binary => insertObject(measurement)
    }

    // Insert the measurement partition key into the partition key store. This
    // write is not batched with the write above, for improved performance. If
    // the below write fails we could miss a key in the key table, but that is
    // expected to be rare because the same partition keys will be written
    // repeatedly during normal ingestion. (The possibility, and risk level,
    // of inconsistency is considered acceptable.) The real world performance
    // impact of this write could be eliminated in the future by caching
    // previously written keys in the app server.

    val statement2 = DB.prepare(insertKeysStatement).bind(
      measurement.customer,
      measurement.customer_site,
      measurement.collection,
      measurement.dataset
    )

    List(measurementInsertStatement, statement2)
  }

  /**
   * Find measurements in the database matching the specified parameters.
   * @param customer
   * @param customer_site
   * @param collection
   * @param dataset
   * @param beginTime Beginning of query time interval, inclusive
   * @param endTime End of query time interval, exclusive
   * @param ordering Timestamp ordering of results, if specified.
   */
  @Deprecated
  def find(
    customer: String,
    customer_site: String,
    collection: String,
    dataset: String,
    beginTime: Date,
    endTime: Date,
    ordering: Ordering.Value = Ordering.Unspecified,
    tableName: String = com.epidata.lib.models.Measurement.DBTableName
  ): List[Model] = {
    import WrapAsScala.iterableAsScalaIterable

    // Find the epochs from which measurements are required, in timestamp
    // sorted order. In practice queries will commonly access only one epoch.
    val orderedEpochs = ordering match {
      case Ordering.Descending => epochForTs(endTime) to epochForTs(beginTime) by -1
      case _ => epochForTs(beginTime) to epochForTs(endTime)
    }

    // Define the database query to execute for a single epoch.
    def queryForEpoch(epoch: Int) = {
      val query = QueryBuilder.select().all().from(tableName).where()
        .and(QueryBuilder.eq("customer", customer))
        .and(QueryBuilder.eq("customer_site", customer_site))
        .and(QueryBuilder.eq("collection", collection))
        .and(QueryBuilder.eq("dataset", dataset))
        .and(QueryBuilder.eq("epoch", epoch))
        .and(QueryBuilder.gte("ts", beginTime))
        .and(QueryBuilder.lt("ts", endTime))
      // Apply an orderBy parameter if ordering is required.
      ordering match {
        case Ordering.Ascending => query.orderBy(QueryBuilder.asc("ts"))
        case Ordering.Descending => query.orderBy(QueryBuilder.desc("ts"))
        case _ =>
      }
      query
    }

    // Execute the queries, concatenating results across epochs.
    orderedEpochs
      .map(queryForEpoch)
      .flatMap(DB.execute)
      .map(rowToMeasurement)
      .toList
  }

  def query(
    customer: String,
    customer_site: String,
    collection: String,
    dataset: String,
    beginTime: Date,
    endTime: Date,
    ordering: Ordering.Value = Ordering.Unspecified,
    tableName: String = com.epidata.lib.models.Measurement.DBTableName,
    size: Int = 10000,
    batch: String = ""
  ): ResultSet = {

    // Define the database query to execute for a single epoch.
    def queryForEpoch = {

      // Find the epochs from which measurements are required, in timestamp
      // sorted order. In practice queries will commonly access only one epoch.
      val orderedEpochs = ordering match {
        case Ordering.Descending => epochForTs(endTime) to epochForTs(beginTime) by -1
        case _ => epochForTs(beginTime) to epochForTs(endTime)
      }

      val epochs = new util.ArrayList[Integer]()
      orderedEpochs.toList.foreach(e => epochs.add(e))

      val query = QueryBuilder.select().all().from(tableName).where()
        .and(QueryBuilder.eq("customer", customer))
        .and(QueryBuilder.eq("customer_site", customer_site))
        .and(QueryBuilder.eq("collection", collection))
        .and(QueryBuilder.eq("dataset", dataset))
        .and(QueryBuilder.in("epoch", epochs))
        .and(QueryBuilder.gte("ts", beginTime))
        .and(QueryBuilder.lt("ts", endTime))

      // Apply an orderBy parameter if ordering is required.
      ordering match {
        case Ordering.Ascending => query.orderBy(QueryBuilder.asc("ts"))
        case Ordering.Descending => query.orderBy(QueryBuilder.desc("ts"))
        case _ =>
      }

      if (batch != null && !batch.isEmpty) {
        val pagingState = PagingState.fromString(batch);
        query.setPagingState(pagingState)
      }

      query.setFetchSize(size)

      query
    }

    def queryForMeasurementSummary = {
      val query = QueryBuilder.select().all().from(tableName).where()
        .and(QueryBuilder.eq("customer", customer))
        .and(QueryBuilder.eq("customer_site", customer_site))
        .and(QueryBuilder.eq("collection", collection))
        .and(QueryBuilder.eq("dataset", dataset))
        .and(QueryBuilder.gte("start_time", beginTime))
        .and(QueryBuilder.lt("start_time", endTime))
      // Apply an orderBy parameter if ordering is required.
      ordering match {
        case Ordering.Ascending => query.orderBy(QueryBuilder.asc("start_time"))
        case Ordering.Descending => query.orderBy(QueryBuilder.desc("start_time"))
        case _ =>
      }

      if (batch != null && !batch.isEmpty) {
        val pagingState = PagingState.fromString(batch);
        query.setPagingState(pagingState)
      }

      query.setFetchSize(size)

      query
    }

    // Execute the query
    tableName match {
      case MeasurementSummary.DBTableName => DB.execute(queryForMeasurementSummary)
      case _ => DB.execute(queryForEpoch)
    }

  }

  private def insertNumeric(measurement: Model): Statement = {

    val insertStatementsStr = measurement.meas_value match {
      case _: Double => insertDoubleStatements
      case _: Long => insertLongStatements
    }

    val insertStatements = insertStatementsStr.map(DB.prepare(_))

    (measurement.meas_lower_limit, measurement.meas_upper_limit) match {
      case (None, None) =>
        // Insert with neither a lower nor upper limit.
        insertStatements(0).bind(
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_value.asInstanceOf[AnyRef],
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse("")
        )
      case (_, None) =>
        // Insert with a lower limit only.
        insertStatements(1).bind(
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_value.asInstanceOf[AnyRef],
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_lower_limit.get.asInstanceOf[AnyRef],
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse("")
        )
      case (None, _) =>
        // Insert with an upper limit only.
        insertStatements(2).bind(
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_value.asInstanceOf[AnyRef],
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_upper_limit.get.asInstanceOf[AnyRef],
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse("")
        )
      case _ =>
        // Insert with both a lower and an upper limit.
        insertStatements(3).bind(
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_value.asInstanceOf[AnyRef],
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_lower_limit.get.asInstanceOf[AnyRef],
          measurement.meas_upper_limit.get.asInstanceOf[AnyRef],
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse("")
        )
    }
  }

  private def insertObject(measurement: Model): Statement = {
    val insertStatementStr = measurement.meas_value match {
      case _: String => insertStringStatement
      case _: Binary => insertBlobStatement
    }

    val insertStatement = DB.prepare(insertStatementStr)

    insertStatement.bind(
      measurement.customer,
      measurement.customer_site,
      measurement.collection,
      measurement.dataset,
      measurement.epoch: java.lang.Integer,
      measurement.ts,
      measurement.key1.getOrElse(""),
      measurement.key2.getOrElse(""),
      measurement.key3.getOrElse(""),
      measurement.meas_value match {
        case value: Binary => ByteBuffer.wrap(value.backing)
        case value => value.asInstanceOf[AnyRef]
      },
      measurement.meas_unit.getOrElse(""),
      measurement.meas_status.getOrElse(""),
      measurement.meas_description.getOrElse(""),
      measurement.val1.getOrElse(""),
      measurement.val2.getOrElse("")
    )
  }

  // Prepared statements for inserting different types of measurements.
  private lazy val insertDoubleStatements = prepareNumericInserts("")
  private lazy val insertLongStatements = prepareNumericInserts("_l")
  private lazy val insertStringStatement = prepareObjectInsert("_s")
  private lazy val insertBlobStatement = prepareObjectInsert("_b")
  private lazy val insertKeysStatement = prepareKeysInsert

  private def prepareNumericInserts(typeSuffix: String) =
    List(

      s"""#INSERT INTO ${Model.DBTableName} (
            #customer,
            #customer_site,
            #collection,
            #dataset,
            #epoch,
            #ts,
            #key1,
            #key2,
            #key3,
            #meas_value${typeSuffix},
            #meas_unit,
            #meas_status,
            #meas_description,
            #val1,
            #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#'),

      s"""#INSERT INTO ${Model.DBTableName} (
            #customer,
            #customer_site,
            #collection,
            #dataset,
            #epoch,
            #ts,
            #key1,
            #key2,
            #key3,
            #meas_value${typeSuffix},
            #meas_unit,
            #meas_status,
            #meas_lower_limit${typeSuffix},
            #meas_description,
            #val1,
            #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#'),

      s"""#INSERT INTO ${Model.DBTableName} (
            #customer,
            #customer_site,
            #collection,
            #dataset,
            #epoch,
            #ts,
            #key1,
            #key2,
            #key3,
            #meas_value${typeSuffix},
            #meas_unit,
            #meas_status,
            #meas_upper_limit${typeSuffix},
            #meas_description,
            #val1,
            #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#'),
      s"""#INSERT INTO ${Model.DBTableName} (
            #customer,
            #customer_site,
            #collection,
            #dataset,
            #epoch,
            #ts,
            #key1,
            #key2,
            #key3,
            #meas_value${typeSuffix},
            #meas_unit,
            #meas_status,
            #meas_lower_limit${typeSuffix},
            #meas_upper_limit${typeSuffix},
            #meas_description,
            #val1,
            #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#')
    )

  private def prepareObjectInsert(typeSuffix: String) =
    s"""#INSERT INTO ${Model.DBTableName} (
          #customer,
          #customer_site,
          #collection,
          #dataset,
          #epoch,
          #ts,
          #key1,
          #key2,
          #key3,
          #meas_value${typeSuffix},
          #meas_unit,
          #meas_status,
          #meas_description,
          #val1,
          #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#')

  private def prepareKeysInsert =
    s"""#INSERT INTO ${MeasurementsKeys.DBTableName} (
         #customer,
         #customer_site,
         #collection,
         #dataset) VALUES (?, ?, ?, ?)""".stripMargin('#')

}
