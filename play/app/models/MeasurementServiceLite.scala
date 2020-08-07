/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import java.util
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import java.nio.ByteBuffer
import SQLite.DBL
import service.Configs
import com.epidata.lib.models.{ Measurement => Model, MeasurementsKeys, MeasurementSummary }
import com.epidata.lib.models.util.{ JsonHelpers, Binary }
import _root_.util.{ EpidataMetrics, Ordering }
import com.datastax.driver.core.querybuilder.{ Clause, QueryBuilder }

import java.sql._

import scala.collection.convert.WrapAsScala
import scala.collection.JavaConverters._

object MeasurementServiceLite {

  import com.epidata.lib.models.Measurement._

  private var prepareStatementMap: Map[String, PreparedStatement] = Map.empty

  def getPrepareStatement(statementSpec: String): PreparedStatement = {
    if (!prepareStatementMap.contains(statementSpec)) {
      val stm = DBL.prepare(statementSpec)
      prepareStatementMap = prepareStatementMap + (statementSpec -> stm)
      stm
    } else {
      prepareStatementMap.get(statementSpec).get
    }
  }

  def reset = {
    prepareStatementMap = Map.empty
  }

  def epochForTs(ts: Date): Int =
    // Divide the timeline into epochs approximately 12 days in duration.
    (ts.getTime() / (1000L * 1000L * 1000L)).toInt

  /**
   * Insert a measurement into the database.
   * @param measurement The Measurement to insert.
   */
  def insert(measurement: Model): Unit = {
    // DBL.session.setAutoCommit(false)
    // val statements = getInsertStatements(measurement)
    // statements.foreach(statement =>
    //   DBL.executeUpdate(statement))
    // DBL.session.setAutoCommit(true)
    DBL.session.setAutoCommit(false)
    if (Configs.ingestionKeyCreation) {
      val statementPartition = getPartitionKeyStatements(measurement)
      DBL.executeUpdate(statementPartition)
    }
    val statementInsert = getInsertStatements(measurement)
    DBL.executeUpdate(statementInsert)
    DBL.session.setAutoCommit(true)

  }

  /**
   * Insert a bulk of measurements into the database.
   * @param measurements The Measurement to insert.
   */
  def bulkInsert(measurements: List[Model]): Unit = {

    // val statements = measurements.flatMap(measurement => getInsertStatements(measurement))
    // DB.batchExecute(statements)
    // measurements.flatMap(measurement => {
    //   val x = getInsertStatements(measurement)
    //   DBL.executeUpdate(x[0])
    // })

    DBL.session.setAutoCommit(false)
    val t0 = EpidataMetrics.getCurrentTime
    measurements.foreach(f => {
      if (Configs.ingestionKeyCreation) {
        DBL.executeUpdate(getPartitionKeyStatements(f))
      }
      DBL.executeUpdate(getInsertStatements(f))
    })
    EpidataMetrics.increment("DB.batchExecute", t0)
    DBL.session.setAutoCommit(true)

  }

  def getInsertStatements(measurement: Model): PreparedStatement = {

    // Insert the measurement itself.
    val measurementInsertStatement = measurement.meas_value match {
      case _: Double | _: Long => getMeasurementInsertStatement(measurement)
      case _: String | _: Binary => getMeasurementInsertStatement(measurement)
      case _ => getMeasurementInsertStatementForNullMeasValue(measurement)
    }

    // if (Configs.ingestionKeyCreation) {

    //   // Insert the measurement partition key into the partition key store. This
    //   // write is not batched with the write above, for improved performance. If Add a comment to this line
    //   // the below write fails we could miss a key in the key table, but that is
    //   // expected to be rare because the same partition keys will be written
    //   // repeatedly during normal ingestion. (The possibility, and risk level,
    //   // of inconsistency is considered acceptable.) The real world performance
    //   // impact of this write could be eliminated in the future by caching
    //   // previously written keys in the app server.
    //   val statement2 = DBL.binds(
    //     getPrepareStatement(insertKeysStatement),
    //     measurement.customer,
    //     measurement.customer_site,
    //     measurement.collection,
    //     measurement.dataset)
    //   List(measurementInsertStatement, statement2)
    // } else {
    //   List(measurementInsertStatement)
    // }
    measurementInsertStatement
  }

  def getPartitionKeyStatements(measurement: Model): PreparedStatement = {
    val statement2 = DBL.binds(
      getPrepareStatement(insertKeysStatement),
      measurement.customer,
      measurement.customer_site,
      measurement.collection,
      measurement.dataset)
    statement2
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
  //   @Deprecated
  //   def find(
  //     customer: String,
  //     customer_site: String,
  //     collection: String,
  //     dataset: String,
  //     beginTime: Date,
  //     endTime: Date,
  //     ordering: Ordering.Value = Ordering.Unspecified,
  //     tableName: String = com.epidata.lib.models.Measurement.DBTableName): List[Model] = {
  //     import WrapAsScala.iterableAsScalaIterable

  //     // Find the epochs from which measurements are required, in timestamp
  //     // sorted order. In practice queries will commonly access only one epoch.
  //     val orderedEpochs = ordering match {
  //       case Ordering.Descending => epochForTs(endTime) to epochForTs(beginTime) by -1
  //       case _ => epochForTs(beginTime) to epochForTs(endTime)
  //     }

  //     // Define the database query to execute for a single epoch.
  //     def queryForEpoch(epoch: Int) = {
  //       val query = QueryBuilder.select().all().from(tableName).where()
  //         .and(QueryBuilder.eq("customer", customer))
  //         .and(QueryBuilder.eq("customer_site", customer_site))
  //         .and(QueryBuilder.eq("collection", collection))
  //         .and(QueryBuilder.eq("dataset", dataset))
  //         .and(QueryBuilder.eq("epoch", epoch))
  //         .and(QueryBuilder.gte("ts", beginTime))
  //         .and(QueryBuilder.lt("ts", endTime))
  //       // Apply an orderBy parameter if ordering is required.
  //       ordering match {
  //         case Ordering.Ascending => query.orderBy(QueryBuilder.asc("ts"))
  //         case Ordering.Descending => query.orderBy(QueryBuilder.desc("ts"))
  //         case _ =>
  //       }
  //       query
  //     }

  //     // Execute the queries, concatenating results across epochs.
  //     orderedEpochs
  //       .map(queryForEpoch)
  //       .flatMap(DBL.prepare)
  //       .flatMap(DBL.execute)
  //       .map(rowToMeasurement)
  //       .toList
  //   }

  def query(
    company: String,
    site: String,
    station: String,
    sensor: String,
    beginTime: Date,
    endTime: Date,
    size: Int,
    batch: String,
    ordering: Ordering.Value,
    tableName: String,
    modelName: String): String = {

    // Get the data from Cassandra
    val rs: ResultSet = MeasurementServiceLite.query(company, site, station, sensor, beginTime, endTime, ordering, tableName, size, batch)

    // Get the next page info NOT APPLICABALE IN SQLITE
    // val nextPage = rs.getExecutionInfo().getPagingState()
    // val nextBatch = if (nextPage == null) "" else nextPage.toString
    val records = new JLinkedList[JLinkedHashMap[String, Object]]()
    while (rs.next() != false) {
      records.add(Model.rowToJLinkedHashMap(rs, tableName, modelName))
    }
    val nextBatch = ""

    // only return the available ones by not fetching.
    // val rows = 1.to(rs.getAvailableWithoutFetching()).map(_ => rs.one())
    // val records = new JLinkedList[JLinkedHashMap[String, Object]]()

    // rows
    //   .map(Model.rowToJLinkedHashMap(_, tableName, modelName))
    //   .foreach(m => records.add(m))

    // Return the json object
    JsonHelpers.toJson(records, nextBatch)
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
    batch: String = ""): ResultSet = {

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

      query.setFetchSize(size)

      query.toString()
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

      query.setFetchSize(size)

      query.toString()
    }

    // Execute the query
    tableName match {
      case MeasurementSummary.DBTableName => DBL.prepare(queryForMeasurementSummary).executeQuery()
      case _ => DBL.prepare(queryForEpoch).executeQuery()
    }

  }

  private def getMeasurementInsertStatement(measurement: Model): PreparedStatement = {

    val insertStatementsStr = measurement.meas_value match {
      case _: Double => insertDoubleStatements
      case _: Long => insertLongStatements
      case _: String => insertStringStatement
      case _: Binary => insertBlobStatement
    }

    val meas_value = measurement.meas_value match {
      case value: Binary => ByteBuffer.wrap(value.backing)
      case _ => measurement.meas_value.asInstanceOf[AnyRef]
    }

    val insertStatements = insertStatementsStr.map(getPrepareStatement(_))

    (measurement.meas_lower_limit, measurement.meas_upper_limit) match {
      case (None, None) =>
        // Insert with neither a lower nor upper limit.
        DBL.binds(
          insertStatements(0),
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_datatype.getOrElse(""),
          meas_value,
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse(""))
      case (_, None) =>
        // Insert with a lower limit only.
        DBL.binds(
          insertStatements(1),
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_datatype.getOrElse(""),
          meas_value,
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_lower_limit.get.asInstanceOf[AnyRef],
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse(""))
      case (None, _) =>
        // Insert with an upper limit only.
        DBL.binds(
          insertStatements(2),
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_datatype.getOrElse(""),
          meas_value,
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_upper_limit.get.asInstanceOf[AnyRef],
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse(""))
      case _ =>
        // Insert with both a lower and an upper limit.
        DBL.binds(
          insertStatements(3),
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_datatype.getOrElse(""),
          meas_value,
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_lower_limit.get.asInstanceOf[AnyRef],
          measurement.meas_upper_limit.get.asInstanceOf[AnyRef],
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse(""))
    }
  }

  private def getMeasurementInsertStatementForNullMeasValue(measurement: Model): PreparedStatement = {
    val insertStatementsStr = insertNullDoubleValueStatement

    val insertStatements = insertStatementsStr.map(getPrepareStatement(_))

    (measurement.meas_lower_limit, measurement.meas_upper_limit) match {
      case (None, None) =>
        // Insert with neither a lower nor upper limit.
        DBL.binds(
          insertStatements(0),
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_datatype.getOrElse(""),
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse(""))
      case (_, None) =>
        // Insert with a lower limit only.
        DBL.binds(
          insertStatements(1),
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_datatype.getOrElse(""),
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_lower_limit.get.asInstanceOf[AnyRef],
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse(""))
      case (None, _) =>
        // Insert with an upper limit only.
        DBL.binds(
          insertStatements(2),
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_datatype.getOrElse(""),
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_upper_limit.get.asInstanceOf[AnyRef],
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse(""))
      case _ =>
        // Insert with both a lower and an upper limit.
        DBL.binds(
          insertStatements(3),
          measurement.customer,
          measurement.customer_site,
          measurement.collection,
          measurement.dataset,
          measurement.epoch: java.lang.Integer,
          measurement.ts,
          measurement.key1.getOrElse(""),
          measurement.key2.getOrElse(""),
          measurement.key3.getOrElse(""),
          measurement.meas_datatype.getOrElse(""),
          measurement.meas_unit.getOrElse(""),
          measurement.meas_status.getOrElse(""),
          measurement.meas_lower_limit.get.asInstanceOf[AnyRef],
          measurement.meas_upper_limit.get.asInstanceOf[AnyRef],
          measurement.meas_description.getOrElse(""),
          measurement.val1.getOrElse(""),
          measurement.val2.getOrElse(""))
    }
  }

  // Prepared statements for inserting different types of measurements.
  private lazy val insertDoubleStatements = prepareInserts("", "")
  private lazy val insertLongStatements = prepareInserts("_l", "_l")
  private lazy val insertStringStatement = prepareInserts("_s", "")
  private lazy val insertBlobStatement = prepareInserts("_b", "")
  private lazy val insertKeysStatement = prepareKeysInsert
  private lazy val insertNullDoubleValueStatement = prepareNullValueInserts("")
  private lazy val insertNullLongValueStatement = prepareNullValueInserts("_l")

  private def prepareInserts(typeSuffix: String, limitTypeSuffix: String) =
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
            #meas_datatype,
            #meas_value${typeSuffix},
            #meas_unit,
            #meas_status,
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
            #meas_datatype,
            #meas_value${typeSuffix},
            #meas_unit,
            #meas_status,
            #meas_lower_limit${limitTypeSuffix},
            #meas_description,
            #val1,
            #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#'),

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
            #meas_datatype,
            #meas_value${typeSuffix},
            #meas_unit,
            #meas_status,
            #meas_upper_limit${limitTypeSuffix},
            #meas_description,
            #val1,
            #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#'),
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
            #meas_datatype,
            #meas_value${typeSuffix},
            #meas_unit,
            #meas_status,
            #meas_lower_limit${limitTypeSuffix},
            #meas_upper_limit${limitTypeSuffix},
            #meas_description,
            #val1,
            #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#'))

  private def prepareNullValueInserts(typeSuffix: String) =
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
         #meas_datatype,
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
         #meas_datatype,
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
         #meas_datatype,
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
         #meas_datatype,
         #meas_unit,
         #meas_status,
         #meas_lower_limit${typeSuffix},
         #meas_upper_limit${typeSuffix},
         #meas_description,
         #val1,
         #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#'))

  private def prepareKeysInsert =
    s"""#INSERT OR IGNORE INTO ${MeasurementsKeys.DBTableName} (
         #customer,
         #customer_site,
         #collection,
         #dataset) VALUES (?, ?, ?, ?)""".stripMargin('#')

}
