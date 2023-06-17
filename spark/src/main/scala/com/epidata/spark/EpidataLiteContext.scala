/*
 * Copyright (c) 2015-2023 EpiData, Inc.
*/

package com.epidata.spark

import java.sql.{ Connection, DriverManager, ResultSet, SQLException, Statement, Timestamp }
import com.epidata.lib.models.{ Measurement => BaseMeasurement, MeasurementCleansed => BaseMeasurementCleansed, MeasurementSummary => BaseMeasurementSummary, MeasurementsKeys => BaseMeasurementsKeys }
import com.epidata.lib.models.{ AutomatedTest => BaseAutomatedTest, AutomatedTestCleansed => BaseAutomatedTestCleansed, AutomatedTestSummary => BaseAutomatedTestSummary }
import com.epidata.lib.models.{ SensorMeasurement => BaseSensorMeasurement, SensorMeasurementCleansed => BaseSensorMeasurementCleansed, SensorMeasurementSummary => BaseSensorMeasurementSummary }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import com.typesafe.config.{ Config, ConfigFactory }
import scala.util.Properties
import scala.collection.JavaConversions._
import scala.collection.JavaConversions
import java.io.File
import java.nio.file.Paths
import java.util.logging._

/**
 * The context of EpiData Lite.
 */
class EpidataLiteContext(epidataConf: EpiDataConf = EpiDataConf("", "")) {

  // Auxiliary constructor for Java and Python
  def this() = {
    this(EpiDataConf("", ""))
    logger.log(Level.FINE, "EpiDataLiteContext object created.")
  }

  val logger = Logger.getLogger("EpiDataLiteContext Logger")
  logger.setLevel(Level.FINE)

  private val basePath = scala.util.Properties.envOrElse("EPIDATA_HOME", "")
  if (basePath.equals("") || basePath == null) {
    throw new IllegalStateException("EPIDATA_HOME environment variable not set")
  }

  private val conf = ConfigFactory.parseFile(new java.io.File(Paths.get(basePath, "conf", "sqlite-defaults.conf").toString())).resolve()
  // println("conf value: " + conf)

  private val logFilePath = Paths.get(basePath, "log", conf.getString("spark.epidata.logFileName")).toString()

  // println("log file path: " + logFilePath)

  val fileHandler = new FileHandler(logFilePath)
  logger.addHandler(fileHandler)

  private lazy val sqliteDBName = conf.getString("spark.epidata.SQLite.dbFileName")

  private val measurementClass = epidataConf.model match {
    case m if m.trim.isEmpty => scala.util.Properties.envOrElse("EPIDATA_MEASUREMENT_MODEL", conf.getString("spark.epidata.measurementClass"))
    case m: String => m
  }
  logger.log(Level.INFO, "Measurement class: " + measurementClass)

  private lazy val streamingBatchDuration = conf.getInt("spark.epidata.streamingBatchDuration")

  private val sqliteDBUrl = epidataConf.dbUrl match {
    case s if s.trim.isEmpty => {
      conf.getString("spark.epidata.SQLite.url") match {
        case s if s.trim.isEmpty => {
          Paths.get("jdbc:sqlite:", basePath, "data", sqliteDBName).toString()
        }
        case s => {
          Paths.get(conf.getString("spark.epidata.SQLite.url"), sqliteDBName).toString()
        }
      }
    }
    case s => s
  }

  // println("SQLite DB url: " + sqliteDBUrl)

  // Connect to SQLite database
  Class.forName("org.sqlite.JDBC")
  private val con: Connection = DriverManager.getConnection(sqliteDBUrl)

  // logger.log(Level.INFO, "Database connection established. Connection object: " + con)

  def init() = {
    logger.log(Level.FINE, "EpiDataLiteContext initialized.")
  }

  def query(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): JList[JLinkedHashMap[String, Object]] = {
    logger.log(Level.FINE, "query method invoked")
    query(fieldQuery, beginTime, endTime, com.epidata.lib.models.Measurement.DBTableName)
  }

  private def getDataFrame(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp,
    tableName: String): JList[JLinkedHashMap[String, Object]] = {

    // Find the equality queries for the partition key fields.
    val FieldsQuery = genericPartitionFields
      .map(partitionFieldsMap).map(fieldQuery)

    // logger.log(Level.FINE, "getDataFrame method invoked with field query: " + FieldsQuery.toString())

    val orderedEpochs = Measurement.epochForTs(beginTime) to Measurement.epochForTs(endTime)
    val epoch = orderedEpochs.toArray

    // Calculating # of bindmarkers
    var epoch_str = ""
    for (i <- 1 to epoch.length) {
      epoch_str += "?,"
    }
    epoch_str = epoch_str.slice(0, epoch_str.length - 1)

    // Create a ResultSet for a specified epoch
    def rsQuery(parameter: List[AnyRef]): ResultSet = {
      val customerCount = parameter.head.asInstanceOf[List[String]].length
      val customerStr = List.fill(customerCount)("?").mkString(", ")
      val siteCount = parameter(1).asInstanceOf[List[String]].length
      val siteStr = List.fill(siteCount)("?").mkString(", ")
      val collectionCount = parameter(2).asInstanceOf[List[String]].length
      val collectionStr = List.fill(collectionCount)("?").mkString(", ")
      val datasetCount = parameter(3).asInstanceOf[List[String]].length
      val datasetStr = List.fill(datasetCount)("?").mkString(", ")

      val query = getSelectStatementString(tableName, customerStr, siteStr, collectionStr, datasetStr, epoch_str)
      // logger.log(Level.INFO, "select statement query string: " + query)

      val stmt = con.prepareStatement(query)

      for (i <- 1 to customerCount) {
        stmt.setString(i, parameter(0).asInstanceOf[List[String]](i - 1))
      }
      for (i <- 1 to siteCount) {
        stmt.setString((i + customerCount), parameter(1).asInstanceOf[List[String]](i - 1))
      }
      for (i <- 1 to collectionCount) {
        stmt.setString((i + customerCount + siteCount), parameter(2).asInstanceOf[List[String]](i - 1))
      }
      for (i <- 1 to datasetCount) {
        stmt.setString((i + customerCount + siteCount + collectionCount), parameter(3).asInstanceOf[List[String]](i - 1))
      }
      tableName match {
        case BaseMeasurementSummary.DBTableName =>
          stmt.setTimestamp((customerCount + siteCount + collectionCount + datasetCount) + 1, beginTime)
          stmt.setTimestamp((customerCount + siteCount + collectionCount + datasetCount) + 2, endTime)
        case _ =>
          for (i <- 1 to epoch.length) {
            stmt.setInt((i + customerCount + siteCount + collectionCount + datasetCount), epoch(i - 1))
          }
          stmt.setTimestamp((customerCount + siteCount + collectionCount + datasetCount + epoch.length) + 1, beginTime)
          stmt.setTimestamp((customerCount + siteCount + collectionCount + datasetCount + epoch.length) + 2, endTime)
      }

      val rs = stmt.executeQuery()

      // logger.log(Level.INFO, "Result set object: " + rs.toString())

      rs
    }

    // Transform ResultSet to corresponding objects
    val select_rs = rsQuery(FieldsQuery)
    val maps = transformResultSet(select_rs, tableName)

    // logger.log(Level.INFO, "Transformed result set: " + maps.toString())

    try {
      select_rs.close()
    } catch {
      case e: SQLException => logger.log(Level.WARNING, "Error closing ResultSet")
    }

    maps
  }

  private def transformResultSet(rs: ResultSet, tableName: String): JList[JLinkedHashMap[String, Object]] = {
    var maps: JList[JLinkedHashMap[String, Object]] = new JLinkedList[JLinkedHashMap[String, Object]]()
    tableName match {
      case BaseMeasurement.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME =>
            while (rs.next()) {
              maps.add(BaseMeasurement.resultSetToJLinkedHashMap(rs, tableName, measurementClass))
              // logger.log(Level.INFO, "ATE map: " + maps.toString())
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              maps.add(BaseMeasurement.resultSetToJLinkedHashMap(rs, tableName, measurementClass))
              // logger.log(Level.INFO, "PAC map: " + maps.toString())
            }
        }

      case BaseMeasurementCleansed.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME =>
            while (rs.next()) {
              maps.add(BaseMeasurementCleansed.resultSetToJLinkedHashMap(rs, tableName, measurementClass))
              // logger.log(Level.INFO, "ATE map: " + maps.toString())
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              maps.add(BaseMeasurementCleansed.resultSetToJLinkedHashMap(rs, tableName, measurementClass))
              // logger.log(Level.INFO, "PAC map: " + maps.toString())
            }
        }

      case BaseMeasurementSummary.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME =>
            while (rs.next()) {
              maps.add(BaseMeasurementSummary.resultSetToJLinkedHashMap(rs, tableName, measurementClass))
              // logger.log(Level.INFO, "ATE map: " + maps.toString())
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              maps.add(BaseMeasurementSummary.resultSetToJLinkedHashMap(rs, tableName, measurementClass))
              // logger.log(Level.INFO, "PAC map: " + maps.toString())
            }
        }
    }

    maps
  }

  /**
   * @param fieldQuery Map indicating required values for specified fields.
   *                   Some fields may be required, but the names of these
   *                   fields will vary based on the system configuration.
   */
  def query(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp,
    tableName: String): JList[JLinkedHashMap[String, Object]] = {

    logger.log(Level.FINE, "Query method invoked with field query: " + fieldQuery.toString())

    if (beginTime.getTime > endTime.getTime) {
      logger.log(Level.WARNING, "beginTime must not be after endTime.")
      throw new IllegalArgumentException("beginTime must not be after endTime. ")
    }

    if (!partitionFieldsMap.values.toSet.subsetOf(fieldQuery.keySet)) {
      logger.log(Level.WARNING, "Required field missing from fieldQuery. " +
        s"Required fields: ${partitionFieldsMap.values.toList}")
      throw new IllegalArgumentException("Required field missing from fieldQuery. " +
        s"Required fields: ${partitionFieldsMap.values.toList}")
    }

    if (fieldQuery.filter(_._2.isEmpty).nonEmpty) {
      logger.log(Level.WARNING, "All fieldQuery entries must have at least one match value.")
      throw new IllegalArgumentException(
        "All fieldQuery entries must have at least one match value.")
    }

    val modelColumns = tableName match {
      case BaseMeasurement.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME => BaseAutomatedTest.getColumns
          case BaseSensorMeasurement.NAME => BaseSensorMeasurement.getColumns
        }

      case BaseMeasurementCleansed.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME => BaseAutomatedTestCleansed.getColumns
          case BaseSensorMeasurement.NAME => BaseSensorMeasurementCleansed.getColumns
        }

      case BaseMeasurementSummary.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME => BaseAutomatedTestSummary.getColumns
          case BaseSensorMeasurement.NAME => BaseSensorMeasurementSummary.getColumns
        }
    }

    if (!fieldQuery.keySet.subsetOf(modelColumns)) {
      logger.log(Level.WARNING, "Unexpected field in fieldQuery.")
      throw new IllegalArgumentException("Unexpected field in fieldQuery.")
    }

    val map = getDataFrame(fieldQuery, beginTime, endTime, tableName)
    // logger.log(Level.INFO, "Map result: " + map)

    // Find the equality queries for the non partition key fields.
    val nonpartitionFields = fieldQuery.keySet.diff(genericPartitionFields.map(partitionFieldsMap).toSet)
    val nonpartitionFieldsQuery = fieldQuery.filterKeys(nonpartitionFields)

    var filtered = nonpartitionFieldsQuery.foldLeft(map)((mp, x) => {
      mp.filter(m => x._2.contains(m.get(x._1)))
    })

    // logger.log(Level.INFO, "Filtered result:" + filtered.toString())

    filtered
  }

  /** Query interface for Java and Python. */
  def query(
    fieldQuery: java.util.Map[String, java.util.List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): JList[JLinkedHashMap[String, Object]] = {
    import scala.collection.JavaConversions._
    query(fieldQuery.toMap.mapValues(_.toList), beginTime, endTime, BaseMeasurement.DBTableName)
  }

  /** Query interface for Java and Python. */
  def queryMeasurementCleansed(
    fieldQuery: java.util.Map[String, java.util.List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): JList[JLinkedHashMap[String, Object]] = {
    // logger.log(Level.FINE, "queryMeasurementCleansed method called.")
    import scala.collection.JavaConversions._
    query(fieldQuery.toMap.mapValues(_.toList), beginTime, endTime, BaseMeasurementCleansed.DBTableName)
  }

  /** Query interface for Java and Python. */
  def queryMeasurementSummary(
    fieldQuery: java.util.Map[String, java.util.List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): JList[JLinkedHashMap[String, Object]] = {
    // logger.log(Level.FINE, "queryMeasurementSummary method called.")
    import scala.collection.JavaConversions._
    query(fieldQuery.toMap.mapValues(_.toList), beginTime, endTime, BaseMeasurementSummary.DBTableName)
  }

  /** List the values of the currently saved partition key fields. */
  def listKeys(): JList[JLinkedHashMap[String, Object]] = {
    logger.log(Level.FINE, "listKeys method called.")

    val query = getKeysStatementString(BaseMeasurementsKeys.DBTableName)
    val stmt = con.prepareStatement(query)
    val rs = stmt.executeQuery()
    var keys: JList[JLinkedHashMap[String, Object]] = new JLinkedList[JLinkedHashMap[String, Object]]()
    measurementClass match {
      case BaseAutomatedTest.NAME =>
        while (rs.next()) {
          val meas_key = MeasurementKey(
            Option(rs.getString("customer")).get,
            Option(rs.getString("customer_site")).get,
            Option(rs.getString("collection")).get,
            Option(rs.getString("dataset")).get)
          keys.add(AutomatedTestKey.toJLinkedHashMap(AutomatedTestKey.keyToAutomatedTest(meas_key)))
        }

      case BaseSensorMeasurement.NAME =>
        while (rs.next()) {
          val meas_key = MeasurementKey(
            Option(rs.getString("customer")).get,
            Option(rs.getString("customer_site")).get,
            Option(rs.getString("collection")).get,
            Option(rs.getString("dataset")).get)
          keys.add(AutomatedTestKey.toJLinkedHashMap(AutomatedTestKey.keyToAutomatedTest(meas_key)))
        }
    }

    try {
      rs.close()
    } catch {
      case e: SQLException => println("Error closing ResultSet")
    }

    keys
  }

  private val genericPartitionFields = List("customer", "customer_site", "collection", "dataset")

  private def partitionFieldsMap = measurementClass match {
    case BaseAutomatedTest.NAME => Map(
      "customer" -> "company",
      "customer_site" -> "site",
      "collection" -> "device_group",
      "dataset" -> "tester")
    case BaseSensorMeasurement.NAME => Map(
      "customer" -> "company",
      "customer_site" -> "site",
      "collection" -> "station",
      "dataset" -> "sensor")
    case _ => throw new IllegalArgumentException(
      "Invalid spark.epidata.measurementClass configuration.")
  }

  private def getSelectStatementString(tableName: String, customerStr: String, siteStr: String, collectionStr: String, datasetStr: String, epoch: String): String = {
    tableName match {
      case BaseMeasurementSummary.DBTableName =>
        val query = s"SELECT * FROM ${tableName} WHERE customer IN (" + customerStr + ") AND customer_site IN (" + siteStr + ") AND collection IN (" + collectionStr + ") AND dataset IN (" + datasetStr + ") AND start_time>=? AND stop_time<?"
        query
      case _ =>
        val query = s"SELECT * FROM ${tableName} WHERE customer IN (" + customerStr + ") AND customer_site IN (" + siteStr + ") AND collection IN (" + collectionStr + ") AND dataset IN (" + datasetStr + ") AND epoch IN (" + epoch + ") AND ts>=? AND ts<?"
        query
    }
  }

  private def getKeysStatementString(tableName: String): String = {
    val query = s"SELECT * FROM ${tableName}"
    query
  }

}

case class EpiDataConf(model: String, dbUrl: String)
