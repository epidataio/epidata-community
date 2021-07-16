/*
 * Copyright (c) 2015-2021 EpiData, Inc.
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

/**
 * The context of an Epidata connection to SQLite.
 */
class EpidataLiteContext(epidataConf: EpiDataConf = EpiDataConf("", "")) {

  // Auxiliary constructor for Java and Python
  def this() = {
    this(EpiDataConf("", ""))
  }

  //  private val conf = ConfigFactory.parseResources("sqlite-defaults.conf").resolve()
  private val conf = ConfigFactory.load("sqlite-defaults.conf").resolve()

  private lazy val sqliteDBName = conf.getString("spark.epidata.SQLite.dbFileName")

  private val measurementClass = epidataConf.model match {
    case m if m.trim.isEmpty => Properties.envOrElse("EPIDATA_MEASUREMENT_MODEL", conf.getString("spark.epidata.measurementClass"))
    case m: String => m
  }
  // println("measurement class: " + measurementClass)

  private lazy val streamingBatchDuration = conf.getInt("spark.epidata.streamingBatchDuration")
  private val basePath = new java.io.File(".").getAbsoluteFile().getParent()

  //  private val sqliteDBUrl = "jdbc:sqlite:" + basePath + "/data/" + sqliteDBName
  private val sqliteDBUrl = epidataConf.dbUrl match {
    case s if s.trim.isEmpty => "jdbc:sqlite:" + basePath + "/data/" + sqliteDBName
    case s => s
  }

  // println("sqlite db url: " + sqliteDBUrl)

  // Connect to SQLite database
  Class.forName("org.sqlite.JDBC")
  private val con: Connection = DriverManager.getConnection(sqliteDBUrl)

  def query(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): JList[JLinkedHashMap[String, Object]] = {
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

      val query = getSelectStatmentString(tableName, customerStr, siteStr, collectionStr, datasetStr, epoch_str)
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
      for (i <- 1 to epoch.length) {
        stmt.setInt((i + customerCount + siteCount + collectionCount + datasetCount), epoch(i - 1))
      }
      stmt.setTimestamp((customerCount + siteCount + collectionCount + datasetCount + epoch.length) + 1, beginTime)
      stmt.setTimestamp((customerCount + siteCount + collectionCount + datasetCount + epoch.length) + 2, endTime)

      val rs = stmt.executeQuery()

      rs
    }

    // Transform ResultSet to corresponding objects
    val select_rs = rsQuery(FieldsQuery)
    val maps = transformResultSet(select_rs, tableName)

    try {
      select_rs.close()
    } catch {
      case e: SQLException => println("Error closing ResultSet")
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
              maps.add(BaseMeasurement.rowToJLinkedHashMap(rs, tableName, measurementClass))
              // println("ATE maps meas_value: " + maps(0).get("meas_value") + ", class: " + maps(0).get("meas_value").getClass)
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              maps.add(BaseMeasurement.rowToJLinkedHashMap(rs, tableName, measurementClass))
              // println("PAC maps: " + maps)
            }
        }

      case BaseMeasurementCleansed.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME =>
            while (rs.next()) {
              maps.add(BaseMeasurement.rowToJLinkedHashMap(rs, tableName, measurementClass))
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              maps.add(BaseMeasurement.rowToJLinkedHashMap(rs, tableName, measurementClass))
            }
        }

      case BaseMeasurementSummary.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME =>
            while (rs.next()) {
              maps.add(BaseMeasurement.rowToJLinkedHashMap(rs, tableName, measurementClass))
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              maps.add(BaseMeasurement.rowToJLinkedHashMap(rs, tableName, measurementClass))
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

    if (beginTime.getTime > endTime.getTime) {
      throw new IllegalArgumentException("beginTime must not be after endTime. ")
    }

    if (!partitionFieldsMap.values.toSet.subsetOf(fieldQuery.keySet)) {
      throw new IllegalArgumentException("Required field missing from fieldQuery. " +
        s"Required fields: ${partitionFieldsMap.values.toList}")
    }

    if (fieldQuery.filter(_._2.isEmpty).nonEmpty) {
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
      throw new IllegalArgumentException("Unexpected field in fieldQuery.")
    }

    val map = getDataFrame(fieldQuery, beginTime, endTime, tableName)

    // Find the equality queries for the non partition key fields.
    val nonpartitionFields = fieldQuery.keySet.diff(genericPartitionFields.map(partitionFieldsMap).toSet)
    val nonpartitionFieldsQuery = fieldQuery.filterKeys(nonpartitionFields)

    var filtered = nonpartitionFieldsQuery.foldLeft(map)((mp, x) => {
      mp.filter(m => x._2.contains(m.get(x._1)))
    })

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
    import scala.collection.JavaConversions._
    query(fieldQuery.toMap.mapValues(_.toList), beginTime, endTime, BaseMeasurementCleansed.DBTableName)
  }

  /** Query interface for Java and Python. */
  def queryMeasurementSummary(
    fieldQuery: java.util.Map[String, java.util.List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): JList[JLinkedHashMap[String, Object]] = {
    import scala.collection.JavaConversions._
    query(fieldQuery.toMap.mapValues(_.toList), beginTime, endTime, BaseMeasurementSummary.DBTableName)
  }

  /** List the values of the currently saved partition key fields. */
  def listKeys(): JList[JLinkedHashMap[String, Object]] = {
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

  private def getSelectStatmentString(tableName: String, customerStr: String, siteStr: String, collectionStr: String, datasetStr: String, epoch: String): String = {
    val query = s"SELECT * FROM ${tableName} WHERE customer IN (" + customerStr + ") AND customer_site IN (" + siteStr + ") AND collection IN (" + collectionStr + ") AND dataset IN (" + datasetStr + ") AND epoch IN (" + epoch + ") AND ts>=? AND ts<?"
    query
  }

  private def getKeysStatementString(tableName: String): String = {
    val query = s"SELECT * FROM ${tableName}"
    query
  }

}

case class EpiDataConf(model: String, dbUrl: String)
