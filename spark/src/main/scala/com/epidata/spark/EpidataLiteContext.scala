/*
 * Copyright (c) 2015-2021 EpiData, Inc.
*/

package com.epidata.spark

import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement, Timestamp}
import com.epidata.lib.models.{Measurement => BaseMeasurement, MeasurementCleansed => BaseMeasurementCleansed, MeasurementsKeys => BaseMeasurementsKeys}
import com.epidata.lib.models.{MeasurementSummary, AutomatedTest => BaseAutomatedTest, SensorMeasurement => BaseSensorMeasurement}
import com.typesafe.config.ConfigFactory
import java.io.File
/**
 * The context of an Epidata connection to SQLite.
 */
class EpidataLiteContext() {
  private val conf = ConfigFactory.parseFile(new File("/Users/JFu/Documents/epidata-interns/spark/conf/sqlite-defaults.conf"))
  private lazy val SQLiteDBName = conf.getString("spark.epidata.SQLiteDBName")
  private lazy val measurementClass = conf.getString("spark.epidata.measurementClass")
  private lazy val streamingBatchDuration = conf.getInt("spark.epidata.streamingBatchDuration")

  def query(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): Array[Any] = {
    query(fieldQuery, beginTime, endTime, com.epidata.lib.models.Measurement.DBTableName)
  }

  private def getDataFrame(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp,
    tableName: String): Array[Any] = {

    // Find the equality queries for the partition key fields.
    val FieldsQuery = genericPartitionFields
      .map(partitionFieldsMap).flatMap(fieldQuery)

    // Add config file for SQLite in folder?
    val con = DriverManager.getConnection(conf.getString("spark.epidata.SQLite.url"))

    val orderedEpochs = Measurement.epochForTs(beginTime) to Measurement.epochForTs(endTime)
    val epoch = orderedEpochs.toArray
    // Calculating # of bindmarkers
    var epoch_str = ""
    for (i <- 1 to epoch.length) {
      epoch_str += "?,"
    }
    epoch_str = epoch_str.slice(0, epoch_str.length-1)

    // Create a ResultSet for a specified epoch
    def rsQuery(parameter: List[AnyRef]): ResultSet = {
      val query = getSelectStatmentString(tableName, epoch_str)
      val stmt = con.prepareStatement(query)
      stmt.setString(1, parameter.head.asInstanceOf[String])
      stmt.setString(2, parameter(1).asInstanceOf[String])
      stmt.setString(3, parameter(2).asInstanceOf[String])
      stmt.setString(4, parameter(3).asInstanceOf[String])
      for (i <- 1 to epoch.length) {
        stmt.setInt(4 + i, epoch(i-1))
      }
      stmt.setTimestamp(4 + epoch.length + 1, beginTime)
      stmt.setTimestamp(4 + epoch.length + 2, endTime)
      val rs = stmt.executeQuery()
      rs
    }

    // Transform ResultSet to corresponding objects
    val select_rs = rsQuery(FieldsQuery)
    val rs = transformResultSet(select_rs, tableName)

    con.close()
    rs
  }

  private def transformResultSet(rs: ResultSet, tableName: String): Array[Any] = {
    var df_list = Array[Any]()
    tableName match {
      case BaseMeasurement.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME =>
            while (rs.next()) {
              df_list :+= AutomatedTest.measurementToAutomatedTest(BaseMeasurement.rowToMeasurement(rs))
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              df_list :+= SensorMeasurement.measurementToSensorMeasurement(BaseMeasurement.rowToMeasurement(rs))
            }
        }

      case BaseMeasurementCleansed.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME =>
            while (rs.next()) {
              df_list :+= AutomatedTestCleansed.measurementCleansedToAutomatedTestCleansed(BaseMeasurementCleansed.rowToMeasurementCleansed(rs))
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              df_list :+= SensorMeasurementCleansed.measurementCleansedToSensorMeasurementCleansed(BaseMeasurementCleansed.rowToMeasurementCleansed(rs))
            }
        }

      case MeasurementSummary.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME =>
            while (rs.next()) {
              df_list :+= BaseAutomatedTest.measurementSummaryToAutomatedTestSummary(MeasurementSummary.rowToMeasurementSummary(rs))
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              df_list :+= BaseSensorMeasurement.measurementSummaryToSensorMeasurementSummary(MeasurementSummary.rowToMeasurementSummary(rs))
            }
        }
    }

    df_list
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
    tableName: String): Array[Any] = {

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

    val dataFrame = getDataFrame(fieldQuery, beginTime, endTime, tableName)
    //    if (!fieldQuery.keySet.subsetOf(BaseMeasurement.getColumns())) {
    //      throw new IllegalArgumentException("Unexpected field in fieldQuery.")
    //    }

    // Find the equality queries for the non partition key fields.
    //    val nonpartitionFields = fieldQuery.keySet.diff(genericPartitionFields.map(partitionFieldsMap).toSet)
    //    val nonpartitionFieldsQuery = fieldQuery.filterKeys(nonpartitionFields)
    //
    //    // Filter by any applicable non partition key fields.
    //    val filtered = nonpartitionFieldsQuery.foldLeft(dataFrame)((df, filter) =>
    //      df.filter(df.col(filter._1).isin(filter._2.map(lit(_)): _*)))

    //    filtered
    dataFrame
  }

  /** Query interface for Java and Python. */
  def query(
    fieldQuery: java.util.Map[String, java.util.List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): Array[Any] = {
    import scala.collection.JavaConversions._
    query(fieldQuery.toMap.mapValues(_.toList), beginTime, endTime, BaseMeasurement.DBTableName)
  }

  /** Query interface for Java and Python. */
  def queryMeasurementCleansed(
    fieldQuery: java.util.Map[String, java.util.List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): Array[Any] = {
    import scala.collection.JavaConversions._
    query(fieldQuery.toMap.mapValues(_.toList), beginTime, endTime, BaseMeasurementCleansed.DBTableName)
  }

  /** Query interface for Java and Python. */
  def queryMeasurementSummary(
    fieldQuery: java.util.Map[String, java.util.List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): Array[Any] = {
    import scala.collection.JavaConversions._
    query(fieldQuery.toMap.mapValues(_.toList), beginTime, endTime, MeasurementSummary.DBTableName)
  }

  /** List the values of the currently saved partition key fields. */
  def listKeys(): Array[Any] = {
    val con = DriverManager.getConnection(conf.getString("spark.epidata.SQLite.url"))
    val query = getKeysStatementString(BaseMeasurementsKeys.DBTableName)
    val rs = con.prepareStatement(query).executeQuery()
    var keys = Array[Any]()
    measurementClass match {
      case BaseAutomatedTest.NAME =>
        while (rs.next()) {
          val meas_key = MeasurementKey(
            Option(rs.getString("customer")).get,
            Option(rs.getString("customer_site")).get,
            Option(rs.getString("collection")).get,
            Option(rs.getString("dataset")).get)
          keys :+= AutomatedTestKey.keyToAutomatedTest(meas_key)
        }
      case BaseSensorMeasurement.NAME =>
        while (rs.next()) {
          val meas_key = MeasurementKey(
            Option(rs.getString("customer")).get,
            Option(rs.getString("customer_site")).get,
            Option(rs.getString("collection")).get,
            Option(rs.getString("dataset")).get)
          keys :+= SensorMeasurementKey.keyToSensorMeasurement(meas_key)
        }
    }

    keys
  }

  // TODO:
  //  @deprecated
  //  def createStream(op: String, meas_names: List[String], params: java.util.Map[String, String]): EpidataStreamingContext = {
  //    val esc = new EpidataStreamingContext(
  //      this,
  //      Seconds(streamingBatchDuration),
  //      com.epidata.lib.models.Measurement.KafkaTopic)
  //
  //    op match {
  //      case "Identity" => esc.saveToCassandra(new Identity())
  //      case "FillMissingValue" => esc.saveToCassandra(new FillMissingValue(meas_names, "rolling", 3))
  //      case "OutlierDetector" => esc.saveToCassandra(new OutlierDetector("meas_value", "quartile"))
  //      case "MeasStatistics" => esc.saveToCassandra(new MeasStatistics(meas_names, "standard"))
  //    }
  //
  //    esc
  //
  //  }

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

  private def getSelectStatmentString(tableName: String, epoch: String): String = {
    val query = s"SELECT * FROM ${tableName} WHERE customer=? AND customer_site=? AND collection=? AND dataset=? AND epoch IN (" + epoch + ") AND ts>=? AND ts<?"
    query
  }

  private def getKeysStatementString(tableName: String): String = {
    val query = s"SELECT * FROM ${tableName}"
    query
  }
}
