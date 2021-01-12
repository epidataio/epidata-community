/*
 * Copyright (c) 2015-2020 EpiData, Inc.
*/

package com.epidata.spark

import com.datastax.driver.core.querybuilder.QueryBuilder

import java.sql.{ Connection, DriverManager, ResultSet, SQLException, Statement, Timestamp }
import java.util.Date
import com.epidata.lib.models.{ Measurement => BaseMeasurement, MeasurementCleansed => BaseMeasurementCleansed, MeasurementsKeys => BaseMeasurementsKeys }
import com.epidata.lib.models.{ AutomatedTest => BaseAutomatedTest, SensorMeasurement => BaseSensorMeasurement, MeasurementSummary }

/**
 * The context of an Epidata connection to Spark, constructed with a provided
 * SparkContext.
 */
class EpidataLiteContext() {
  // Coming from ipython conf => now want it in spark conf
  private lazy val SQLiteDBName = "1234"
  private lazy val measurementClass = "automated_test"
  private lazy val streamingBatchDuration = 5

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
    val epochs = Measurement.epochForTs(beginTime) to Measurement.epochForTs(endTime)

    // Add config file for SQLite in folder?
    val con = DriverManager.getConnection("jdbc:sqlite:/Users/JFu/Desktop/empty.db")

    // Create a ResultSet for a specified epoch
    def rsQuery(parameter: List[Any]): ResultSet = {
      val query = QueryBuilder.select().all().from(tableName).where()
        .and(QueryBuilder.eq("customer", parameter(0)))
        .and(QueryBuilder.eq("customer_site", parameter(1)))
        .and(QueryBuilder.eq("collection", parameter(2)))
        .and(QueryBuilder.eq("dataset", parameter(3)))
        .and(QueryBuilder.in("epoch", epochs))
        .and(QueryBuilder.gte("ts", beginTime))
        .and(QueryBuilder.lt("ts", endTime))
        .toString()
      val rs = con.createStatement().executeQuery(query)
      rs
    }

    // Transform ResultSet to corresponding objects
    val select_rs = rsQuery(FieldsQuery)
    val rs = transformResultSet(select_rs, tableName)

    con.close()
    rs
  }
  /**
   * Todo: Change unionrs to rs
   * If SQLite has strict range query as last part of the statment like Cassandra
   *  yes, then filter after
   *  no, add key1 & 2 to query builder
   * Check if conversion to rdd needs to start up Spark
   */
  private def transformResultSet(rs: ResultSet, tableName: String): Array[Any] = {
    var df_list = Array[Any]()
    print(s"Table name is  ${tableName}")
    tableName match {
      case BaseMeasurement.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME =>
            while (rs.next()) {
              df_list = df_list :+ AutomatedTest.measurementToAutomatedTest(BaseMeasurement.rowToMeasurement(rs))
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              df_list = df_list :+ SensorMeasurement.measurementToSensorMeasurement(BaseMeasurement.rowToMeasurement(rs))
            }
        }

      case BaseMeasurementCleansed.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME =>
            while (rs.next()) {
              df_list = df_list :+ AutomatedTestCleansed.measurementCleansedToAutomatedTestCleansed(BaseMeasurementCleansed.rowToMeasurementCleansed(rs))
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              df_list = df_list :+ SensorMeasurementCleansed.measurementCleansedToSensorMeasurementCleansed(BaseMeasurementCleansed.rowToMeasurementCleansed(rs))
            }
        }

      case MeasurementSummary.DBTableName =>
        measurementClass match {
          case BaseAutomatedTest.NAME =>
            while (rs.next()) {
              df_list = df_list :+ BaseAutomatedTest.measurementSummaryToAutomatedTestSummary(MeasurementSummary.rowToMeasurementSummary(rs))
            }
          case BaseSensorMeasurement.NAME =>
            while (rs.next()) {
              df_list = df_list :+ BaseSensorMeasurement.measurementSummaryToSensorMeasurementSummary(MeasurementSummary.rowToMeasurementSummary(rs))
            }
        }
    }

    print(s"The length of final array is ${df_list.length}")
    df_list
  }

  // query: have some checks, call getdataframe, return array/whatever
  // getDataFrame: database query, call transformation, return final arr
  // transformResultSet: 2 layers transformation (Resultset->MeasureLite-> Auto/Sensor)

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
    //    print(fieldQuery.keySet)
    //    print(BaseMeasurement.getColumns())
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
    val con = DriverManager.getConnection("SQLite.url")
    val query = QueryBuilder.select().all().from(BaseMeasurementsKeys.DBTableName).toString()
    val rs = con.createStatement().executeQuery(query)
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

}
