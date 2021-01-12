/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark

import com.datastax.spark.connector._
import java.sql.Timestamp
import com.epidata.lib.models.{ Measurement => BaseMeasurement, MeasurementCleansed => BaseMeasurementCleansed, MeasurementSummary, SensorMeasurement => BaseSensorMeasurement, AutomatedTest => BaseAutomatedTest, MeasurementsKeys => BaseMeasurementsKeys }
import com.epidata.spark.ops.{ Identity, OutlierDetector, MeasStatistics, FillMissingValue }
import com.epidata.spark.utils.DataFrameUtils
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.sql.functions.lit
import org.apache.spark.streaming.Seconds

/**
 * The context of an Epidata connection to Spark, constructed with a provided
 * SparkContext.
 */
class EpidataContext(private val sparkContext: SparkContext) {

  /** Constructor used from Java and Python. */
  def this(javaSparkContext: JavaSparkContext) = this(javaSparkContext.sc)

  private val sqlContext = new SQLContext(sparkContext)

  // Configuration parameters.
  private lazy val cassandraKeyspaceName = sparkContext.getConf.get("spark.epidata.cassandraKeyspaceName")
  private lazy val measurementClass =
    sparkContext.getConf.get("spark.epidata.measurementClass")

  private lazy val kafkaBrokers = sparkContext.getConf.get("spark.epidata.kafkaBrokers", "localhost:9092")
  private lazy val streamingBatchDuration: Int = sparkContext.getConf.get("spark.epidata.streaming.batchDuration", EpidataStreamingContext.BatchDurationInSecond.toString).toInt

  def getCassandraKeyspaceName = cassandraKeyspaceName
  def getKafkaBrokers = kafkaBrokers

  def query(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): DataFrame = {
    query(fieldQuery, beginTime, endTime, com.epidata.lib.models.Measurement.DBTableName)
  }

  private def getUnionRDD(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp,
    tableName: String): RDD[Measurement] = {

    import MeasurementHelpers._

    // Find the equality queries for the partition key fields.
    val partitionFieldsQuery = genericPartitionFields
      .map(partitionFieldsMap)
      .map(fieldQuery)

    val table = sparkContext.cassandraTable[Measurement](cassandraKeyspaceName, tableName)

    // Find all epochs covered by the query.
    val epochs = Measurement.epochForTs(beginTime) to Measurement.epochForTs(endTime)

    // Create an RDD for a specified epoch, using a CQL query.
    def rddForPartition(partition: List[Any]): RDD[Measurement] =
      table.where(
        DataFrameUtils.whereStatementForTable(tableName),
        partition ++ List(beginTime, endTime): _*).withAscOrder

    val partitions = for (
      a <- partitionFieldsQuery(0);
      b <- partitionFieldsQuery(1);
      c <- partitionFieldsQuery(2);
      d <- partitionFieldsQuery(3);
      e <- epochs
    ) yield List(a, b, c, d, e)

    // Create and concatenate the RDDs for all epochs in range.
    val unionRDD = partitions
      .map(rddForPartition)
      .reduceLeft(_ ++ _)

    unionRDD

  }

  private def getUnionRDDMeasurementCleansed(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp,
    tableName: String): RDD[MeasurementCleansed] = {

    import MeasurementHelpers._

    // Find the equality queries for the partition key fields.
    val partitionFieldsQuery = genericPartitionFields
      .map(partitionFieldsMap)
      .map(fieldQuery)

    val table = sparkContext.cassandraTable[MeasurementCleansed](cassandraKeyspaceName, tableName)

    // Find all epochs covered by the query.
    val epochs = Measurement.epochForTs(beginTime) to Measurement.epochForTs(endTime)

    // Create an RDD for a specified epoch, using a CQL query.
    def rddForPartition(partition: List[Any]): RDD[MeasurementCleansed] =
      table.where(
        DataFrameUtils.whereStatementForTable(tableName),
        partition ++ List(beginTime, endTime): _*).withAscOrder

    val partitions = for (
      a <- partitionFieldsQuery(0);
      b <- partitionFieldsQuery(1);
      c <- partitionFieldsQuery(2);
      d <- partitionFieldsQuery(3);
      e <- epochs
    ) yield List(a, b, c, d, e)

    // Create and concatenate the RDDs for all epochs in range.
    val unionRDD = partitions
      .map(rddForPartition)
      .reduceLeft(_ ++ _)

    unionRDD
  }

  private def getUnionRDDMeasurementSummary(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp,
    tableName: String): RDD[MeasurementSummary] = {

    import MeasurementHelpers._

    // Find the equality queries for the partition key fields.
    val partitionFieldsQuery = genericPartitionFields
      .map(partitionFieldsMap)
      .map(fieldQuery)

    val table = sparkContext.cassandraTable[MeasurementSummary](cassandraKeyspaceName, tableName)

    // Create an RDD for a specified epoch, using a CQL query.
    def rddForPartition(partition: List[Any]): RDD[MeasurementSummary] =
      table.where(
        DataFrameUtils.whereStatementForTable(tableName),
        partition ++ List(beginTime, endTime): _*).withAscOrder

    val partitions = for (
      a <- partitionFieldsQuery(0);
      b <- partitionFieldsQuery(1);
      c <- partitionFieldsQuery(2);
      d <- partitionFieldsQuery(3)
    ) yield List(a, b, c, d)

    // Create and concatenate the RDDs for all epochs in range.
    val unionRDD = partitions
      .map(rddForPartition)
      .reduceLeft(_ ++ _)

    unionRDD

  }

  private def getDataFrame(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp,
    tableName: String): DataFrame = {

    tableName match {
      case BaseMeasurement.DBTableName =>
        val unionRDD = getUnionRDD(fieldQuery, beginTime, endTime, tableName)

        measurementClass match {
          case BaseAutomatedTest.NAME => sqlContext.createDataFrame(unionRDD.map(AutomatedTest.measurementToAutomatedTest))
          case BaseSensorMeasurement.NAME => sqlContext.createDataFrame(unionRDD.map(SensorMeasurement.measurementToSensorMeasurement))
        }

      case BaseMeasurementCleansed.DBTableName =>
        val unionRDD = getUnionRDDMeasurementCleansed(fieldQuery, beginTime, endTime, tableName)
        measurementClass match {
          case BaseAutomatedTest.NAME => sqlContext.createDataFrame(unionRDD.map(AutomatedTestCleansed.measurementCleansedToAutomatedTestCleansed))
          case BaseSensorMeasurement.NAME => sqlContext.createDataFrame(unionRDD.map(SensorMeasurementCleansed.measurementCleansedToSensorMeasurementCleansed))
        }

      case MeasurementSummary.DBTableName =>
        val unionRDD = getUnionRDDMeasurementSummary(fieldQuery, beginTime, endTime, tableName)
        measurementClass match {
          case BaseAutomatedTest.NAME => sqlContext.createDataFrame(unionRDD.map(BaseAutomatedTest.measurementSummaryToAutomatedTestSummary))
          case BaseSensorMeasurement.NAME => sqlContext.createDataFrame(unionRDD.map(BaseSensorMeasurement.measurementSummaryToSensorMeasurementSummary))
        }
    }

  }

  /**
   * Read Measurements from Cassandra into a DataFrame. The Measurements
   * matching the query and falling between beginTime and endTime are returned.
   *
   * @param fieldQuery Map indicating required values for specified fields.
   *                   Some fields may be required, but the names of these
   *                   fields will vary based on the system configuration.
   */
  def query(
    fieldQuery: Map[String, List[String]],
    beginTime: Timestamp,
    endTime: Timestamp,
    tableName: String): DataFrame = {

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

    if (!fieldQuery.keySet.subsetOf(dataFrame.columns.toSet)) {
      throw new IllegalArgumentException("Unexpected field in fieldQuery.")
    }

    // Find the equality queries for the non partition key fields.
    val nonpartitionFields = fieldQuery.keySet.diff(genericPartitionFields.map(partitionFieldsMap).toSet)
    val nonpartitionFieldsQuery = fieldQuery.filterKeys(nonpartitionFields)

    // Filter by any applicable non partition key fields.
    val filtered = nonpartitionFieldsQuery.foldLeft(dataFrame)((df, filter) =>
      df.filter(df.col(filter._1).isin(filter._2.map(lit(_)): _*)))

    filtered
  }

  /** Query interface for Java and Python. */
  def query(
    fieldQuery: java.util.Map[String, java.util.List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): DataFrame = {
    import scala.collection.JavaConversions._
    query(fieldQuery.toMap.mapValues(_.toList), beginTime, endTime, BaseMeasurement.DBTableName)
  }

  /** Query interface for Java and Python. */
  def queryMeasurementCleansed(
    fieldQuery: java.util.Map[String, java.util.List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): DataFrame = {
    import scala.collection.JavaConversions._
    query(fieldQuery.toMap.mapValues(_.toList), beginTime, endTime, BaseMeasurementCleansed.DBTableName)
  }

  /** Query interface for Java and Python. */
  def queryMeasurementSummary(
    fieldQuery: java.util.Map[String, java.util.List[String]],
    beginTime: Timestamp,
    endTime: Timestamp): DataFrame = {
    import scala.collection.JavaConversions._
    query(fieldQuery.toMap.mapValues(_.toList), beginTime, endTime, MeasurementSummary.DBTableName)
  }

  /** List the values of the currently saved partition key fields. */
  def listKeys(): DataFrame = {
    import MeasurementHelpers._
    import AutomatedTestKey._
    import SensorMeasurementKey._

    val table = sparkContext.cassandraTable[MeasurementKey](cassandraKeyspaceName, BaseMeasurementsKeys.DBTableName)
    measurementClass match {
      case BaseAutomatedTest.NAME => sqlContext.createDataFrame(table.map(keyToAutomatedTest))
      case BaseSensorMeasurement.NAME => sqlContext.createDataFrame(table.map(keyToSensorMeasurement))
    }
  }

  @deprecated
  def createStream(op: String, meas_names: List[String], params: java.util.Map[String, String]): EpidataStreamingContext = {
    val esc = new EpidataStreamingContext(
      this,
      Seconds(streamingBatchDuration),
      com.epidata.lib.models.Measurement.KafkaTopic)

    op match {
      case "Identity" => esc.saveToCassandra(new Identity())
      case "FillMissingValue" => esc.saveToCassandra(new FillMissingValue(meas_names, "rolling", 3))
      case "OutlierDetector" => esc.saveToCassandra(new OutlierDetector("meas_value", "quartile"))
      case "MeasStatistics" => esc.saveToCassandra(new MeasStatistics(meas_names, "standard"))
    }

    esc

  }

  def getSQLContext = sqlContext
  def getSparkContext = sparkContext

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
