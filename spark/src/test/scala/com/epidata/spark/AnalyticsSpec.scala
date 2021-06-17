/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{ YamlTransformations, SparkTemplate, EmbeddedCassandra }
import com.epidata.spark.analytics.IMR
import com.epidata.specs.CassandraSchema
import org.apache.log4j.{ Level, Logger }
import org.junit.runner.RunWith
import org.scalatest._
import java.sql.Timestamp
import org.apache.spark.MeasurementValue
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AnalyticsAutomatedTestSpec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll with EmbeddedCassandra with SparkTemplate with Matchers {
  override def clearCache(): Unit = CassandraConnector.evictCache()

  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)

  private val cassandraKeyspaceName = "epidata_analytics_test"
  private val measurementClass = "automated_test"

  YamlTransformations.Default.addTransformation("cdc_raw_directory", "./raw_dir/")

  // Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))

  var conf = defaultConf
    .set("spark.epidata.cassandraKeyspaceName", cassandraKeyspaceName)
    .set("spark.epidata.measurementClass", measurementClass)

  useSparkConf(conf)

  val connector = CassandraConnector(conf)

  connector.withSessionDo { session =>
    session.execute(CassandraSchema.keyspaceCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.measurementsOriginalTableCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.measurementsCleansedTableCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.measurementsSummaryTableCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.measurementKeysTableCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.userTableCreation.format(cassandraKeyspaceName))
  }

  val beginTime = new Timestamp(1428004316123L)
  val endTime = new Timestamp(1428004316123L + 10000L)

  before {
    // Clear existing measurements.
    connector.withSessionDo { session =>
      session.execute(s"TRUNCATE epidata_analytics_test.${com.epidata.lib.models.Measurement.DBTableName}")
      session.execute(s"TRUNCATE epidata_analytics_test.${com.epidata.lib.models.MeasurementsKeys.DBTableName}")
    }

    // Add fixtures

    val ts1 = beginTime
    val ts2 = new Timestamp(beginTime.getTime + 2)
    val ts3 = new Timestamp(beginTime.getTime + 3)
    val ts4 = new Timestamp(beginTime.getTime + 4)
    val ts5 = new Timestamp(beginTime.getTime + 5)
    val epoch = Measurement.epochForTs(beginTime)

    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts1, "100001", "Test-1",
      "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts2, "100001", "Test-1",
      "Meas-1", 48.1, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts3, "100001", "Test-1",
      "Meas-1", 49.2, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts4, "100001", "Test-1",
      "Meas-2", "STRING", null, "PASS", null, null, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value_s", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts5, "100001", "Test-1",
      "Meas-2", "STRING", null, "PASS", null, null, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value_s", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  "IMR" should "return correct results" in {

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1"),
        "meas_name" -> List("Meas-1")),
      beginTime,
      endTime)

    val (imr, imrSchema) = IMR(results.collect.toList, results.schema)
    val row0 = imr(0).toSeq
    val row1 = imr(1).toSeq
    val row2 = imr(2).toSeq

    // Original meas_value field.
    row0(9).asInstanceOf[MeasurementValue].value should equal(45.7)
    row1(9).asInstanceOf[MeasurementValue].value should equal(48.1)
    row2(9).asInstanceOf[MeasurementValue].value should equal(49.2)

    // I field.
    row0(17) should equal(45.7)
    row1(17) should equal(48.1)
    row2(17) should equal(49.2)

    val iMean = (45.7 + 48.1 + 49.2) / 3.0
    row0(18) should equal(iMean)
    row1(18) should equal(iMean)
    row2(18) should equal(iMean)

    val iLcl = iMean - 2.66 * iMean
    row0(19) should equal(iLcl)
    row1(19) should equal(iLcl)
    row2(19) should equal(iLcl)

    val iUcl = iMean + 2.66 * iMean
    row0(20) should equal(iUcl)
    row1(20) should equal(iUcl)
    row2(20) should equal(iUcl)

    val mr = List(Double.NaN, 48.1 - 45.7, 49.2 - 48.1)
    row0(21).asInstanceOf[Double].isNaN should be(true)
    row1(21) should equal(mr(1))
    row2(21) should equal(mr(2))

    val mrMean = (48.1 - 45.7 + 49.2 - 48.1) / 2.0
    row0(22) should equal(mrMean)
    row1(22) should equal(mrMean)
    row2(22) should equal(mrMean)

    val mrLcl = 0.0
    row0(23) should equal(mrLcl)
    row1(23) should equal(mrLcl)
    row2(23) should equal(mrLcl)

    val mrUcl = mrMean + 3.267 * mrMean
    row0(24) should equal(mrUcl)
    row1(24) should equal(mrUcl)
    row2(24) should equal(mrUcl)
  }

  "IMR" should "raise exception with too few rows" in {
    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1")),
      beginTime,
      new Timestamp(beginTime.getTime + 1))

    intercept[IllegalArgumentException] {
      IMR(results.collect.toList, results.schema)
    }
  }

  "IMR" should "raise exception on non unique meas_name" in {

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1")
      // Not restricting meas_name field.
      ),
      beginTime,
      endTime)

    intercept[IllegalArgumentException] {
      IMR(results.collect.toList, results.schema)
    }
  }

  "IMR" should "raise exception on non numeric meas_value" in {

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1"),
        "meas_name" -> List("Meas-2")),
      beginTime,
      endTime)

    intercept[IllegalArgumentException] {
      IMR(results.collect.toList, results.schema)
    }
  }

}

@RunWith(classOf[JUnitRunner])
class AnalyticsSensorMeasurementSpec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll with EmbeddedCassandra with SparkTemplate with Matchers {
  override def clearCache(): Unit = CassandraConnector.evictCache()

  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)

  private val cassandraKeyspaceName = "epidata_analytics_test"
  private val measurementClass = "sensor_measurement"

  YamlTransformations.Default.addTransformation("cdc_raw_directory", "./raw_dir/")

  // Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))

  var conf = defaultConf
    .set("spark.epidata.cassandraKeyspaceName", cassandraKeyspaceName)
    .set("spark.epidata.measurementClass", measurementClass)

  useSparkConf(conf)

  val connector = CassandraConnector(conf)

  connector.withSessionDo { session =>
    session.execute(CassandraSchema.keyspaceCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.measurementsOriginalTableCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.measurementsCleansedTableCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.measurementsSummaryTableCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.measurementKeysTableCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.userTableCreation.format(cassandraKeyspaceName))
  }

  val beginTime = new Timestamp(1428004316123L)
  val endTime = new Timestamp(1428004316123L + 10000L)

  before {

    // Clear existing measurements.
    connector.withSessionDo { session =>
      session.execute(s"TRUNCATE epidata_analytics_test.${com.epidata.lib.models.Measurement.DBTableName}")
      session.execute(s"TRUNCATE epidata_analytics_test.${com.epidata.lib.models.MeasurementsKeys.DBTableName}")
    }

    // Add fixtures

    val ts1 = beginTime
    val ts2 = new Timestamp(beginTime.getTime + 2)
    val ts3 = new Timestamp(beginTime.getTime + 3)
    val epoch = Measurement.epochForTs(beginTime)

    sc.parallelize(Seq(("Company-1", "Site-1", "Station-1", "Sensor-1", epoch, ts1, "Event-1", "Meas-1",
      "", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", null, null)))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    sc.parallelize(Seq(("Company-1", "Site-1", "Station-1", "Sensor-1", epoch, ts2, "Event-1", "Meas-1",
      "", 48.1, "degree C", "PASS", 40.0, 90.0, "Description", null, null)))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    sc.parallelize(Seq(("Company-1", "Site-1", "Station-1", "Sensor-1", epoch, ts3, "Event-1", "Meas-1",
      "", 49.2, "degree C", "PASS", 40.0, 90.0, "Description", null, null)))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  "IMR" should "return correct results" in {

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "station" -> List("Station-1"),
        "sensor" -> List("Sensor-1")),
      beginTime,
      endTime)

    val (imr, imrSchema) = IMR(results.collect.toList, results.schema)
    val row0 = imr(0).toSeq
    val row1 = imr(1).toSeq
    val row2 = imr(2).toSeq

    // Original meas_value field.
    print(row0)
    row0(8).asInstanceOf[MeasurementValue].value should equal(45.7)
    row1(8).asInstanceOf[MeasurementValue].value should equal(48.1)
    row2(8).asInstanceOf[MeasurementValue].value should equal(49.2)

    // I field.
    row0(14) should equal(45.7)
    row1(14) should equal(48.1)
    row2(14) should equal(49.2)

    val iMean = (45.7 + 48.1 + 49.2) / 3.0
    row0(15) should equal(iMean)
    row1(15) should equal(iMean)
    row2(15) should equal(iMean)

    val iLcl = iMean - 2.66 * iMean
    row0(16) should equal(iLcl)
    row1(16) should equal(iLcl)
    row2(16) should equal(iLcl)

    val iUcl = iMean + 2.66 * iMean
    row0(17) should equal(iUcl)
    row1(17) should equal(iUcl)
    row2(17) should equal(iUcl)

    val mr = List(Double.NaN, 48.1 - 45.7, 49.2 - 48.1)
    row0(18).asInstanceOf[Double].isNaN should be(true)
    row1(18) should equal(mr(1))
    row2(18) should equal(mr(2))

    val mrMean = (48.1 - 45.7 + 49.2 - 48.1) / 2.0
    row0(19) should equal(mrMean)
    row1(19) should equal(mrMean)
    row2(19) should equal(mrMean)

    val mrLcl = 0.0
    row0(20) should equal(mrLcl)
    row1(20) should equal(mrLcl)
    row2(20) should equal(mrLcl)

    val mrUcl = mrMean + 3.267 * mrMean
    row0(21) should equal(mrUcl)
    row1(21) should equal(mrUcl)
    row2(21) should equal(mrUcl)
  }
}
