/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{ YamlTransformations, SparkTemplate, EmbeddedCassandra }
import com.epidata.specs.CassandraSchema
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.runner.RunWith
import org.scalatest._
import java.sql.Timestamp
import org.apache.spark.MeasurementValue
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EpidataContextAutomatedTestSpec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll with EmbeddedCassandra with SparkTemplate with Matchers {
  override def clearCache(): Unit = CassandraConnector.evictCache()

  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)

  private val cassandraKeyspaceName = "epidata_test"
  private val measurementClass = "automated_test"

  YamlTransformations.Default.addTransformation("cdc_raw_directory", "./raw_dir/")

  // Sets up CassandraConfig and SparkContext
  useCassandraConfig(Seq(YamlTransformations.Default))

  var conf = defaultConf
    .set("spark.epidata.cassandraKeyspaceName", cassandraKeyspaceName)
    .set("spark.epidata.measurementClass", measurementClass)

  useSparkConf(conf)

  println("sc: " + sc)

  val connector = CassandraConnector(conf)

  connector.withSessionDo { session =>
    session.execute(CassandraSchema.keyspaceCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.measurementsOriginalTableCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.measurementKeysTableCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.userTableCreation.format(cassandraKeyspaceName))

    println("sessions executed")
  }

  val beginTime = new Timestamp(1428004316123L)
  val endTime = new Timestamp(1428004316123L + 10000L)

  before {

    // Clear existing measurements.
    connector.withSessionDo { session =>
      session.execute(s"TRUNCATE epidata_test.${com.epidata.lib.models.Measurement.DBTableName}")
      session.execute(s"TRUNCATE epidata_test.${com.epidata.lib.models.MeasurementsKeys.DBTableName}")
    }

  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  "Double automated test" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-1",
      "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1")),
      beginTime,
      endTime)

    results.count should equal(1)
    val result = results.first.toSeq
    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("1000")
    result(3) should equal("Station-1")
    results.first.getAs[Timestamp](4).getTime should equal(ts.getTime)
    result(5) should equal("100001")
    result(6) should equal("Test-1")
    result(7) should equal("Meas-1")
    result(9) should equal(MeasurementValue(45.7))
    result(10) should equal("degree C")
    result(11) should equal("PASS")
    result(12) should equal(MeasurementValue(40.0))
    result(13) should equal(MeasurementValue(90.0))
    result(14) should equal("Description")
    result(15) should equal("PASS")
    result(16) should equal("PASS")
  }

  "Long automated test" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-1",
      "Meas-1", 45, "degree C", "PASS", 40, 90, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value_l", "meas_unit",
        "meas_status", "meas_lower_limit_l", "meas_upper_limit_l", "meas_description", "val1", "val2"))

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1")),
      beginTime,
      endTime)

    results.count should equal(1)
    val result = results.first.toSeq
    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("1000")
    result(3) should equal("Station-1")
    results.first.getAs[Timestamp](4).getTime should equal(ts.getTime)
    result(5) should equal("100001")
    result(6) should equal("Test-1")
    result(7) should equal("Meas-1")
    result(9) should equal(MeasurementValue(45))
    result(10) should equal("degree C")
    result(11) should equal("PASS")
    result(12) should equal(MeasurementValue(40))
    result(13) should equal(MeasurementValue(90))
    result(14) should equal("Description")
    result(15) should equal("PASS")
    result(16) should equal("PASS")
  }

  "String automated test" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-1",
      "Meas-1", "POWER ON", "PASS", "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value_s",
        "meas_status", "meas_description", "val1", "val2"))

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1")),
      beginTime,
      endTime)

    results.count should equal(1)
    val result = results.first.toSeq
    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("1000")
    result(3) should equal("Station-1")
    results.first.getAs[Timestamp](4).getTime should equal(ts.getTime)
    result(5) should equal("100001")
    result(6) should equal("Test-1")
    result(7) should equal("Meas-1")
    result(9) should equal(MeasurementValue("POWER ON"))
    results.first.isNullAt(10) should be(true)
    result(11) should equal("PASS")
    results.first.isNullAt(12) should be(true)
    results.first.isNullAt(13) should be(true)
    result(14) should equal("Description")
    result(15) should equal("PASS")
    result(16) should equal("PASS")
  }

  "Binary automated test" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)
    val data = Array[Byte](16, 1, 2, 3, 4)

    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-1",
      "Meas-1", data, "V", "PASS", "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value_b", "meas_unit",
        "meas_status", "meas_description", "val1", "val2"))

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1")),
      beginTime,
      endTime)

    results.count should equal(1)
    val result = results.first.toSeq
    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("1000")
    result(3) should equal("Station-1")
    results.first.getAs[Timestamp](4).getTime should equal(ts.getTime)
    result(5) should equal("100001")
    result(6) should equal("Test-1")
    result(7) should equal("Meas-1")
    result(10) should equal("V")
    result(11) should equal("PASS")
    results.first.isNullAt(12) should be(true)
    results.first.isNullAt(13) should be(true)
    result(14) should equal("Description")
    result(15) should equal("PASS")
    result(16) should equal("PASS")
  }

  "Automated test query" should "fail without required fields" in {

    val ec = new EpidataContext(sc)

    intercept[IllegalArgumentException] {
      ec.query(
        Map(
          "company" -> List("Company-1"),
          // Missing required field 'site'
          "device_group" -> List("1000"),
          "tester" -> List("Station-1")),
        beginTime,
        endTime)
    }

    intercept[IllegalArgumentException] {
      ec.query(
        Map(
          "company" -> List("Company-1"),
          "site" -> List("Site-1"),
          "device_groupWRONG" -> List("1000"), // Misspelled required field 'device_group'
          "tester" -> List("Station-1")),
        beginTime,
        endTime)
    }
  }

  "Automated test query" should "fail without values for query fields" in {

    val ec = new EpidataContext(sc)

    intercept[IllegalArgumentException] {
      ec.query(
        Map(
          "company" -> List("Company-1"),
          "site" -> List("Site-1"),
          "device_group" -> List("1000"),
          "tester" -> List()),
        beginTime,
        endTime)
    }

    intercept[IllegalArgumentException] {
      ec.query(
        Map(
          "company" -> List("Company-1"),
          "site" -> List("Site-1"),
          "device_group" -> List("1000"),
          "tester" -> List("Station-1"),
          "meas_status" -> List()),
        beginTime,
        endTime)
    }
  }

  "Automated test query" should "fail with unexpected fields" in {

    val ec = new EpidataContext(sc)

    intercept[IllegalArgumentException] {
      ec.query(
        Map(
          "company" -> List("Company-1"),
          "site" -> List("Site-1"),
          "device_group" -> List("1000"),
          "tester" -> List("Station-1"),
          "UNEXPECTED_FIELD" -> List("SOMEVALUE") // Unexpected field.
        ),
        beginTime,
        endTime)
    }
  }

  "Automated test query" should "filter results" in {

    val ec = new EpidataContext(sc)

    val ts1 = beginTime
    val ts2 = new Timestamp(beginTime.getTime + 2)
    val ts3 = new Timestamp(beginTime.getTime + 3)
    val ts4 = new Timestamp(beginTime.getTime + 4)
    val epoch = Measurement.epochForTs(beginTime)

    // First Measurement.
    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts1, "100001", "Test-1",
      "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    // Now a Site-2 Measurement.
    sc.parallelize(Seq(("Company-1", "Site-2", "1000", "Station-1", epoch, ts1, "100001", "Test-1",
      "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    // Now a Site-1 Measurement, using Meas-2.
    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts2, "100001", "Test-1",
      "Meas-2", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    // Another measurement, matching the same parameters as the original.
    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts3, "100001", "Test-1",
      "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    // A failed measurement.
    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts4, "100001", "Test-1",
      "Meas-1", 45.7, "degree C", "FAIL", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1"),
        "meas_name" -> List("Meas-1"),
        "meas_status" -> List("PASS")),
      beginTime,
      endTime)

    results.count should equal(2)
    results.first.toSeq(1) should equal("Site-1")
    results.first.toSeq(7) should equal("Meas-1")
    results.first.toSeq(11) should equal("PASS")
    results.collect()(1).toSeq(1) should equal("Site-1")
    results.collect()(1).toSeq(7) should equal("Meas-1")
    results.collect()(1).toSeq(11) should equal("PASS")
  }

  "Automated test query" should "query by set membership" in {

    val ec = new EpidataContext(sc)

    val ts1 = beginTime
    val ts2 = new Timestamp(beginTime.getTime + 2)
    val ts3 = new Timestamp(beginTime.getTime + 3)
    val ts4 = new Timestamp(beginTime.getTime + 4)
    val ts5 = new Timestamp(beginTime.getTime + 5)
    val epoch = Measurement.epochForTs(beginTime)

    // First Measurement.
    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1", epoch, ts1, "100001", "Test-1",
      "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    // Now a Site-2 Measurement.
    sc.parallelize(Seq(("Company-1", "Site-2", "1000", "Station-1", epoch, ts2, "100001", "Test-1",
      "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    // Now a Site-3 Measurement, using Meas-2.
    sc.parallelize(Seq(("Company-1", "Site-3", "1000", "Station-1", epoch, ts3, "100001", "Test-1",
      "Meas-2", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    // Another measurement, matching the same parameters as the original.
    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-2", epoch, ts4, "100001", "Test-1",
      "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    // A measurement in a different device group.
    sc.parallelize(Seq(("Company-1", "Site-1", "1050", "Station-2", epoch, ts5, "100001", "Test-1",
      "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    // A failed measurement.
    sc.parallelize(Seq(("Company-1", "Site-2", "1000", "Station-2", epoch, ts5, "100001", "Test-1",
      "Meas-1", 45.7, "degree C", "FAIL", 40.0, 90.0, "Description", "PASS", "PASS")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1", "Site-2"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1", "Station-2"),
        "meas_name" -> List("Meas-1"),
        "meas_status" -> List("PASS", "FAIL")),
      beginTime,
      endTime)

    results.count should equal(4)
    val sortedResults = results.collect().sortBy(_.getAs[Timestamp](4).getTime)
    sortedResults(0).toSeq(1) should equal("Site-1")
    sortedResults(1).toSeq(1) should equal("Site-2")
    sortedResults(2).toSeq(3) should equal("Station-2")
    sortedResults(3).toSeq(1) should equal("Site-2")
    sortedResults(3).toSeq(3) should equal("Station-2")
    sortedResults(3).toSeq(10) should equal("degree C")
  }

  "Automated test query" should "return results for two epochs" in {

    val ec = new EpidataContext(sc)

    def createMeasurement(ts: Timestamp) =
      sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1",
        Measurement.epochForTs(ts),
        ts, "100001", "Test-1", "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
        .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
          "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
          "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    val halfEpoch = 500L * 1000L * 1000L

    val ts = (0 to 3).map(x => new Timestamp(beginTime.getTime + halfEpoch * x))
    ts.slice(0, 3).reverse.foreach(createMeasurement)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1")),
      ts(0),
      ts(3)).collect()

    results.length should equal(3)

    results(0).getAs[Timestamp](4) should equal(ts(0))
    results(1).getAs[Timestamp](4) should equal(ts(1))
    results(2).getAs[Timestamp](4) should equal(ts(2))
  }

  "Automated test query" should "return results for three epochs" in {

    val ec = new EpidataContext(sc)

    def createMeasurement(ts: Timestamp) =
      sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1",
        Measurement.epochForTs(ts),
        ts, "100001", "Test-1", "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")))
        .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
          "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
          "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    val halfEpoch = 500L * 1000L * 1000L

    val ts = (0 to 5).map(x => new Timestamp(beginTime.getTime + halfEpoch * x))
    ts.slice(0, 5).reverse.foreach(createMeasurement)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Station-1")),
      ts(0),
      ts(5)).collect()

    results.length should equal(5)

    results(0).getAs[Timestamp](4) should equal(ts(0))
    results(1).getAs[Timestamp](4) should equal(ts(1))
    results(2).getAs[Timestamp](4) should equal(ts(2))
    results(3).getAs[Timestamp](4) should equal(ts(3))
    results(4).getAs[Timestamp](4) should equal(ts(4))
  }

  "Measurement keys" should "be returned" in {
    val ec = new EpidataContext(sc)

    sc.parallelize(Seq(("Company-1", "Site-1", "1000", "Station-1")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.MeasurementsKeys.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset"))
    sc.parallelize(Seq(("Company-2", "Site-1", "1000", "Station-1")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.MeasurementsKeys.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset"))
    sc.parallelize(Seq(("Company-3", "Site-3", "3000", "Station-3")))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.MeasurementsKeys.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset"))

    val results = ec.listKeys().collect()

    results.length should equal(3)
    results(0).toSeq(0) should equal("Company-1")
    results(1).toSeq(0) should equal("Company-2")
    results(2).toSeq(0) should equal("Company-3")
    results(2).toSeq(1) should equal("Site-3")
  }

}

@RunWith(classOf[JUnitRunner])
class EpidataContextSensorMeasurementSpec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll with EmbeddedCassandra with SparkTemplate with Matchers {
  override def clearCache(): Unit = CassandraConnector.evictCache()

  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)

  private val master = "local[2]"
  private val appName = "EpidataContextSensorMeasurementSpec"

  private val cassandraConnectionHost = "127.0.0.1"
  private val cassandraKeyspaceName = "epidata_test"
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
    session.execute(CassandraSchema.measurementKeysTableCreation.format(cassandraKeyspaceName))
    session.execute(CassandraSchema.userTableCreation.format(cassandraKeyspaceName))
  }

  val beginTime = new Timestamp(1428004316123L)
  val endTime = new Timestamp(1428004316123L + 10000L)

  before {

    // Clear existing measurements.
    connector.withSessionDo { session =>
      session.execute(s"TRUNCATE epidata_test.${com.epidata.lib.models.Measurement.DBTableName}")
      session.execute(s"TRUNCATE epidata_test.${com.epidata.lib.models.MeasurementsKeys.DBTableName}")
    }

  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  "Double sensor measurement" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    sc.parallelize(Seq(("Company-1", "Site-1", "Station-1", "Sensor-1", epoch, ts, "Event-1", "Meas-1",
      "", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", null, null)))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "station" -> List("Station-1"),
        "sensor" -> List("Sensor-1")),
      beginTime,
      endTime)

    results.count should equal(1)
    val result = results.first.toSeq
    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("Station-1")
    result(3) should equal("Sensor-1")
    results.first.getAs[Timestamp](4).getTime should equal(ts.getTime)
    result(5) should equal("Event-1")
    result(6) should equal("Meas-1")
    result(8) should equal(MeasurementValue(45.7))
    result(9) should equal("degree C")
    result(10) should equal("PASS")
    result(11) should equal(MeasurementValue(40.0))
    result(12) should equal(MeasurementValue(90.0))
    result(13) should equal("Description")
  }

  "Long sensor measurement" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    sc.parallelize(Seq(("Company-1", "Site-1", "Station-1", "Sensor-1", epoch, ts, "Event-1", "Meas-1",
      "", 45, "degree C", "PASS", 40, 90, "Description", null, null)))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value_l", "meas_unit",
        "meas_status", "meas_lower_limit_l", "meas_upper_limit_l", "meas_description", "val1", "val2"))

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "station" -> List("Station-1"),
        "sensor" -> List("Sensor-1")),
      beginTime,
      endTime)

    results.count should equal(1)
    val result = results.first.toSeq
    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("Station-1")
    result(3) should equal("Sensor-1")
    results.first.getAs[Timestamp](4).getTime should equal(ts.getTime)
    result(5) should equal("Event-1")
    result(6) should equal("Meas-1")
    result(8) should equal(MeasurementValue(45))
    result(9) should equal("degree C")
    result(10) should equal("PASS")
    result(11) should equal(MeasurementValue(40))
    result(12) should equal(MeasurementValue(90))
    result(13) should equal("Description")
  }

  "Large long sensor measurement" should "be returned" in {

    val large = 3448388841L

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    sc.parallelize(Seq(("Company-1", "Site-1", "Station-1", "Sensor-1", epoch, ts, "Event-1", "Meas-1",
      "", large, "ns", "PASS", large - 9, large + 10, "Description", null, null)))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value_l", "meas_unit",
        "meas_status", "meas_lower_limit_l", "meas_upper_limit_l", "meas_description", "val1", "val2"))

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "station" -> List("Station-1"),
        "sensor" -> List("Sensor-1")),
      beginTime,
      endTime)

    results.count should equal(1)
    val result = results.first.toSeq
    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("Station-1")
    result(3) should equal("Sensor-1")
    results.first.getAs[Timestamp](4).getTime should equal(ts.getTime)
    result(5) should equal("Event-1")
    result(6) should equal("Meas-1")
    result(8) should equal(MeasurementValue(large))
    result(9) should equal("ns")
    result(10) should equal("PASS")
    result(11) should equal(MeasurementValue(large - 9))
    result(12) should equal(MeasurementValue(large + 10))
    result(13) should equal("Description")
  }

  "String sensor measurement" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    sc.parallelize(Seq(("Company-1", "Site-1", "Station-1", "Sensor-1", epoch, ts, "Event-1", "Meas-1",
      "", "STRING VALUE", null, "PASS", null, null, "Description", null, null)))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value_s", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "station" -> List("Station-1"),
        "sensor" -> List("Sensor-1")),
      beginTime,
      endTime)

    results.count should equal(1)
    val result = results.first.toSeq
    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("Station-1")
    result(3) should equal("Sensor-1")
    results.first.getAs[Timestamp](4).getTime should equal(ts.getTime)
    result(5) should equal("Event-1")
    result(6) should equal("Meas-1")
    result(8) should equal(MeasurementValue("STRING VALUE"))
    results.first.isNullAt(9) should be(true)
    result(10) should equal("PASS")
    results.first.isNullAt(11) should be(true)
    results.first.isNullAt(12) should be(true)
    result(13) should equal("Description")
  }

  "Binary sensor measurement" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)
    val data = Array[Byte](16, 1, 2, 3, 4)

    sc.parallelize(Seq(("Company-1", "Site-1", "Station-1", "Sensor-1", epoch, ts, "Event-1", "Meas-1",
      "", data, "V", "PASS", null, null, "Description", null, null)))
      .saveToCassandra(cassandraKeyspaceName, com.epidata.lib.models.Measurement.DBTableName, SomeColumns("customer", "customer_site",
        "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value_b", "meas_unit",
        "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2"))

    val ec = new EpidataContext(sc)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "station" -> List("Station-1"),
        "sensor" -> List("Sensor-1")),
      beginTime,
      endTime)

    results.count should equal(1)
    val result = results.first.toSeq
    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("Station-1")
    result(3) should equal("Sensor-1")
    results.first.getAs[Timestamp](4).getTime should equal(ts.getTime)
    result(5) should equal("Event-1")
    result(6) should equal("Meas-1")
    results.first.getAs[MeasurementValue](8).value.asInstanceOf[Array[Byte]] should equal(data)
    result(9) should equal("V")
    result(10) should equal("PASS")
    results.first.isNullAt(11) should be(true)
    results.first.isNullAt(12) should be(true)
    result(13) should equal("Description")
  }

  "Sensor measurement query" should "fail without required fields" in {

    val ec = new EpidataContext(sc)

    intercept[IllegalArgumentException] {
      ec.query(
        Map(
          "company" -> List("Company-1"),
          "site" -> List("Site-1"),
          "station" -> List("Station-1"),
          "tester" -> List("Sensor-1") // Missing required field 'sensor'.
        ),
        beginTime,
        endTime)
    }
  }

}
