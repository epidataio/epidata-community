/*
 * Copyright (c) 2015-2021 EpiData, Inc.
*/

/*
package com.epidata.spark

import java.sql.{ Connection, DriverManager, ResultSet, SQLException, Statement, Timestamp }
import com.epidata.lib.models.{ Measurement => BaseMeasurement, MeasurementCleansed => BaseMeasurementCleansed, MeasurementsKeys => BaseMeasurementsKeys }
import com.epidata.lib.models.{ MeasurementSummary, AutomatedTest => BaseAutomatedTest, SensorMeasurement => BaseSensorMeasurement }
import com.epidata.lib.models.util.{ Binary, Datatype }
import org.apache.spark.MeasurementValue
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import com.epidata.specs.SQLiteSchema
import scala.collection.JavaConversions._
import scala.collection.JavaConversions
import java.util.stream.Collectors
import javax.xml.bind.DatatypeConverter
import org.apache.log4j.{ Level, Logger }
import org.junit.runner.RunWith
import org.scalatest._
import java.sql.Timestamp
import org.scalatestplus.junit.JUnitRunner
import java.nio.ByteBuffer
import java.io.File

@RunWith(classOf[JUnitRunner])
class EpidataLiteContextAutomatedTestSpec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll with Matchers {
  //  override def clearCache(): Unit = CassandraConnector.evictCache()

  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)

  private val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  private val basePath = new java.io.File(".").getAbsoluteFile().getParentFile().getParent()

  //  private val sqliteDBName = conf.getString("spark.epidata.SQLite.test.dbFileName")
  private val sqliteDBName = conf.getString("spark.epidata.SQLite.dbFileName")
  private val sqliteDBUrl = "jdbc:sqlite:" + basePath + "/data/" + sqliteDBName

  //  private val con: Connection = DriverManager.getConnection(dbUrl)

  //  val path = new java.io.File(".").getAbsoluteFile()
  //  private val sqliteDatabaseUrl = "jdbc:sqlite:" + path.getParentFile().getParent() + "/data/epidata_test.db"

  private val measurementClass = "automated_test"

  //  val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  //    .withValue(
  //      "spark.epidata.SQLite.url",
  //      ConfigValueFactory.fromAnyRef(sqliteDatabaseUrl))
  //    .withValue(
  //      "spark.epidata.measurementClass",
  //      ConfigValueFactory.fromAnyRef(measurementClass))

  private val connector: Connection = DriverManager.getConnection(sqliteDBUrl)

  connector.createStatement().execute(SQLiteSchema.measurementsOriginalTableCreation)
  connector.createStatement().execute(SQLiteSchema.measurementsCleansedTableCreation)
  connector.createStatement().execute(SQLiteSchema.measurementsSummaryTableCreation)
  connector.createStatement().execute(SQLiteSchema.measurementKeysTableCreation)
  connector.createStatement().execute(SQLiteSchema.userTableCreation)

  val beginTime = new Timestamp(1619240032000L)
  val endTime = new Timestamp(1619240032000L + 10000L)

  before {
    // Clear existing measurements.
    connector.createStatement().execute(s"DELETE from ${com.epidata.lib.models.Measurement.DBTableName}")
    connector.createStatement().execute(s"DELETE from ${com.epidata.lib.models.MeasurementCleansed.DBTableName}")
    connector.createStatement().execute(s"DELETE from ${com.epidata.lib.models.MeasurementSummary.DBTableName}")
    connector.createStatement().execute(s"DELETE from ${com.epidata.lib.models.MeasurementsKeys.DBTableName}")
    connector.createStatement().execute(s"DELETE from users")
  }

  override def afterAll(): Unit = {
    if (connector != null) {
      connector.close()
    }
  }

  "Double automated test" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(ts)

    val insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
 customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
 meas_datatype, meas_value, meas_unit, meas_status, meas_lower_limit, meas_upper_limit, meas_description)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    val prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 45.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")

    prepare_insert.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Tester-1")),
      beginTime,
      endTime)

    results.size() should equal(1)
    val result = results.head.values().stream().collect(Collectors.toList())

    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("1000")
    result(3) should equal("Tester-1")
    result(4) should equal(ts.getTime)
    result(5) should equal("100001")
    result(6) should equal("Test-1")
    result(7) should equal("Meas-1")
    result(8) should equal("double")
    result(9) should equal(45.7)
    result(10) should equal("degree C")
    result(11) should equal("PASS")
    result(12) should equal(40.0)
    result(13) should equal(90.0)
    result(14) should equal("Description")

  }

  "Long automated test" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    val insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value_l, meas_unit, meas_status, meas_lower_limit_l, meas_upper_limit_l, meas_description)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    val prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "integer")
    prepare_insert.setLong(11, 45)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setLong(14, 40)
    prepare_insert.setLong(15, 90)
    prepare_insert.setString(16, "Description")

    prepare_insert.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Tester-1")),
      beginTime,
      endTime)

    results.size() should equal(1)
    val result = results.head.values().stream().collect(Collectors.toList())

    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("1000")
    result(3) should equal("Tester-1")
    result(4) should equal(ts.getTime)
    result(5) should equal("100001")
    result(6) should equal("Test-1")
    result(7) should equal("Meas-1")
    result(8) should equal("integer")
    result(9) should equal(45)
    result(10) should equal("degree C")
    result(11) should equal("PASS")
    result(12) should equal(40)
    result(13) should equal(90)
    result(14) should equal("Description")

  }

  "String automated test" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    val insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value_s, meas_status, meas_description)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    val prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "string")
    prepare_insert.setString(11, "POWER ON")
    prepare_insert.setString(12, "PASS")
    prepare_insert.setString(13, "Description")

    prepare_insert.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Tester-1")),
      beginTime,
      endTime)

    results.size() should equal(1)
    val result = results.head.values().stream().collect(Collectors.toList())

    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("1000")
    result(3) should equal("Tester-1")
    result(4) should equal(ts.getTime)
    result(5) should equal("100001")
    result(6) should equal("Test-1")
    result(7) should equal("Meas-1")
    result(8) should equal("string")
    result(9) should equal("POWER ON")
    result(10) should equal("PASS")
    result(11) should equal("Description")

  }

  "Binary automated test" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    val dataBytes = Array[Long](16, 1, 2, 3, 4).map(_.toByte)
    val data = ByteBuffer.wrap(dataBytes)
    val binary = Binary.fromBase64(Datatype.Long, DatatypeConverter.printBase64Binary(data.array()))

    val insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value_b, meas_unit, meas_status, meas_description)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    val prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "array")
    prepare_insert.setBytes(11, binary.backing)
    prepare_insert.setString(12, "V")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setString(14, "Description")

    prepare_insert.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Tester-1")),
      beginTime,
      endTime)

    results.size() should equal(1)
    val result = results.head.values().stream().collect(Collectors.toList())
    println("result: " + result)

    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("1000")
    result(3) should equal("Tester-1")
    result(4) should equal(ts.getTime)
    result(5) should equal("100001")
    result(6) should equal("Test-1")
    result(7) should equal("Meas-1")
    result(8) should equal("array")
    //    result(9).asInstanceOf[Array[Byte]] should equal(binary.backing)
    result(10) should equal("V")
    result(11) should equal("PASS")
    result(12) should equal("Description")

  }

  "Automated test query" should "fail without required fields" in {

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    intercept[IllegalArgumentException] {
      ec.query(
        Map(
          "company" -> List("Company-1"),
          // Missing required field 'site'
          "device_group" -> List("1000"),
          "tester" -> List("Tester-1")),
        beginTime,
        endTime)
    }

    intercept[IllegalArgumentException] {
      ec.query(
        Map(
          "company" -> List("Company-1"),
          "site" -> List("Site-1"),
          "device_groupWRONG" -> List("1000"), // Misspelled required field 'device_group'
          "tester" -> List("Tester-1")),
        beginTime,
        endTime)
    }
  }

  "Automated test query" should "fail without values for query fields" in {

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

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
          "tester" -> List("Tester-1"),
          "meas_status" -> List()),
        beginTime,
        endTime)
    }
  }

  "Automated test query" should "fail with unexpected fields" in {

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    intercept[IllegalArgumentException] {
      ec.query(
        Map(
          "company" -> List("Company-1"),
          "site" -> List("Site-1"),
          "device_group" -> List("1000"),
          "tester" -> List("Tester-1"),
          "UNEXPECTED_FIELD" -> List("SOMEVALUE") // Unexpected field.
        ),
        beginTime,
        endTime)
    }
  }

  "Automated test query" should "filter results" in {

    val ts1 = beginTime
    val ts2 = new Timestamp(beginTime.getTime + 2)
    val ts3 = new Timestamp(beginTime.getTime + 3)
    val ts4 = new Timestamp(beginTime.getTime + 4)
    val epoch = Measurement.epochForTs(beginTime)

    // First Measurement.
    var insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value, meas_unit, meas_status, meas_lower_limit, meas_upper_limit, meas_description)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    var prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts1.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 45.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")

    prepare_insert.executeUpdate()

    // Now a Site-2 Measurement.
    insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value, meas_unit, meas_status, meas_lower_limit, meas_upper_limit, meas_description)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-2")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts1.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 45.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")

    prepare_insert.executeUpdate()

    // Now a Site-1 Measurement, using Meas-2.
    insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value, meas_unit, meas_status, meas_lower_limit, meas_upper_limit, meas_description)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts2.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-2")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 45.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")

    prepare_insert.executeUpdate()

    // Another measurement, matching the same parameters as the original.
    insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value, meas_unit, meas_status, meas_lower_limit, meas_upper_limit, meas_description)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts3.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 45.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")

    prepare_insert.executeUpdate()

    // A failed measurement with meas_value below meas_lower_limit
    insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value, meas_unit, meas_status, meas_lower_limit, meas_upper_limit, meas_description)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts4.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 35.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "FAIL")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")

    prepare_insert.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Tester-1"),
        "meas_name" -> List("Meas-1"),
        "meas_status" -> List("PASS")),
      beginTime,
      endTime)

    results.size() should equal(2)
    val firstResult = results.head.values().stream().collect(Collectors.toList())
    val secondResult = results(1).values().stream().collect(Collectors.toList())

    firstResult(1) should equal("Site-1")
    firstResult(7) should equal("Meas-1")
    firstResult(11) should equal("PASS")

    secondResult(1) should equal("Site-1")
    secondResult(7) should equal("Meas-1")
    secondResult(11) should equal("PASS")
  }

  "Automated test query" should "query by set membership" in {

    val ts1 = beginTime
    val ts2 = new Timestamp(beginTime.getTime + 2)
    val ts3 = new Timestamp(beginTime.getTime + 3)
    val ts4 = new Timestamp(beginTime.getTime + 4)
    val ts5 = new Timestamp(beginTime.getTime + 5)
    val ts6 = new Timestamp(beginTime.getTime + 6)
    val epoch = Measurement.epochForTs(beginTime)

    // First Measurement.
    var insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value, meas_unit, meas_status, meas_lower_limit, meas_upper_limit, meas_description, val1, val2)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    var prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts1.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 45.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")
    prepare_insert.setString(17, "PASS")
    prepare_insert.setString(18, "PASS")

    prepare_insert.executeUpdate()

    // Now a Site-2 Measurement
    prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-2")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts2.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 45.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")
    prepare_insert.setString(17, "PASS")
    prepare_insert.setString(18, "PASS")

    prepare_insert.executeUpdate()

    // Now a Site-3 Measurement, using Meas-2.
    prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-3")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts3.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-2")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 4.98)
    prepare_insert.setString(12, "V")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setDouble(14, 3.3)
    prepare_insert.setDouble(15, 5.5)
    prepare_insert.setString(16, "Description")
    prepare_insert.setString(17, "PASS")
    prepare_insert.setString(18, "PASS")

    prepare_insert.executeUpdate()

    // Another measurement with Tester-2
    prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-2")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts4.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 45.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")
    prepare_insert.setString(17, "PASS")
    prepare_insert.setString(18, "PASS")

    prepare_insert.executeUpdate()

    // A measurement in a different device group, matching the same parameters as the original
    prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "1050")
    prepare_insert.setString(4, "Tester-2")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts5.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 45.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")
    prepare_insert.setString(17, "PASS")
    prepare_insert.setString(18, "PASS")

    prepare_insert.executeUpdate()

    // A failed measurement
    prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-2")
    prepare_insert.setString(3, "1000")
    prepare_insert.setString(4, "Tester-2")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts6.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "100001")
    prepare_insert.setString(8, "Test-1")
    prepare_insert.setString(9, "Meas-1")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 38.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "FAIL")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")
    prepare_insert.setString(17, "PASS")
    prepare_insert.setString(18, "PASS")

    prepare_insert.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1", "Site-2"),
        "device_group" -> List("1000"),
        "tester" -> List("Tester-1", "Tester-2"),
        "meas_name" -> List("Meas-1", "Meas-2"),
        "meas_status" -> List("PASS", "FAIL")),
      beginTime,
      endTime)

    results.size() should equal(4)
    val sortedResults = results.stream().collect(Collectors.toList()).sortBy(_("ts").asInstanceOf[Long])

    sortedResults(0).get("site") should equal("Site-1")
    sortedResults(1).get("site") should equal("Site-2")
    sortedResults(2).get("tester") should equal("Tester-2")
    sortedResults(3).get("site") should equal("Site-2")
    sortedResults(3).get("tester") should equal("Tester-2")
    sortedResults(3).get("meas_unit") should equal("degree C")

  }

  "Automated test query" should "return results for two epochs" in {

    // Create Measurement method
    def createMeasurement(ts: Timestamp) = {
      var insert_q =
        s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value, meas_unit, meas_status, meas_lower_limit, meas_upper_limit, meas_description, val1, val2)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

      var prepare_insert = connector.prepareStatement(insert_q.toString)
      prepare_insert.setString(1, "Company-1")
      prepare_insert.setString(2, "Site-1")
      prepare_insert.setString(3, "1000")
      prepare_insert.setString(4, "Tester-1")
      prepare_insert.setInt(5, Measurement.epochForTs(ts).asInstanceOf[Integer])
      prepare_insert.setTimestamp(6, ts.asInstanceOf[Timestamp])
      prepare_insert.setString(7, "100001")
      prepare_insert.setString(8, "Test-1")
      prepare_insert.setString(9, "Meas-1")
      prepare_insert.setString(10, "double")
      prepare_insert.setDouble(11, 45.7)
      prepare_insert.setString(12, "degree C")
      prepare_insert.setString(13, "PASS")
      prepare_insert.setDouble(14, 40.0)
      prepare_insert.setDouble(15, 90.0)
      prepare_insert.setString(16, "Description")
      prepare_insert.setString(17, "PASS")
      prepare_insert.setString(18, "PASS")

      prepare_insert.executeUpdate()
    }

    val halfEpoch = 500L * 1000L * 1000L
    val ts = (0 to 3).map(x => new Timestamp(beginTime.getTime + halfEpoch * x))

    ts.slice(0, 3).reverse.foreach(createMeasurement)

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Tester-1")),
      ts(0),
      ts(3))

    results.size() should equal(3)
    val firstResult = results.head.values().stream().collect(Collectors.toList())
    val secondResult = results(1).values().stream().collect(Collectors.toList())
    val thirdResult = results(2).values().stream().collect(Collectors.toList())

    firstResult(4) should equal(ts(0).getTime)
    secondResult(4) should equal(ts(1).getTime)
    thirdResult(4) should equal(ts(2).getTime)
  }

  "Automated test query" should "return results for three epochs" in {

    // Create Measurement method
    def createMeasurement(ts: Timestamp) = {
      var insert_q =
        s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value, meas_unit, meas_status, meas_lower_limit, meas_upper_limit, meas_description, val1, val2)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

      var prepare_insert = connector.prepareStatement(insert_q.toString)
      prepare_insert.setString(1, "Company-1")
      prepare_insert.setString(2, "Site-1")
      prepare_insert.setString(3, "1000")
      prepare_insert.setString(4, "Tester-1")
      prepare_insert.setInt(5, Measurement.epochForTs(ts).asInstanceOf[Integer])
      prepare_insert.setTimestamp(6, ts.asInstanceOf[Timestamp])
      prepare_insert.setString(7, "100001")
      prepare_insert.setString(8, "Test-1")
      prepare_insert.setString(9, "Meas-1")
      prepare_insert.setString(10, "double")
      prepare_insert.setDouble(11, 45.7)
      prepare_insert.setString(12, "degree C")
      prepare_insert.setString(13, "PASS")
      prepare_insert.setDouble(14, 40.0)
      prepare_insert.setDouble(15, 90.0)
      prepare_insert.setString(16, "Description")
      prepare_insert.setString(17, "PASS")
      prepare_insert.setString(18, "PASS")

      prepare_insert.executeUpdate()
    }

    val halfEpoch = 500L * 1000L * 1000L

    val ts = (0 to 5).map(x => new Timestamp(beginTime.getTime + halfEpoch * x))
    ts.slice(0, 5).reverse.foreach(createMeasurement)

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "device_group" -> List("1000"),
        "tester" -> List("Tester-1")),
      ts(0),
      ts(5))

    results.size() should equal(5)
    val firstResult = results.head.values().stream().collect(Collectors.toList())
    val secondResult = results(1).values().stream().collect(Collectors.toList())
    val thirdResult = results(2).values().stream().collect(Collectors.toList())
    val fourthResult = results(3).values().stream().collect(Collectors.toList())
    val fifthResult = results(4).values().stream().collect(Collectors.toList())

    firstResult(4) should equal(ts(0).getTime)
    secondResult(4) should equal(ts(1).getTime)
    thirdResult(4) should equal(ts(2).getTime)
    fourthResult(4) should equal(ts(3).getTime)
    fifthResult(4) should equal(ts(4).getTime)
  }

  "Measurement keys" should "be returned" in {

    val insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (
customer, customer_site, collection, dataset)
VALUES (?, ?, ?, ?)"""

    val prepare_insert_1 = connector.prepareStatement(insert_q.toString)
    prepare_insert_1.setString(1, "Company-1")
    prepare_insert_1.setString(2, "Site-1")
    prepare_insert_1.setString(3, "1000")
    prepare_insert_1.setString(4, "Tester-1")

    prepare_insert_1.executeUpdate()

    val prepare_insert_2 = connector.prepareStatement(insert_q.toString)
    prepare_insert_2.setString(1, "Company-2")
    prepare_insert_2.setString(2, "Site-1")
    prepare_insert_2.setString(3, "1000")
    prepare_insert_2.setString(4, "Tester-1")

    prepare_insert_2.executeUpdate()

    val prepare_insert_3 = connector.prepareStatement(insert_q.toString)
    prepare_insert_3.setString(1, "Company-3")
    prepare_insert_3.setString(2, "Site-3")
    prepare_insert_3.setString(3, "1000")
    prepare_insert_3.setString(4, "Tester-3")

    prepare_insert_3.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.listKeys()

    results.size() should equal(3)
    val firstResult = results.head.values().stream().collect(Collectors.toList())
    val secondResult = results(1).values().stream().collect(Collectors.toList())
    val thirdResult = results(2).values().stream().collect(Collectors.toList())

    firstResult(0) should equal("Company-1")
    secondResult(0) should equal("Company-2")
    thirdResult(0) should equal("Company-3")
    thirdResult(1) should equal("Site-3")
    thirdResult(3) should equal("Tester-3")

  }

}

@RunWith(classOf[JUnitRunner])
class EpidataLiteContextSensorMeasurementSpec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll with Matchers {
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)

  private val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  private val basePath = new java.io.File(".").getAbsoluteFile().getParentFile().getParent()

  //  private val sqliteDBName = conf.getString("spark.epidata.SQLite.test.dbFileName")
  private val sqliteDBName = conf.getString("spark.epidata.SQLite.dbFileName")
  private val sqliteDBUrl = "jdbc:sqlite:" + basePath + "/data/" + sqliteDBName

  private val appName = "EpidataLiteContextSensorMeasurementSpec"
  private val measurementClass = "sensor_measurement"

  private val connector: Connection = DriverManager.getConnection(sqliteDBUrl)

  connector.createStatement().execute(SQLiteSchema.measurementsOriginalTableCreation)
  connector.createStatement().execute(SQLiteSchema.measurementsCleansedTableCreation)
  connector.createStatement().execute(SQLiteSchema.measurementsSummaryTableCreation)
  connector.createStatement().execute(SQLiteSchema.measurementKeysTableCreation)
  connector.createStatement().execute(SQLiteSchema.userTableCreation)

  val beginTime = new Timestamp(1619240032000L)
  val endTime = new Timestamp(1619240032000L + 10000L)

  before {
    // Clear existing measurements.
    connector.createStatement().execute(s"DELETE from ${com.epidata.lib.models.Measurement.DBTableName}")
    connector.createStatement().execute(s"DELETE from ${com.epidata.lib.models.MeasurementCleansed.DBTableName}")
    connector.createStatement().execute(s"DELETE from ${com.epidata.lib.models.MeasurementSummary.DBTableName}")
    connector.createStatement().execute(s"DELETE from ${com.epidata.lib.models.MeasurementsKeys.DBTableName}")
    connector.createStatement().execute(s"DELETE from users")
  }

  override def afterAll(): Unit = {
    if (connector != null) {
      connector.close()
    }
  }

  "Double sensor measurement" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    val insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value, meas_unit, meas_status, meas_lower_limit, meas_upper_limit, meas_description, "val1", "val2")
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    val prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "Station-1")
    prepare_insert.setString(4, "Sensor-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "Event-1")
    prepare_insert.setString(8, "Meas-1")
    prepare_insert.setString(9, "")
    prepare_insert.setString(10, "double")
    prepare_insert.setDouble(11, 45.7)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setDouble(14, 40.0)
    prepare_insert.setDouble(15, 90.0)
    prepare_insert.setString(16, "Description")
    prepare_insert.setString(17, null)
    prepare_insert.setString(18, null)

    prepare_insert.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "station" -> List("Station-1"),
        "sensor" -> List("Sensor-1")),
      beginTime,
      endTime)

    results.size() should equal(1)
    val result = results.head.values().stream().collect(Collectors.toList())

    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("Station-1")
    result(3) should equal("Sensor-1")
    result(4) should equal(ts.getTime)
    result(5) should equal("Event-1")
    result(6) should equal("Meas-1")
    result(7) should equal("double")
    result(8) should equal(45.7)
    result(9) should equal("degree C")
    result(10) should equal("PASS")
    result(11) should equal(40.0)
    result(12) should equal(90.0)
    result(13) should equal("Description")

  }

  "Long sensor measurement" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    val insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value_l, meas_unit, meas_status, meas_lower_limit_l, meas_upper_limit_l, meas_description, "val1", "val2")
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    val prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "Station-1")
    prepare_insert.setString(4, "Sensor-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "Event-1")
    prepare_insert.setString(8, "Meas-1")
    prepare_insert.setString(9, "")
    prepare_insert.setString(10, "integer")
    prepare_insert.setLong(11, 45)
    prepare_insert.setString(12, "degree C")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setLong(14, 40)
    prepare_insert.setLong(15, 90)
    prepare_insert.setString(16, "Description")
    prepare_insert.setString(17, null)
    prepare_insert.setString(18, null)

    prepare_insert.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "station" -> List("Station-1"),
        "sensor" -> List("Sensor-1")),
      beginTime,
      endTime)

    results.size() should equal(1)
    val result = results.head.values().stream().collect(Collectors.toList())

    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("Station-1")
    result(3) should equal("Sensor-1")
    result(4) should equal(ts.getTime)
    result(5) should equal("Event-1")
    result(6) should equal("Meas-1")
    result(7) should equal("integer")
    result(8) should equal(45)
    result(9) should equal("degree C")
    result(10) should equal("PASS")
    result(11) should equal(40)
    result(12) should equal(90)
    result(13) should equal("Description")

  }

  "Large long sensor measurement" should "be returned" in {

    val large = 3448388841L

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    val insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value_l, meas_unit, meas_status, meas_lower_limit_l, meas_upper_limit_l, meas_description, "val1", "val2")
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    val prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "Station-1")
    prepare_insert.setString(4, "Sensor-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "Event-1")
    prepare_insert.setString(8, "Meas-1")
    prepare_insert.setString(9, "")
    prepare_insert.setString(10, "long")
    prepare_insert.setLong(11, large)
    prepare_insert.setString(12, "ns")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setLong(14, (large - 10))
    prepare_insert.setLong(15, (large + 10))
    prepare_insert.setString(16, "Description")
    prepare_insert.setString(17, null)
    prepare_insert.setString(18, null)

    prepare_insert.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "station" -> List("Station-1"),
        "sensor" -> List("Sensor-1")),
      beginTime,
      endTime)

    results.size() should equal(1)
    val result = results.head.values().stream().collect(Collectors.toList())

    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("Station-1")
    result(3) should equal("Sensor-1")
    result(4) should equal(ts.getTime)
    result(5) should equal("Event-1")
    result(6) should equal("Meas-1")
    result(7) should equal("long")
    result(8) should equal(large)
    result(9) should equal("ns")
    result(10) should equal("PASS")
    result(11) should equal(large - 10)
    result(12) should equal(large + 10)
    result(13) should equal("Description")
  }

  "String sensor measurement" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    val insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value_s, meas_unit, meas_status, meas_description, "val1", "val2")
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    val prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "Station-1")
    prepare_insert.setString(4, "Sensor-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "Event-1")
    prepare_insert.setString(8, "Meas-1")
    prepare_insert.setString(9, "")
    prepare_insert.setString(10, "String")
    prepare_insert.setString(11, "STRING VALUE")
    prepare_insert.setString(12, null)
    prepare_insert.setString(13, "PASS")
    prepare_insert.setString(14, "Description")
    prepare_insert.setString(15, null)
    prepare_insert.setString(16, null)

    prepare_insert.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "station" -> List("Station-1"),
        "sensor" -> List("Sensor-1")),
      beginTime,
      endTime)

    results.size() should equal(1)
    val result = results.head.values().stream().collect(Collectors.toList())

    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("Station-1")
    result(3) should equal("Sensor-1")
    result(4) should equal(ts.getTime)
    result(5) should equal("Event-1")
    result(6) should equal("Meas-1")
    result(7) should equal("String")
    result(8) should equal("STRING VALUE")
    result(9) should equal("PASS")
    result(10) should equal("Description")

  }

  "Binary sensor measurement" should "be returned" in {

    val ts = beginTime
    val epoch = Measurement.epochForTs(beginTime)

    val dataBytes = Array[Long](16, 1, 2, 3, 4).map(_.toByte)
    val data = ByteBuffer.wrap(dataBytes)
    val binary = Binary.fromBase64(Datatype.Long, DatatypeConverter.printBase64Binary(data.array()))

    val insert_q =
      s"""INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3,
meas_datatype, meas_value_b, meas_unit, meas_status, meas_description, "val1", "val2")
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    val prepare_insert = connector.prepareStatement(insert_q.toString)
    prepare_insert.setString(1, "Company-1")
    prepare_insert.setString(2, "Site-1")
    prepare_insert.setString(3, "Station-1")
    prepare_insert.setString(4, "Sensor-1")
    prepare_insert.setInt(5, epoch.asInstanceOf[Integer])
    prepare_insert.setTimestamp(6, ts.asInstanceOf[Timestamp])
    prepare_insert.setString(7, "Event-1")
    prepare_insert.setString(8, "Meas-1")
    prepare_insert.setString(9, "")
    prepare_insert.setString(10, "array")
    prepare_insert.setBytes(11, binary.backing)
    prepare_insert.setString(12, "V")
    prepare_insert.setString(13, "PASS")
    prepare_insert.setString(14, "Description")
    prepare_insert.setString(15, null)
    prepare_insert.setString(16, null)

    prepare_insert.executeUpdate()

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

    val results = ec.query(
      Map(
        "company" -> List("Company-1"),
        "site" -> List("Site-1"),
        "station" -> List("Station-1"),
        "sensor" -> List("Sensor-1")),
      beginTime,
      endTime)

    results.size() should equal(1)
    val result = results.head.values().stream().collect(Collectors.toList())
    println("binary sensor measurement result: " + result)

    result(0) should equal("Company-1")
    result(1) should equal("Site-1")
    result(2) should equal("Station-1")
    result(3) should equal("Sensor-1")
    result(4) should equal(ts.getTime)
    result(5) should equal("Event-1")
    result(6) should equal("Meas-1")
    result(7) should equal("array")
    //    result(8).asInstanceOf[Array[Byte]] should equal(binary.backing)
    result(9) should equal("V")
    result(10) should equal("PASS")
    result(11) should equal("Description")
  }

  "Sensor measurement query" should "fail without required fields" in {

    val m = EpiDataConf(measurementClass, sqliteDBUrl)
    // println("model conf: " + m.model)
    // println("db url: " + m.dbUrl)

    val ec = new EpidataLiteContext(m)

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
*/
