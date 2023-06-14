/*
 * Copyright (c) 2015-2021 EpiData, Inc.
*/

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
import java.nio.file.Paths

import java.sql.{ DriverManager, Timestamp }
import java.util

import scala.io.{ Source, StdIn }
import org.apache.http.client.methods.{ CloseableHttpResponse, HttpPost }
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{ BasicCookieStore, BasicCredentialsProvider, HttpClientBuilder, HttpClients }
import org.apache.http.util.EntityUtils
import org.json4s.DefaultFormats
import org.json4s.native.Json

import scala.util.parsing.json._

@RunWith(classOf[JUnitRunner])
class EpidataLiteStreamingSpec extends FlatSpec with BeforeAndAfter with BeforeAndAfterAll with Matchers {
  //  override def clearCache(): Unit = CassandraConnector.evictCache()

  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)

  private val basePath = scala.util.Properties.envOrElse("EPIDATA_HOME", "")
  if (basePath.equals("")) {
    throw new IllegalStateException("EPIDATA_HOME environment variable not set")
  }

  private val conf = ConfigFactory.parseResources(Paths.get(basePath, "conf", "sqlite-defaults.conf").toString()).resolve()

  //  private val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  //  private val basePath = new java.io.File(".").getAbsoluteFile().getParentFile().getParent()

  //  private val sqliteDBName = conf.getString("spark.epidata.SQLite.test.dbFileName")
  private val sqliteDBName = conf.getString("spark.epidata.SQLite.dbFileName")
  private val sqliteDBUrl = Paths.get("jdbc:sqlite:", basePath, "data", sqliteDBName).toString()
  //  private val sqliteDBUrl = "jdbc:sqlite:" + basePath + "/data/" + sqliteDBName

  private val measurementClass = "automated_test"

  private val connector: Connection = DriverManager.getConnection(sqliteDBUrl)

  connector.createStatement().execute(SQLiteSchema.measurementsOriginalTableCreation)
  connector.createStatement().execute(SQLiteSchema.measurementsCleansedTableCreation)

  var result: ResultSet = null
  var post: HttpPost = null
  var response: CloseableHttpResponse = null
  var jsonBody: String = null
  var lst: List[Map[String, Any]] = null
  //  val statement = connector.create

  //construct http post request to stream/measurements
  private val HOST = "127.0.0.1:9000"
  private val AUTHENTICATION_URL = "http://" + HOST + "/authenticate/app"
  private val CREATE_MEASUREMENT_URL = "http://" + HOST + "/stream/measurements"
  private val json = """{"accessToken":"epidata123"}"""
  private val client = HttpClients.createDefault()
  post = new HttpPost(AUTHENTICATION_URL)

  post.addHeader("Content-Type", "application/json")
  post.addHeader("Set-Cookie", "")
  post.setEntity(new StringEntity(json))

  response = client.execute(post)
  val entity = response.getEntity
  val str = EntityUtils.toString(entity, "UTF-8")
  println(str)
  val cookie = response.getHeaders("Set-Cookie")(0).getValue

  before {
    // Clear existing measurements.
    connector.createStatement().execute(s"DELETE from ${com.epidata.lib.models.Measurement.DBTableName}")
    connector.createStatement().execute(s"DELETE from ${com.epidata.lib.models.MeasurementCleansed.DBTableName}")

  }

  after {
    result.close()
  }

  override def afterAll(): Unit = {
    if (connector != null) {
      connector.close()
    }
    // Stop stream
    esc.stopStream()

    println("Stream processing stoppqed successfully.")

    println("\n EpiDataLite Stream Test completed")
    println("----------------------------------------------------")
  }

  /*  ----- EpiDataLite Stream Test Started ----- */
  println("\n EpiDataLite Stream Test Started")
  val esc = new EpidataLiteStreamingContext()
  esc.init()

  // Create Transformation
  val op1 = esc.createTransformations("Identity", List("Meas-1"), Map[String, String]())
  println("transformation created: " + op1)

  val op2 = esc.createTransformations("Identity", List("Meas-1"), Map[String, String]())
  println("transformation created: " + op2)

  // Create Stream
  esc.createStream("measurements_original", "measurements_intermediate", op1)
  println("stream 1 created: " + op1)

  esc.createStream("measurements_intermediate", "measurements_cleansed", op2)
  println("stream 2 created: " + op2)

  // Start Stream
  esc.startStream()
  println("Stream started successfully")

  val COMPANY = "EpiData"
  val SITE = "San_Francisco"
  val STATION = "WSN-1"
  val timestamp = new Timestamp(System.currentTimeMillis())
  val current_time = timestamp.getTime()

  "Double automated test" should "be returned" in {
    val double_measurement = Map(
      "company" -> "Epidata_double",
      "site" -> SITE,
      "station" -> STATION,
      "sensor" -> "Anemometer",
      "ts" -> current_time,
      "event" -> "none",
      "meas_name" -> "Wind_Speed_",
      "meas_value" -> 14.0,
      "meas_unit" -> "mph",
      "meas_datatype" -> "double",
      "meas_status" -> "PASS",
      "meas_lower_limit" -> 0,
      "meas_upper_limit" -> 25,
      "meas_description" -> "test-double")
    //convert list of map object to json string
    //    val parsed = JSON.parseFull(str).get.asInstanceOf[Map[String, Any]]//map[sessionid -> ###]
    lst = List(double_measurement)
    //  lst = parsed.asInstanceOf[Map[String, Any]] :: lst
    jsonBody = Json(DefaultFormats).write(lst)
    post = new HttpPost(CREATE_MEASUREMENT_URL)
    post.addHeader("Content-Type", "application/json")
    post.setHeader("Cookie", cookie)
    post.setEntity(new StringEntity(jsonBody))
    response = client.execute(post)
    //    print(jsonBody)

    result = connector.createStatement().executeQuery(s"select * from ${com.epidata.lib.models.Measurement.DBTableName}")

    result.getString("customer") should equal("Epidata_double")
    result.getString("meas_datatype") should equal("double")
    result.getDouble("meas_value") should equal(14.0)

    result.close()
    result = connector.createStatement().executeQuery(s"select * from ${com.epidata.lib.models.MeasurementCleansed.DBTableName}")

    result.getString("customer") should equal("Epidata_double")
    result.getString("meas_datatype") should equal("double")
    result.getDouble("meas_value") should equal(14.0)

  }

  "int automated test" should "be returned" in {
    val long_measurement = Map(
      "company" -> "Epidata_long",
      "site" -> SITE,
      "station" -> STATION,
      "sensor" -> "Anemometer",
      "ts" -> current_time,
      "event" -> "none",
      "meas_name" -> "Wind_Speed",
      "meas_value" -> 16,
      "meas_unit" -> "mph",
      "meas_datatype" -> "long",
      "meas_status" -> "PASS",
      "meas_lower_limit" -> 0,
      "meas_upper_limit" -> 25,
      "meas_description" -> "test-long")
    //convert list of map object to json string
    //    val parsed = JSON.parseFull(str).get.asInstanceOf[Map[String, Any]]//map[sessionid -> ###]
    lst = List(long_measurement)
    //  lst = parsed.asInstanceOf[Map[String, Any]] :: lst
    jsonBody = Json(DefaultFormats).write(lst)
    post = new HttpPost(CREATE_MEASUREMENT_URL)
    post.addHeader("Content-Type", "application/json")
    post.setHeader("Cookie", cookie)
    post.setEntity(new StringEntity(jsonBody))
    response = client.execute(post)
    //    print(jsonBody)

    result = connector.createStatement().executeQuery(s"select * from ${com.epidata.lib.models.Measurement.DBTableName}")

    result.getString("customer") should equal("Epidata_long")
    result.getString("meas_datatype") should equal("long")
    result.getInt("meas_value_l") should equal(16)

    result.close()

    result = connector.createStatement().executeQuery(s"select * from ${com.epidata.lib.models.MeasurementCleansed.DBTableName} ")

    result.getString("customer") should equal("Epidata_long")
    result.getString("meas_datatype") should equal("long")
    result.getInt("meas_value_l") should equal(16)

  }

  //  "long automated test" should "be returned" in {
  //    val long_measurement = Map(
  //      "company" -> "Epidata_large",
  //      "site" -> SITE,
  //      "station" -> STATION,
  //      "sensor" -> "Anemometer",
  //      "ts" -> current_time,
  //      "event" -> "none",
  //      "meas_name" -> "Wind_Speed",
  //      "meas_value" -> 3448388841L,
  //      "meas_unit" -> "mph",
  //      "meas_datatype" -> "large_long",
  //      "meas_status" -> "PASS",
  //      "meas_lower_limit" -> 0L,
  //      "meas_upper_limit" -> 5000000000L,
  //      "meas_description" -> "test-large-long")
  //    //convert list of map object to json string
  //    //    val parsed = JSON.parseFull(str).get.asInstanceOf[Map[String, Any]]//map[sessionid -> ###]
  //    lst = List(long_measurement)
  //    //  lst = parsed.asInstanceOf[Map[String, Any]] :: lst
  //    jsonBody = Json(DefaultFormats).write(lst)
  //    post = new HttpPost(CREATE_MEASUREMENT_URL)
  //    post.addHeader("Content-Type", "application/json")
  //    post.setHeader("Cookie", cookie)
  //    post.setEntity(new StringEntity(jsonBody))
  //    response = client.execute(post)
  //    //    print(jsonBody)
  //
  //    result = connector.createStatement().executeQuery(s"select * from ${com.epidata.lib.models.Measurement.DBTableName}")
  //
  //    val rsmd = result.getMetaData
  //    val columnsNumber = rsmd.getColumnCount
  //      for (i <- 1 to columnsNumber) {
  //        if (i > 1) System.out.print(",  ")
  //        val columnValue = result.getString(i)
  //        System.out.print(columnValue + " " + rsmd.getColumnName(i))
  //      }
  //
  //
  //
  //    print("record:" + result.toString)
  //
  //    result.getString("customer") should equal("Epidata_large")
  //    result.getString("meas_datatype") should equal("large_long")
  //    result.getLong("meas_value_l") should equal(3448388841L)
  //
  //
  //
  //    result.close()
  //
  //
  //    result = connector.createStatement().executeQuery(s"select * from ${com.epidata.lib.models.MeasurementCleansed.DBTableName}")
  //
  //    result.getString("customer") should equal("Epidata_large")
  //    result.getString("meas_datatype") should equal("large_long")
  //    result.getLong("meas_value_l") should equal(3448388841L)
  //
  //
  //
  //  }

  "String automated test" should "be returned" in {
    val string_measurement = Map(
      "company" -> "Epidata_String",
      "site" -> SITE,
      "station" -> STATION,
      "sensor" -> "Anemometer",
      "ts" -> current_time,
      "event" -> "none",
      "meas_name" -> "Wind_Speed_",
      "meas_value" -> "dummy-string",
      "meas_unit" -> "mph",
      "meas_datatype" -> "string",
      "meas_status" -> "PASS",
      "meas_lower_limit" -> 0,
      "meas_upper_limit" -> 25,
      "meas_description" -> "test-double")
    //convert list of map object to json string
    //    val parsed = JSON.parseFull(str).get.asInstanceOf[Map[String, Any]]//map[sessionid -> ###]
    lst = List(string_measurement)
    //  lst = parsed.asInstanceOf[Map[String, Any]] :: lst
    jsonBody = Json(DefaultFormats).write(lst)
    post = new HttpPost(CREATE_MEASUREMENT_URL)
    post.addHeader("Content-Type", "application/json")
    post.setHeader("Cookie", cookie)
    post.setEntity(new StringEntity(jsonBody))
    response = client.execute(post)
    //    print(jsonBody)

    result = connector.createStatement().executeQuery(s"select * from ${com.epidata.lib.models.Measurement.DBTableName}")

    result.getString("customer") should equal("Epidata_String")
    result.getString("meas_datatype") should equal("string")
    result.getString("meas_value_s") should equal("dummy-string")

    result.close()
    result = connector.createStatement().executeQuery(s"select * from ${com.epidata.lib.models.MeasurementCleansed.DBTableName}")

    result.getString("customer") should equal("Epidata_String")
    result.getString("meas_datatype") should equal("string")
    result.getString("meas_value_s") should equal("dummy-string")

  }

}
