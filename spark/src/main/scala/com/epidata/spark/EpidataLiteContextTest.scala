/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.spark

import com.typesafe.config.ConfigFactory
import java.io.File
import java.nio.file.Paths
import java.sql.{ Connection, DriverManager, SQLException, Timestamp }
// import java.util
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, HashMap => JHashMap, LinkedList => JLinkedList, List => JList, ArrayList => JArrayList, Arrays => JArrays }

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.io.StdIn
import scala.util.Properties
//import scala.collection.mutable.Map
import com.epidata.spark.ops._

object elcTest extends App {

  /*
  val ec = new EpidataLiteContext()
  ec.init()

  //  ----- EpiDataLite Batch  Test -----

  println("\n EpiDataLite Batch Test Started")

  Class.forName("org.sqlite.JDBC");
  private val basePath = scala.util.Properties.envOrElse("EPIDATA_HOME", "")
  if (basePath.equals("") || (basePath == null)) {
    throw new IllegalStateException("EPIDATA_HOME environment variable not set")
  }

  private val conf = ConfigFactory.parseFile(new java.io.File(Paths.get(basePath, "conf", "sqlite-defaults.conf").toString())).resolve()
  println("conf value: " + conf)
  //  private val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  //  private val basePath = new java.io.File(".").getAbsoluteFile().getParent()

  private val dbName = conf.getString("spark.epidata.SQLite.test.dbFileName")
  private val dbUrl = Paths.get("jdbc:sqlite:", basePath, "data", dbName).toString()
  //private val dbUrl = "jdbc:sqlite:" + basePath + "/data/" + dbName

  println("sqlite db url: " + dbUrl)

  private val con: Connection = DriverManager.getConnection(dbUrl)
  //  val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  //  val con = DriverManager.getConnection(conf.getString("spark.epidata.SQLite.url"))
  val stmt = con.createStatement()

  // Clear tables
  val dop_original_command = s"DROP TABLE IF EXISTS ${com.epidata.lib.models.Measurement.DBTableName}"
  val dop_cleansed_command = s"DROP TABLE IF EXISTS ${com.epidata.lib.models.MeasurementCleansed.DBTableName}"
  val dop_summary_command = s"DROP TABLE IF EXISTS ${com.epidata.lib.models.MeasurementSummary.DBTableName}"
  val drop_keys_command = s"DROP TABLE IF EXISTS ${com.epidata.lib.models.MeasurementsKeys.DBTableName}"
  stmt.execute(dop_original_command)
  stmt.execute(dop_cleansed_command)
  stmt.execute(dop_summary_command)
  stmt.execute(drop_keys_command)

  // Create Tables
  val original = "play/conf/schema/measurements_original"
  val cleansed = "play/conf/schema/measurements_cleansed"
  val summary = "play/conf/schema/measurements_summary"
  val keys = "play/conf/schema/measurements_keys"
  val original_source = Source.fromFile(original)
  val cleansed_source = Source.fromFile(cleansed)
  val summary_source = Source.fromFile(summary)
  val keys_source = Source.fromFile(keys)
  val create_original = original_source.getLines.mkString
  val create_cleansed = cleansed_source.getLines.mkString
  val create_summary = summary_source.getLines.mkString
  val create_key = keys_source.getLines.mkString

  original_source.close()
  cleansed_source.close()
  summary_source.close()
  keys_source.close()
  //println(s"measurements_original schema is ${create_original}")
  //println(s"measurements_keys schema is ${create_key}")
  stmt.execute(create_original)
  stmt.execute(create_cleansed)
  stmt.execute(create_summary)
  stmt.execute(create_key)

  // Manual Insert for measurements_original
  val beginTime = new Timestamp(1619240032000L)
  val testTime = new Timestamp(1619240032000L + 5000L)
  val endTime = new Timestamp(1619240032000L + 10000L)
  val ts = beginTime
  val orderedEpochs = Measurement.epochForTs(beginTime) to Measurement.epochForTs(endTime)
  var epoch = orderedEpochs.toArray
  val meas_value_l = 1000000
  val meas_lower_limit_l = 1234567
  val meas_upper_limit_l = 7654321
  val meas_value_b = Array[Byte]()
  val company1_site1_station1_test1 = Array("Company-1", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 11.1, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 20.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site1_station1_test2 = Array("Company-1", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 12.2, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 30.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site1_station2_test1 = Array("Company-1", "Site-1", "1000", "Station-2", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 13.3, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 40.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site1_station2_test2 = Array("Company-1", "Site-1", "1000", "Station-2", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 14.4, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site2_station1_test1 = Array("Company-1", "Site-2", "1000", "Station-1", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 15.5, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site2_station1_test2 = Array("Company-1", "Site-2", "1000", "Station-1", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 16.6, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site2_station2_test1 = Array("Company-1", "Site-2", "1000", "Station-2", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 17.7, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site2_station2_test2 = Array("Company-1", "Site-2", "1000", "Station-2", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 18.8, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site1_station1_test1 = Array("Company-2", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 21.1, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 20.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site1_station1_test2 = Array("Company-2", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 22.2, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 30.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site1_station2_test1 = Array("Company-2", "Site-1", "1000", "Station-2", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 23.3, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 40.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site1_station2_test2 = Array("Company-2", "Site-1", "1000", "Station-2", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 24.4, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site2_station1_test1 = Array("Company-2", "Site-2", "1000", "Station-1", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 25.5, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site2_station1_test2 = Array("Company-2", "Site-2", "1000", "Station-1", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 26.6, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site2_station2_test1 = Array("Company-2", "Site-2", "1000", "Station-2", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 27.7, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site2_station2_test2 = Array("Company-2", "Site-2", "1000", "Station-2", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 28.8, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val columns = Array("customer", "customer_site",
    "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_datatype", "meas_value", "meas_value_l", "meas_value_s", "meas_value_b", "meas_unit",
    "meas_status", "meas_lower_limit", "meas_lower_limit_l", " meas_upper_limit", "meas_upper_limit_l", "meas_description", "val1", "val2")
  val insert_q =
    s"""#INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
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
     #meas_value,
     #meas_unit,
     #meas_status,
     #meas_description,
     #val1,
     #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#')

  val insert_q_null =
    s"""#INSERT OR REPLACE INTO ${com.epidata.lib.models.Measurement.DBTableName} (
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
        #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#')

  //println(s"prebinding: ${insert_q.toString}")
  // Company-1 Test Data
  val prepare_insert = con.prepareStatement(insert_q.toString)
  prepare_insert.setString(1, company1_site1_station1_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site1_station1_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site1_station1_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site1_station1_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company1_site1_station1_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site1_station1_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site1_station1_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site1_station1_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company1_site1_station1_test1(10).asInstanceOf[Double])
  prepare_insert.setString(12, company1_site1_station1_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site1_station1_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site1_station1_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site1_station1_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site1_station1_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site1_station1_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site1_station1_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site1_station1_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site1_station1_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company1_site1_station1_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site1_station1_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site1_station1_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site1_station1_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company1_site1_station1_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site1_station1_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site1_station1_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site1_station1_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site1_station1_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site1_station1_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site1_station1_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site1_station1_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site1_station1_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company1_site1_station1_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site1_station1_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site1_station1_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site1_station1_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company1_site1_station1_test2(10).asInstanceOf[Double])
  prepare_insert.setString(12, company1_site1_station1_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site1_station1_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site1_station1_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site1_station1_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site1_station1_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site1_station1_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site1_station1_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site1_station1_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site1_station1_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company1_site1_station1_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site1_station1_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site1_station1_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site1_station1_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company1_site1_station1_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site1_station1_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site1_station1_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site1_station1_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site1_station1_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site1_station2_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site1_station2_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site1_station2_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site1_station2_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company1_site1_station2_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site1_station2_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site1_station2_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site1_station2_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company1_site1_station2_test1(10).asInstanceOf[Double])
  prepare_insert.setString(12, company1_site1_station2_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site1_station2_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site1_station2_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site1_station2_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site1_station2_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site1_station2_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site1_station2_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site1_station2_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site1_station2_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company1_site1_station2_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site1_station2_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site1_station2_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site1_station2_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company1_site1_station2_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site1_station2_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site1_station2_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site1_station2_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site1_station2_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site1_station2_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site1_station2_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site1_station2_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site1_station2_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company1_site1_station2_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site1_station2_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site1_station2_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site1_station2_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company1_site1_station2_test2(10).asInstanceOf[Double])
  prepare_insert.setString(12, company1_site1_station2_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site1_station2_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site1_station2_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site1_station2_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site1_station2_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site1_station2_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site1_station2_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site1_station2_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site1_station2_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company1_site1_station2_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site1_station2_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site1_station2_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site1_station2_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company1_site1_station2_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site1_station2_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site1_station2_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site1_station2_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site1_station2_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site2_station1_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site2_station1_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site2_station1_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site2_station1_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company1_site2_station1_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site2_station1_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site2_station1_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site2_station1_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company1_site2_station1_test1(10).asInstanceOf[Double])
  prepare_insert.setString(12, company1_site2_station1_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site2_station1_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site2_station1_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site2_station1_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site2_station1_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site2_station1_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site2_station1_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site2_station1_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site2_station1_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company1_site2_station1_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site2_station1_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site2_station1_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site2_station1_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company1_site2_station1_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site2_station1_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site2_station1_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site2_station1_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site2_station1_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site2_station1_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site2_station1_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site2_station1_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site2_station1_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company1_site2_station1_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site2_station1_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site2_station1_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site2_station1_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company1_site2_station1_test2(10).asInstanceOf[Double])
  prepare_insert.setString(12, company1_site2_station1_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site2_station1_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site2_station1_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site2_station1_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site2_station1_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site2_station1_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site2_station1_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site2_station1_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site2_station1_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company1_site2_station1_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site2_station1_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site2_station1_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site2_station1_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company1_site2_station1_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site2_station1_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site2_station1_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site2_station1_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site2_station1_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site2_station2_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site2_station2_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site2_station2_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site2_station2_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company1_site2_station2_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site2_station2_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site2_station2_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site2_station2_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company1_site2_station2_test1(10).asInstanceOf[Double])
  prepare_insert.setString(12, company1_site2_station2_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site2_station2_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site2_station2_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site2_station2_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site2_station2_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site2_station2_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site2_station2_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site2_station2_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site2_station2_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company1_site2_station2_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site2_station2_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site2_station2_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site2_station2_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company1_site2_station2_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site2_station2_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site2_station2_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site2_station2_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site2_station2_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site2_station2_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site2_station2_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site2_station2_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site2_station2_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company1_site2_station2_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site2_station2_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site2_station2_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site2_station2_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company1_site2_station2_test2(10).asInstanceOf[Double])
  prepare_insert.setString(12, company1_site2_station2_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site2_station2_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site2_station2_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site2_station2_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site2_station2_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company1_site2_station2_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company1_site2_station2_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company1_site2_station2_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company1_site2_station2_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company1_site2_station2_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company1_site2_station2_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company1_site2_station2_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company1_site2_station2_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company1_site2_station2_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company1_site2_station2_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company1_site2_station2_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company1_site2_station2_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company1_site2_station2_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  // Compnay-2 Test Data
  prepare_insert.setString(1, company2_site1_station1_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site1_station1_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site1_station1_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site1_station1_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company2_site1_station1_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site1_station1_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site1_station1_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site1_station1_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company2_site1_station1_test1(10).asInstanceOf[Double])
  prepare_insert.setString(12, company2_site1_station1_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site1_station1_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site1_station1_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site1_station1_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site1_station1_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site1_station1_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site1_station1_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site1_station1_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site1_station1_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company2_site1_station1_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site1_station1_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site1_station1_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site1_station1_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company2_site1_station1_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site1_station1_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site1_station1_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site1_station1_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site1_station1_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site1_station1_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site1_station1_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site1_station1_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site1_station1_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company2_site1_station1_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site1_station1_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site1_station1_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site1_station1_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company2_site1_station1_test2(10).asInstanceOf[Double])
  prepare_insert.setString(12, company2_site1_station1_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site1_station1_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site1_station1_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site1_station1_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site1_station1_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site1_station1_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site1_station1_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site1_station1_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site1_station1_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company2_site1_station1_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site1_station1_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site1_station1_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site1_station1_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company2_site1_station1_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site1_station1_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site1_station1_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site1_station1_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site1_station1_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site1_station2_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site1_station2_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site1_station2_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site1_station2_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company2_site1_station2_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site1_station2_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site1_station2_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site1_station2_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company2_site1_station2_test1(10).asInstanceOf[Double])
  prepare_insert.setString(12, company2_site1_station2_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site1_station2_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site1_station2_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site1_station2_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site1_station2_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site1_station2_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site1_station2_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site1_station2_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site1_station2_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company2_site1_station2_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site1_station2_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site1_station2_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site1_station2_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company2_site1_station2_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site1_station2_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site1_station2_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site1_station2_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site1_station2_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site1_station2_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site1_station2_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site1_station2_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site1_station2_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company2_site1_station2_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site1_station2_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site1_station2_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site1_station2_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company2_site1_station2_test2(10).asInstanceOf[Double])
  prepare_insert.setString(12, company2_site1_station2_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site1_station2_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site1_station2_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site1_station2_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site1_station2_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site1_station2_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site1_station2_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site1_station2_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site1_station2_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company2_site1_station2_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site1_station2_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site1_station2_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site1_station2_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company2_site1_station2_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site1_station2_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site1_station2_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site1_station2_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site1_station2_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site2_station1_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site2_station1_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site2_station1_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site2_station1_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company2_site2_station1_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site2_station1_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site2_station1_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site2_station1_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company2_site2_station1_test1(10).asInstanceOf[Double])
  prepare_insert.setString(12, company2_site2_station1_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site2_station1_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site2_station1_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site2_station1_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site2_station1_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site2_station1_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site2_station1_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site2_station1_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site2_station1_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company2_site2_station1_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site2_station1_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site2_station1_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site2_station1_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company2_site2_station1_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site2_station1_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site2_station1_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site2_station1_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site2_station1_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site2_station1_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site2_station1_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site2_station1_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site2_station1_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company2_site2_station1_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site2_station1_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site2_station1_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site2_station1_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company2_site2_station1_test2(10).asInstanceOf[Double])
  prepare_insert.setString(12, company2_site2_station1_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site2_station1_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site2_station1_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site2_station1_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site2_station1_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site2_station1_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site2_station1_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site2_station1_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site2_station1_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company2_site2_station1_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site2_station1_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site2_station1_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site2_station1_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company2_site2_station1_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site2_station1_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site2_station1_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site2_station1_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site2_station1_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site2_station2_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site2_station2_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site2_station2_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site2_station2_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company2_site2_station2_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site2_station2_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site2_station2_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site2_station2_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company2_site2_station2_test1(10).asInstanceOf[Double])
  prepare_insert.setString(12, company2_site2_station2_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site2_station2_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site2_station2_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site2_station2_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site2_station2_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site2_station2_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site2_station2_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site2_station2_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site2_station2_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company2_site2_station2_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site2_station2_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site2_station2_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site2_station2_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company2_site2_station2_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site2_station2_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site2_station2_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site2_station2_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site2_station2_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site2_station2_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site2_station2_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site2_station2_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site2_station2_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, company2_site2_station2_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site2_station2_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site2_station2_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site2_station2_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, company2_site2_station2_test2(10).asInstanceOf[Double])
  prepare_insert.setString(12, company2_site2_station2_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site2_station2_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site2_station2_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site2_station2_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site2_station2_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, company2_site2_station2_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, company2_site2_station2_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, company2_site2_station2_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, company2_site2_station2_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, company2_site2_station2_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, company2_site2_station2_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, company2_site2_station2_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, company2_site2_station2_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, company2_site2_station2_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, company2_site2_station2_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, company2_site2_station2_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, company2_site2_station2_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, company2_site2_station2_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  // Company-1 Test Data - null value
  val prepare_insert_null = con.prepareStatement(insert_q_null.toString)
  prepare_insert_null.setString(1, company1_site1_station1_test1(0).asInstanceOf[String])
  prepare_insert_null.setString(2, company1_site1_station1_test1(1).asInstanceOf[String])
  prepare_insert_null.setString(3, company1_site1_station1_test1(2).asInstanceOf[String])
  prepare_insert_null.setString(4, company1_site1_station1_test1(3).asInstanceOf[String])
  prepare_insert_null.setInt(5, epoch(0))
  prepare_insert_null.setTimestamp(6, beginTime)
  prepare_insert_null.setString(7, company1_site1_station1_test1(6).asInstanceOf[String])
  prepare_insert_null.setString(8, company1_site1_station1_test1(7).asInstanceOf[String])
  prepare_insert_null.setString(9, company1_site1_station1_test1(8).asInstanceOf[String])
  prepare_insert_null.setString(10, company1_site1_station1_test1(9).asInstanceOf[String])
  //  prepare_insert_null.setDouble(11, company1_site1_station1_test1(10).asInstanceOf[Double])
  prepare_insert_null.setString(11, company1_site1_station1_test1(14).asInstanceOf[String])
  prepare_insert_null.setString(12, company1_site1_station1_test1(15).asInstanceOf[String])
  prepare_insert_null.setString(13, company1_site1_station1_test1(20).asInstanceOf[String])
  prepare_insert_null.setString(14, company1_site1_station1_test1(21).asInstanceOf[String])
  prepare_insert_null.setString(15, company1_site1_station1_test1(22).asInstanceOf[String])
  prepare_insert_null.executeUpdate()

  // Insert Check - Measurements Original
  val rsOriginal = con.prepareStatement(s"SELECT * FROM ${com.epidata.lib.models.Measurement.DBTableName}").executeQuery()
  while (rsOriginal.next()) {
    val t = com.epidata.lib.models.Measurement.resultSetToMeasurement(rsOriginal)
    println(s"Insert Check: ${t.toString}")
  }
  //println()

  // var fieldQueryOriginal = new JLinkedHashMap[String, JList[String]]()
  // fieldQueryOriginal.put("company", JArrays.asList("Company-2", "Company-1"))
  // fieldQueryOriginal.put("site", JArrays.asList("Site-1", "Site-2"))
  // fieldQueryOriginal.put("device_group", JArrays.asList("1000"))
  // fieldQueryOriginal.put("tester", JArrays.asList("Station-1", "Station-2"))
  // fieldQueryOriginal.put("test_name", JArrays.asList("Test-1", "Test-2"))

  // val resultsOriginal = ec.query(
  //   fieldQueryOriginal,
  //   beginTime,
  //   endTime)
  // val measOriginalIter = resultsOriginal.iterator()
  // while (measOriginalIter.hasNext()) {
  //   println(s"meas_original query row: ${measOriginalIter.next()}")
  // }

  // Manual Insert for measurements_cleansed
  val company1_site1_station1_test1_cleansed = Array("Company-1", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 11.1, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 20.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site1_station1_test2_cleansed = Array("Company-1", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 12.2, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 30.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site1_station2_test1_cleansed = Array("Company-1", "Site-1", "1000", "Station-2", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 13.3, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 40.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site1_station2_test2_cleansed = Array("Company-1", "Site-1", "1000", "Station-2", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 14.4, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site2_station1_test1_cleansed = Array("Company-1", "Site-2", "1000", "Station-1", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 15.5, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site2_station1_test2_cleansed = Array("Company-1", "Site-2", "1000", "Station-1", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 16.6, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site2_station2_test1_cleansed = Array("Company-1", "Site-2", "1000", "Station-2", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 17.7, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company1_site2_station2_test2_cleansed = Array("Company-1", "Site-2", "1000", "Station-2", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 18.8, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site1_station1_test1_cleansed = Array("Company-2", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 21.1, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 20.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site1_station1_test2_cleansed = Array("Company-2", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 22.2, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 30.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site1_station2_test1_cleansed = Array("Company-2", "Site-1", "1000", "Station-2", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 23.3, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 40.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site1_station2_test2_cleansed = Array("Company-2", "Site-1", "1000", "Station-2", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 24.4, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site2_station1_test1_cleansed = Array("Company-2", "Site-2", "1000", "Station-1", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 25.5, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site2_station1_test2_cleansed = Array("Company-2", "Site-2", "1000", "Station-1", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 26.6, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site2_station2_test1_cleansed = Array("Company-2", "Site-2", "1000", "Station-2", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 27.7, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val company2_site2_station2_test2_cleansed = Array("Company-2", "Site-2", "1000", "Station-2", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 28.8, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", "meas_cleansed_flag", "meas_cleansed_method", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val columns_cleansed = Array("customer", "customer_site",
    "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_datatype", "meas_value", "meas_value_l", "meas_value_s", "meas_value_b", "meas_unit",
    "meas_status", "meas_flag", "meas_method", "meas_lower_limit", "meas_lower_limit_l", " meas_upper_limit", "meas_upper_limit_l", "meas_description", "val1", "val2")
  val insert_q_cleansed =
    s"""#INSERT OR REPLACE INTO ${com.epidata.lib.models.MeasurementCleansed.DBTableName} (
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
     #meas_value,
     #meas_unit,
     #meas_status,
     #meas_flag,
     #meas_method,
     #meas_description,
     #val1,
     #val2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#')
  //println(s"prebinding: ${insert_q_cleansed.toString}")
  // Company-1 Test Data
  val prepare_insert_cleansed = con.prepareStatement(insert_q_cleansed.toString)
  prepare_insert_cleansed.setString(1, company1_site1_station1_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site1_station1_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site1_station1_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site1_station1_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company1_site1_station1_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site1_station1_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site1_station1_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site1_station1_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company1_site1_station1_test1_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company1_site1_station1_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site1_station1_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site1_station1_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site1_station1_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site1_station1_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site1_station1_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site1_station1_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site1_station1_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site1_station1_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site1_station1_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site1_station1_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company1_site1_station1_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site1_station1_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site1_station1_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site1_station1_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company1_site1_station1_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site1_station1_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site1_station1_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site1_station1_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site1_station1_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site1_station1_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site1_station1_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site1_station1_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site1_station1_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site1_station1_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site1_station1_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company1_site1_station1_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site1_station1_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site1_station1_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site1_station1_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company1_site1_station1_test2_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company1_site1_station1_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site1_station1_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site1_station1_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site1_station1_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site1_station1_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site1_station1_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site1_station1_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site1_station1_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site1_station1_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site1_station1_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site1_station1_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company1_site1_station1_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site1_station1_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site1_station1_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site1_station1_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company1_site1_station1_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site1_station1_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site1_station1_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site1_station1_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site1_station1_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site1_station1_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site1_station1_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site1_station2_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site1_station2_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site1_station2_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site1_station2_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company1_site1_station2_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site1_station2_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site1_station2_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site1_station2_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company1_site1_station2_test1_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company1_site1_station2_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site1_station2_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site1_station2_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site1_station2_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site1_station2_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site1_station2_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site1_station2_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site1_station2_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site1_station2_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site1_station2_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site1_station2_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company1_site1_station2_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site1_station2_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site1_station2_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site1_station2_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company1_site1_station2_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site1_station2_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site1_station2_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site1_station2_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site1_station2_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site1_station2_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site1_station2_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site1_station2_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site1_station2_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site1_station2_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site1_station2_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company1_site1_station2_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site1_station2_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site1_station2_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site1_station2_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company1_site1_station2_test2_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company1_site1_station2_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site1_station2_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site1_station2_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site1_station2_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site1_station2_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site1_station2_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site1_station2_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site1_station2_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site1_station2_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site1_station2_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site1_station2_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company1_site1_station2_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site1_station2_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site1_station2_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site1_station2_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company1_site1_station2_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site1_station2_test2_cleansed(15).asInstanceOf[String])

  prepare_insert_cleansed.setString(14, company1_site1_station2_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site1_station2_test2_cleansed(17).asInstanceOf[String])

  prepare_insert_cleansed.setString(16, company1_site1_station2_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site1_station2_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site1_station2_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site2_station1_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site2_station1_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site2_station1_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site2_station1_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company1_site2_station1_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site2_station1_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site2_station1_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site2_station1_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company1_site2_station1_test1_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company1_site2_station1_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site2_station1_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site2_station1_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site2_station1_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site2_station1_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site2_station1_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site2_station1_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site2_station1_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site2_station1_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site2_station1_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site2_station1_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company1_site2_station1_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site2_station1_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site2_station1_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site2_station1_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company1_site2_station1_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site2_station1_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site2_station1_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site2_station1_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site2_station1_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site2_station1_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site2_station1_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site2_station1_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site2_station1_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site2_station1_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site2_station1_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company1_site2_station1_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site2_station1_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site2_station1_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site2_station1_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company1_site2_station1_test2_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company1_site2_station1_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site2_station1_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site2_station1_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site2_station1_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site2_station1_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site2_station1_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site2_station1_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site2_station1_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site2_station1_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site2_station1_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site2_station1_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company1_site2_station1_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site2_station1_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site2_station1_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site2_station1_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company1_site2_station1_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site2_station1_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site2_station1_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site2_station1_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site2_station1_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site2_station1_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site2_station1_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site2_station2_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site2_station2_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site2_station2_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site2_station2_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company1_site2_station2_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site2_station2_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site2_station2_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site2_station2_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company1_site2_station2_test1_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company1_site2_station2_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site2_station2_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site2_station2_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site2_station2_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site2_station2_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site2_station2_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site2_station2_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site2_station2_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site2_station2_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site2_station2_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site2_station2_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company1_site2_station2_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site2_station2_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site2_station2_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site2_station2_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company1_site2_station2_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site2_station2_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site2_station2_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site2_station2_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site2_station2_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site2_station2_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site2_station2_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site2_station2_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site2_station2_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site2_station2_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site2_station2_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company1_site2_station2_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site2_station2_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site2_station2_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site2_station2_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company1_site2_station2_test2_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company1_site2_station2_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site2_station2_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site2_station2_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site2_station2_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site2_station2_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site2_station2_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site2_station2_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company1_site2_station2_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company1_site2_station2_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company1_site2_station2_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company1_site2_station2_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company1_site2_station2_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company1_site2_station2_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company1_site2_station2_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company1_site2_station2_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company1_site2_station2_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company1_site2_station2_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company1_site2_station2_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company1_site2_station2_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company1_site2_station2_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company1_site2_station2_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company1_site2_station2_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  // Compnay-2 Test Data
  prepare_insert_cleansed.setString(1, company2_site1_station1_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site1_station1_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site1_station1_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site1_station1_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company2_site1_station1_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site1_station1_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site1_station1_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site1_station1_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company2_site1_station1_test1_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company2_site1_station1_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site1_station1_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site1_station1_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site1_station1_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site1_station1_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site1_station1_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site1_station1_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site1_station1_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site1_station1_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site1_station1_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site1_station1_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company2_site1_station1_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site1_station1_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site1_station1_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site1_station1_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company2_site1_station1_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site1_station1_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site1_station1_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site1_station1_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site1_station1_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site1_station1_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site1_station1_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site1_station1_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site1_station1_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site1_station1_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site1_station1_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company2_site1_station1_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site1_station1_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site1_station1_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site1_station1_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company2_site1_station1_test2_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company2_site1_station1_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site1_station1_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site1_station1_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site1_station1_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site1_station1_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site1_station1_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site1_station1_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site1_station1_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site1_station1_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site1_station1_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site1_station1_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company2_site1_station1_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site1_station1_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site1_station1_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site1_station1_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company2_site1_station1_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site1_station1_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site1_station1_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site1_station1_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site1_station1_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site1_station1_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site1_station1_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site1_station2_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site1_station2_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site1_station2_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site1_station2_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company2_site1_station2_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site1_station2_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site1_station2_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site1_station2_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company2_site1_station2_test1_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company2_site1_station2_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site1_station2_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site1_station2_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site1_station2_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site1_station2_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site1_station2_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site1_station2_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site1_station2_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site1_station2_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site1_station2_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site1_station2_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company2_site1_station2_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site1_station2_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site1_station2_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site1_station2_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company2_site1_station2_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site1_station2_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site1_station2_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site1_station2_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site1_station2_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site1_station2_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site1_station2_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site1_station2_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site1_station2_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site1_station2_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site1_station2_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company2_site1_station2_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site1_station2_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site1_station2_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site1_station2_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company2_site1_station2_test2_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company2_site1_station2_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site1_station2_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site1_station2_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site1_station2_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site1_station2_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site1_station2_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site1_station2_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site1_station2_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site1_station2_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site1_station2_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site1_station2_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company2_site1_station2_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site1_station2_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site1_station2_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site1_station2_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company2_site1_station2_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site1_station2_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site1_station2_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site1_station2_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site1_station2_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site1_station2_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site1_station2_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site2_station1_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site2_station1_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site2_station1_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site2_station1_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company2_site2_station1_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site2_station1_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site2_station1_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site2_station1_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company2_site2_station1_test1_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company2_site2_station1_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site2_station1_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site2_station1_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site2_station1_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site2_station1_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site2_station1_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site2_station1_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site2_station1_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site2_station1_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site2_station1_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site2_station1_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company2_site2_station1_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site2_station1_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site2_station1_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site2_station1_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company2_site2_station1_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site2_station1_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site2_station1_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site2_station1_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site2_station1_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site2_station1_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site2_station1_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site2_station1_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site2_station1_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site2_station1_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site2_station1_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company2_site2_station1_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site2_station1_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site2_station1_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site2_station1_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company2_site2_station1_test2_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company2_site2_station1_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site2_station1_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site2_station1_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site2_station1_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site2_station1_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site2_station1_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site2_station1_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site2_station1_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site2_station1_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site2_station1_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site2_station1_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company2_site2_station1_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site2_station1_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site2_station1_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site2_station1_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company2_site2_station1_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site2_station1_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site2_station1_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site2_station1_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site2_station1_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site2_station1_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site2_station1_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site2_station2_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site2_station2_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site2_station2_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site2_station2_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company2_site2_station2_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site2_station2_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site2_station2_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site2_station2_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company2_site2_station2_test1_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company2_site2_station2_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site2_station2_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site2_station2_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site2_station2_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site2_station2_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site2_station2_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site2_station2_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site2_station2_test1_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site2_station2_test1_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site2_station2_test1_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site2_station2_test1_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company2_site2_station2_test1_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site2_station2_test1_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site2_station2_test1_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site2_station2_test1_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company2_site2_station2_test1_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site2_station2_test1_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site2_station2_test1_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site2_station2_test1_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site2_station2_test1_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site2_station2_test1_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site2_station2_test1_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site2_station2_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site2_station2_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site2_station2_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site2_station2_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, beginTime)
  prepare_insert_cleansed.setString(7, company2_site2_station2_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site2_station2_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site2_station2_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site2_station2_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, company2_site2_station2_test2_cleansed(10).asInstanceOf[Double])
  prepare_insert_cleansed.setString(12, company2_site2_station2_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site2_station2_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site2_station2_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site2_station2_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site2_station2_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site2_station2_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site2_station2_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()
  prepare_insert_cleansed.setString(1, company2_site2_station2_test2_cleansed(0).asInstanceOf[String])
  prepare_insert_cleansed.setString(2, company2_site2_station2_test2_cleansed(1).asInstanceOf[String])
  prepare_insert_cleansed.setString(3, company2_site2_station2_test2_cleansed(2).asInstanceOf[String])
  prepare_insert_cleansed.setString(4, company2_site2_station2_test2_cleansed(3).asInstanceOf[String])
  prepare_insert_cleansed.setInt(5, epoch(0))
  prepare_insert_cleansed.setTimestamp(6, testTime)
  prepare_insert_cleansed.setString(7, company2_site2_station2_test2_cleansed(6).asInstanceOf[String])
  prepare_insert_cleansed.setString(8, company2_site2_station2_test2_cleansed(7).asInstanceOf[String])
  prepare_insert_cleansed.setString(9, company2_site2_station2_test2_cleansed(8).asInstanceOf[String])
  prepare_insert_cleansed.setString(10, company2_site2_station2_test2_cleansed(9).asInstanceOf[String])
  prepare_insert_cleansed.setDouble(11, 99999.0)
  prepare_insert_cleansed.setString(12, company2_site2_station2_test2_cleansed(14).asInstanceOf[String])
  prepare_insert_cleansed.setString(13, company2_site2_station2_test2_cleansed(15).asInstanceOf[String])
  prepare_insert_cleansed.setString(14, company2_site2_station2_test2_cleansed(16).asInstanceOf[String])
  prepare_insert_cleansed.setString(15, company2_site2_station2_test2_cleansed(17).asInstanceOf[String])
  prepare_insert_cleansed.setString(16, company2_site2_station2_test2_cleansed(22).asInstanceOf[String])
  prepare_insert_cleansed.setString(17, company2_site2_station2_test2_cleansed(23).asInstanceOf[String])
  prepare_insert_cleansed.setString(18, company2_site2_station2_test2_cleansed(24).asInstanceOf[String])
  prepare_insert_cleansed.executeUpdate()

  // Insert Check - Measurements Cleansed
  val rsCleansed = con.prepareStatement(s"SELECT * FROM ${com.epidata.lib.models.MeasurementCleansed.DBTableName}").executeQuery()
  while (rsCleansed.next()) {
    val t = com.epidata.lib.models.MeasurementCleansed.resultSetToMeasurementCleansed(rsCleansed)
    println(s"Insert Check: ${t.toString}")
  }
  //println()

  // val fieldQueryCleansed = new JLinkedHashMap[String, JList[String]]()
  // fieldQueryCleansed.put("company", JArrays.asList("Company-2", "Company-1"))
  // fieldQueryCleansed.put("site", JArrays.asList("Site-1", "Site-2"))
  // fieldQueryCleansed.put("device_group", JArrays.asList("1000"))
  // fieldQueryCleansed.put("tester", JArrays.asList("Station-1", "Station-2"))
  // fieldQueryCleansed.put("test_name", JArrays.asList("Test-1", "Test-2"))

  // val resultsCleansed = ec.queryMeasurementCleansed(
  //   fieldQueryCleansed,
  //   beginTime,
  //   endTime)
  // val measCleansedIter = resultsCleansed.iterator()
  // while (measCleansedIter.hasNext()) {
  //   println(s"meas_cleansed query row: ${measCleansedIter.next()}")
  // }

  // Manual Insert for measurements_summary
  val testStartTime = new Timestamp(1619240032000L + 1000L)
  val testStopTime = new Timestamp(1619240032000L + 9000L)

  val company1_site1_station1_test1_summary = Array("Company-1", "Site-1", "1000", "Station-1", testStartTime, testStopTime, "100001", "Test-1",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 5.01, "std" -> 0.151, "min" -> 4.51, "max" -> 5.51).toString(), "basic_statistics")
  val company1_site1_station1_test2_summary = Array("Company-1", "Site-1", "1000", "Station-1", testStartTime, testStopTime, "100001", "Test-2",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 100.1, "std" -> 3.01, "min" -> 90.1, "max" -> 110.1).toString(), "basic_statistics")

  val company1_site1_station2_test1_summary = Array("Company-1", "Site-1", "1000", "Station-2", testStartTime, testStopTime, "100001", "Test-1",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 5.02, "std" -> 0.152, "min" -> 4.52, "max" -> 5.52).toString(), "basic_statistics")
  val company1_site1_station2_test2_summary = Array("Company-1", "Site-1", "1000", "Station-2", testStartTime, testStopTime, "100001", "Test-2",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 100.2, "std" -> 3.02, "min" -> 90.2, "max" -> 110.2).toString(), "basic_statistics")

  val company1_site2_station1_test1_summary = Array("Company-1", "Site-2", "1000", "Station-1", testStartTime, testStopTime, "100001", "Test-1",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 5.03, "std" -> 0.153, "min" -> 4.53, "max" -> 5.53).toString(), "basic_statistics")
  val company1_site2_station1_test2_summary = Array("Company-1", "Site-2", "1000", "Station-1", testStartTime, testStopTime, "100001", "Test-2",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 100.3, "std" -> 3.03, "min" -> 90.3, "max" -> 110.3).toString(), "basic_statistics")

  val company1_site2_station2_test1_summary = Array("Company-1", "Site-2", "1000", "Station-2", testStartTime, testStopTime, "100001", "Test-1",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 5.04, "std" -> 0.154, "min" -> 4.54, "max" -> 5.54).toString(), "basic_statistics")
  val company1_site2_station2_test2_summary = Array("Company-1", "Site-2", "1000", "Station-2", testStartTime, testStopTime, "100001", "Test-2",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 100.4, "std" -> 3.04, "min" -> 90.4, "max" -> 110.4).toString(), "basic_statistics")

  val company2_site1_station1_test1_summary = Array("Company-2", "Site-1", "1000", "Station-1", testStartTime, testStopTime, "100001", "Test-1",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 5.05, "std" -> 0.155, "min" -> 4.55, "max" -> 5.55).toString(), "basic_statistics")
  val company2_site1_station1_test2_summary = Array("Company-2", "Site-1", "1000", "Station-1", testStartTime, testStopTime, "100001", "Test-2",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 100.5, "std" -> 3.05, "min" -> 90.5, "max" -> 110.5).toString(), "basic_statistics")

  val company2_site1_station2_test1_summary = Array("Company-2", "Site-1", "1000", "Station-2", testStartTime, testStopTime, "100001", "Test-1",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 5.06, "std" -> 0.156, "min" -> 4.56, "max" -> 5.56).toString(), "basic_statistics")
  val company2_site1_station2_test2_summary = Array("Company-2", "Site-1", "1000", "Station-2", testStartTime, testStopTime, "100001", "Test-2",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 100.6, "std" -> 3.06, "min" -> 90.6, "max" -> 110.6).toString(), "basic_statistics")

  val company2_site2_station1_test1_summary = Array("Company-2", "Site-2", "1000", "Station-1", testStartTime, testStopTime, "100001", "Test-1",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 5.07, "std" -> 0.157, "min" -> 4.57, "max" -> 5.57).toString(), "basic_statistics")
  val company2_site2_station1_test2_summary = Array("Company-2", "Site-2", "1000", "Station-1", testStartTime, testStopTime, "100001", "Test-2",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 100.7, "std" -> 3.07, "min" -> 90.7, "max" -> 110.7).toString(), "basic_statistics")

  val company2_site2_station2_test1_summary = Array("Company-2", "Site-2", "1000", "Station-2", testStartTime, testStopTime, "100001", "Test-1",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 5.08, "std" -> 0.158, "min" -> 4.58, "max" -> 5.58).toString(), "basic_statistics")
  val company2_site2_station2_test2_summary = Array("Company-2", "Site-2", "1000", "Station-2", testStartTime, testStopTime, "100001", "Test-2",
    "Meas-1", "meas_statistics", Map("count" -> 10, "mean" -> 100.8, "std" -> 3.08, "min" -> 90.8, "max" -> 110.8).toString(), "basic_statistics")

  val columns_summary = Array("customer", "customer_site",
    "collection", "dataset", "start_time", "stop_time", "key1", "key2", "key3", "meas_summary_name", "meas_summary_value", "meas_summary_description")
  val insert_q_summary =
    s"""#INSERT OR REPLACE INTO ${com.epidata.lib.models.MeasurementSummary.DBTableName} (
      #customer,
      #customer_site,
      #collection,
      #dataset,
      #start_time,
      #stop_time,
      #key1,
      #key2,
      #key3,
      #meas_summary_name,
      #meas_summary_value,
      #meas_summary_description) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin('#')
  //println(s"prebinding: ${insert_q_summary.toString}")

  // Company-1 Test Data
  val prepare_insert_summary = con.prepareStatement(insert_q_summary.toString)
  prepare_insert_summary.setString(1, company1_site1_station1_test1_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company1_site1_station1_test1_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company1_site1_station1_test1_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company1_site1_station1_test1_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company1_site1_station1_test1_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company1_site1_station1_test1_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company1_site1_station1_test1_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company1_site1_station1_test1_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company1_site1_station1_test1_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company1_site1_station1_test1_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company1_site1_station1_test2_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company1_site1_station1_test2_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company1_site1_station1_test2_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company1_site1_station1_test2_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company1_site1_station1_test2_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company1_site1_station1_test2_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company1_site1_station1_test2_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company1_site1_station1_test2_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company1_site1_station1_test2_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company1_site1_station1_test2_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company1_site1_station2_test1_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company1_site1_station2_test1_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company1_site1_station2_test1_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company1_site1_station2_test1_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company1_site1_station2_test1_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company1_site1_station2_test1_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company1_site1_station2_test1_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company1_site1_station2_test1_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company1_site1_station2_test1_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company1_site1_station2_test1_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company1_site1_station2_test2_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company1_site1_station2_test2_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company1_site1_station2_test2_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company1_site1_station2_test2_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company1_site1_station2_test2_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company1_site1_station2_test2_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company1_site1_station2_test2_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company1_site1_station2_test2_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company1_site1_station2_test2_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company1_site1_station2_test2_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company1_site2_station1_test1_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company1_site2_station1_test1_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company1_site2_station1_test1_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company1_site2_station1_test1_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company1_site2_station1_test1_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company1_site2_station1_test1_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company1_site2_station1_test1_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company1_site2_station1_test1_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company1_site2_station1_test1_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company1_site2_station1_test1_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company1_site2_station1_test2_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company1_site2_station1_test2_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company1_site2_station1_test2_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company1_site2_station1_test2_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company1_site2_station1_test2_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company1_site2_station1_test2_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company1_site2_station1_test2_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company1_site2_station1_test2_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company1_site2_station1_test2_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company1_site2_station1_test2_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company1_site2_station2_test1_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company1_site2_station2_test1_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company1_site2_station2_test1_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company1_site2_station2_test1_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company1_site2_station2_test1_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company1_site2_station2_test1_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company1_site2_station2_test1_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company1_site2_station2_test1_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company1_site2_station2_test1_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company1_site2_station2_test1_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company1_site2_station2_test2_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company1_site2_station2_test2_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company1_site2_station2_test2_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company1_site2_station2_test2_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company1_site2_station2_test2_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company1_site2_station2_test2_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company1_site2_station2_test2_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company1_site2_station2_test2_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company1_site2_station2_test2_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company1_site2_station2_test2_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  // Company-2 Test Data
  prepare_insert_summary.setString(1, company2_site1_station1_test1_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company2_site1_station1_test1_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company2_site1_station1_test1_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company2_site1_station1_test1_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company2_site1_station1_test1_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company2_site1_station1_test1_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company2_site1_station1_test1_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company2_site1_station1_test1_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company2_site1_station1_test1_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company2_site1_station1_test1_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company2_site1_station1_test2_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company2_site1_station1_test2_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company2_site1_station1_test2_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company2_site1_station1_test2_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company2_site1_station1_test2_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company2_site1_station1_test2_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company2_site1_station1_test2_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company2_site1_station1_test2_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company2_site1_station1_test2_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company2_site1_station1_test2_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company2_site1_station2_test1_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company2_site1_station2_test1_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company2_site1_station2_test1_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company2_site1_station2_test1_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company2_site1_station2_test1_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company2_site1_station2_test1_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company2_site1_station2_test1_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company2_site1_station2_test1_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company2_site1_station2_test1_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company2_site1_station2_test1_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company2_site1_station2_test2_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company2_site1_station2_test2_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company2_site1_station2_test2_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company2_site1_station2_test2_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company2_site1_station2_test2_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company2_site1_station2_test2_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company2_site1_station2_test2_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company2_site1_station2_test2_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company2_site1_station2_test2_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company2_site1_station2_test2_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company2_site2_station1_test1_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company2_site2_station1_test1_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company2_site2_station1_test1_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company2_site2_station1_test1_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company2_site2_station1_test1_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company2_site2_station1_test1_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company2_site2_station1_test1_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company2_site2_station1_test1_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company2_site2_station1_test1_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company2_site2_station1_test1_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company2_site2_station1_test2_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company2_site2_station1_test2_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company2_site2_station1_test2_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company2_site2_station1_test2_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company2_site2_station1_test2_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company2_site2_station1_test2_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company2_site2_station1_test2_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company2_site2_station1_test2_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company2_site2_station1_test2_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company2_site2_station1_test2_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company2_site2_station2_test1_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company2_site2_station2_test1_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company2_site2_station2_test1_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company2_site2_station2_test1_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company2_site2_station2_test1_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company2_site2_station2_test1_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company2_site2_station2_test1_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company2_site2_station2_test1_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company2_site2_station2_test1_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company2_site2_station2_test1_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  prepare_insert_summary.setString(1, company2_site2_station2_test2_summary(0).asInstanceOf[String])
  prepare_insert_summary.setString(2, company2_site2_station2_test2_summary(1).asInstanceOf[String])
  prepare_insert_summary.setString(3, company2_site2_station2_test2_summary(2).asInstanceOf[String])
  prepare_insert_summary.setString(4, company2_site2_station2_test2_summary(3).asInstanceOf[String])
  prepare_insert_summary.setTimestamp(5, testStartTime)
  prepare_insert_summary.setTimestamp(6, testStopTime)
  prepare_insert_summary.setString(7, company2_site2_station2_test2_summary(6).asInstanceOf[String])
  prepare_insert_summary.setString(8, company2_site2_station2_test2_summary(7).asInstanceOf[String])
  prepare_insert_summary.setString(9, company2_site2_station2_test2_summary(8).asInstanceOf[String])
  prepare_insert_summary.setString(10, company2_site2_station2_test2_summary(9).asInstanceOf[String])
  prepare_insert_summary.setString(11, company2_site2_station2_test2_summary(10).asInstanceOf[String])
  prepare_insert_summary.setString(12, company2_site2_station2_test2_summary(11).asInstanceOf[String])
  prepare_insert_summary.executeUpdate()

  // Insert Check - Measurements Summary
  val rsSummary = con.prepareStatement(s"SELECT * FROM ${com.epidata.lib.models.MeasurementSummary.DBTableName}").executeQuery()
  while (rsSummary.next()) {
    val t = com.epidata.lib.models.MeasurementSummary.resultSetToMeasurementSummary(rsSummary)
    println(s"Insert Check: ${t.toString}")
  }
  //println()

  // val fieldQuerySummary = new JLinkedHashMap[String, JList[String]]()
  // fieldQuerySummary.put("company", JArrays.asList("Company-2", "Company-1"))
  // fieldQuerySummary.put("site", JArrays.asList("Site-1", "Site-2"))
  // fieldQuerySummary.put("device_group", JArrays.asList("1000"))
  // fieldQuerySummary.put("tester", JArrays.asList("Station-1", "Station-2"))
  // fieldQuerySummary.put("test_name", JArrays.asList("Test-1", "Test-2"))

  // val resultsSummary = ec.queryMeasurementSummary(
  //   fieldQuerySummary,
  //   beginTime,
  //   endTime)
  // val measSummaryIter = resultsSummary.iterator()
  // while (measSummaryIter.hasNext()) {
  //   println(s"meas_summary query row: ${measSummaryIter.next()}")
  // }

  // Measurement Keys
  //println("----------------------------------------------------")
  println("\n")
  val insert_keys_val = Array[String]("Company-2", "Site-2", "2000", "Station-2")
  val insert_keys_val_1 = Array[String]("Company-3", "Site-3", "3000", "Station-3")
  val insert_keys_query = s"INSERT OR REPLACE INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) VALUES (?, ?, ?, ?)"
  val keys_stmt = con.prepareStatement(insert_keys_query)
  keys_stmt.setString(1, insert_keys_val.head)
  keys_stmt.setString(2, insert_keys_val(1))
  keys_stmt.setString(3, insert_keys_val(2))
  keys_stmt.setString(4, insert_keys_val(3))
  keys_stmt.executeUpdate()
  keys_stmt.setString(1, insert_keys_val_1.head)
  keys_stmt.setString(2, insert_keys_val_1(1))
  keys_stmt.setString(3, insert_keys_val_1(2))
  keys_stmt.setString(4, insert_keys_val_1(3))
  keys_stmt.executeUpdate()
  // Insert Check
  val k_rs = con.prepareStatement(s"SELECT * FROM ${com.epidata.lib.models.MeasurementsKeys.DBTableName}").executeQuery()
  var k_vals = ""
  while (k_rs.next()) {
    for (t <- 1 to 4) {
      k_vals += k_rs.getString(t) + " "
    }
    println(s"keys insert check: ${k_vals.slice(0, k_vals.length - 1)}")
    k_vals = ""
  }
  val k_results = ec.listKeys()
  val keysIter = k_results.iterator()
  while (keysIter.hasNext()) {
    println(s"key query row: ${keysIter.next()}")
  }
  try { k_rs.close() } catch { case e: SQLException => println("Error closing ResultSet") }
  try { rsOriginal.close() } catch { case e: SQLException => println("Error closing Measurement Original ResultSet") }
  try { rsCleansed.close() } catch { case e: SQLException => println("Error closing Measurement Cleansed ResultSet") }
  try { rsSummary.close() } catch { case e: SQLException => println("Error closing Measurement Summary ResultSet") }
  try { keys_stmt.close() } catch { case e: SQLException => println("Error closing Statement") }
  try { prepare_insert.close() } catch { case e: SQLException => println("Error closing Statement") }
  try { prepare_insert_cleansed.close() } catch { case e: SQLException => println("Error closing Statement") }
  try { prepare_insert_summary.close() } catch { case e: SQLException => println("Error closing Statement") }
  try { stmt.close() } catch { case e: SQLException => println("Error closing Statement") }
  try { con.close() } catch { case e: SQLException => println("Error closing database connection") }
  println("\n EpiDataLite Batch Query Test completed")
  println("----------------------------------------------------")
  */

  //  ----- EpiDataLite Stream Test Started -----
  println("\n EpiDataLite Stream Test Started")

  val esc = new EpidataLiteStreamingContext()

  for (i <- Seq(1, 2, 3)) {

    esc.init()

    // def addShutdownHook(esc: EpidataLiteStreamingContext): Unit = {
    //  Runtime.getRuntime().addShutdownHook(new Thread { override def run() { esc.stopStream() } })
    // }

    // addShutdownHook(esc)

    //  println("Enter 'Q' to stop streaming DEBUGGING 0")
    //  while ((StdIn.readChar()).toLower.compare('q') != 0) {
    //    println("Continuing streaming. Enter 'Q' to stop streaming.")
    //  }

    // Create Transformation
    //    val op1 = esc.createTransformation("Identity", List("Temperature", "Wind_Speed", "Relative_Humidity"), Map[String, String]())
    val op1 = esc.createTransformation("Identity", List("Temperature_1", "Voltage_1", "Current_1", "Temperature_2", "Voltage_2", "Current_2"), Map[String, String]())
    println("transformation created: " + op1)

    //    val op2 = esc.createTransformation("FillMissingValue", List("Temperature", "Wind_Speed", "Relative_Humidity"), Map("method" -> "rolling", "s" -> 3))
    val op2 = esc.createTransformation("FillMissingValue", List("Temperature_1", "Voltage_1", "Current_1", "Temperature_2", "Voltage_2", "Current_2"), Map("method" -> "rolling", "s" -> 3))
    println("transformation created: " + op2)

    class CustomFillMissingValue(
        meas_names: List[String],
        method: String,
        s: Int) extends FillMissingValue(
      meas_names: List[String],
      method: String,
      s: Int) {
      override def apply(measurements: ListBuffer[java.util.Map[String, Object]]): ListBuffer[java.util.Map[String, Object]] = {

        val field = "meas_value"

        //convert to an odd number so there are even number of measuremnts on either side
        val size = if (s % 2 == 0) s + 1 else s

        method match {
          case "rolling" =>
            var filteredMeasCollection = new ListBuffer[java.util.Map[String, Object]]()

            for (meas_name <- meas_names) {
              val filteredMeas = measurements
                .filter(m => meas_name.equals(m.get("meas_name").asInstanceOf[String]))

              for (index <- filteredMeas.indices) {
                // If value is null, then substitute it!
                if (filteredMeas(index).get("meas_value") == null) {
                  var sum: Double = 0
                  var count = 0

                  for (i <- index - (size - 1) / 2 to index + (size + 1) / 2) {
                    if (i >= 0 && i < filteredMeas.size && filteredMeas(i).get("meas_value") != null) {
                      sum = sum + filteredMeas(i).get("meas_value").asInstanceOf[Double]
                      count = count + 1
                    }
                  }

                  if (count > 0) {
                    val measValue = (sum / count).asInstanceOf[Object]
                    filteredMeas(index).put("meas_value", measValue)
                    filteredMeas(index).put("meas_flag", "custom_substituted")
                    filteredMeas(index).put("meas_method", method)
                  }
                }
              }

              filteredMeasCollection ++= filteredMeas

            }
            filteredMeasCollection
        }
      }
    }

    val CustomOp2 = new CustomFillMissingValue(List("Temperature", "Wind_Speed", "Relative_Humidity"), "rolling", 3)
    println("transformation created: " + CustomOp2)

    var list = new JArrayList[String]()
    list.add("Temperature")
    list.add("Wind_Speed")
    list.add("Relative_Humidity")
    val mutableMap = new JHashMap[String, String]
    //  val op3 = esc.createTransformation("Identity", list, mutableMap)
    //  println("transformation created: " + op3)

    //  val op4 = esc.createTransformation("Identity", List("Temperature", "Wind_Speed", "Relative_Humidity"), Map[String, String]())
    //  println("transformation created: " + op4)

    //  val op5 = esc.createTransformation("Identity", List("Temperature", "Wind_Speed", "Relative_Humidity"), Map[String, String]())
    //  println("transformation created: " + op5)

    //    val op6 = esc.createTransformation("MeasStatistics", List("Temperature", "Wind_Speed", "Relative_Humidity"), Map("method" -> "standard"))
    val op6 = esc.createTransformation("MeasStatistics", List("Temperature_1", "Voltage_1", "Current_1", "Temperature_2", "Voltage_2", "Current_2"), Map("method" -> "standard"))
    println("transformation created: " + op6)

    // val op7 = esc.createTransformation("Resample", List("Temperature"), Map("time_interval" -> 1, "timeunit" -> "min"))
    // println("transformation created: " + op7)

    // val op8 = esc.createTransformation("NAs", List("Temperature"), Map[String, String]())
    // println("transformation created: " + op8)

    // val op9 = esc.createTransformation("Transpose", List("Temperature", "Wind_Speed", "Relative_Humidity"), Map[String, String]())
    // println("transformation created: " + op9)

    // val op10 = esc.createTransformation("InverseTranspose", List("Temperature", "Wind_Speed", "Relative_Humidity"), Map[String, String]())
    // println("transformation created: " + op10)

    // val op11 = esc.createTransformation("OutlierDetector", List("Temperature", "Wind_Speed", "Relative_Humidity"), Map("method" -> "quartile"))
    // println("transformation created: " + op11)

    // val op12 = esc.createTransformation("Outliers", List("Temperature", "Wind_Speed", "Relative_Humidity"), Map("mpercentage" -> 25, "method" -> "quartile"))
    // println("transformation created: " + op12)

    // Create Streams

    //     play producer
    //           |
    //           ↓
    //          op1 --------
    //         /     \      |
    //        ↓       ↓     |
    //   --- op2     op3    |
    //  |     | \     /     |
    //  |     |  ↓   ↓      |
    //  |     |   op4       |
    //  |     |   |         |
    //  |     \   |         |
    //  |      \  |         |
    //  |       \ |         |
    //  |        ↓          ↓
    //  |        op5       op6
    //   \        |        /
    //     \      |      /
    //       \    ↓    /
    //    play datasink

    //op2
    esc.createStream("measurements_original", "measurements_substituted", op2)
    // println("STREAM 1 created: " + op2)

    //CustomOp2
    // esc.createStream("measurements_original", "measurements_substituted", CustomOp2)
    // println("STREAM 1 created: " + CustomOp2)

    //op1
    esc.createStream("measurements_substituted", "measurements_cleansed", op1)
    // println("STREAM 2 created: " + op1)

    //op3
    //  esc.createStream("measurements_intermediate_1", "measurements_intermediate_3", op3)
    //  println("STREAM 3 created: " + op3)

    // esc.createStream("measurements_original", "measurements_intermediate", op9)
    // println("STREAM 9 created: " + op9)

    // esc.createStream("measurements_intermediate", "measurements_cleansed", op10)
    // println("STREAM 10 created: " + op10)

    //op4
    val op4topics = ListBuffer[String]()
    op4topics += "measurements_intermediate_2"
    op4topics += "measurements_intermediate_3"
    val op4buffers = ListBuffer[Integer]()
    op4buffers += 5
    op4buffers += 12
    //  println(op4buffers)
    //  esc.createStream(op4topics, op4buffers, "measurements_intermediate_4", op4)
    //  println("STREAM 4 created: " + op4 + "\n")

    //op5 - measurements_cleansed
    val op5topics = ListBuffer[String]()
    op5topics += "measurements_intermediate_2"
    op5topics += "measurements_intermediate_4"
    //  esc.createStream(op5topics, "measurements_cleansed", op5)
    //  println("STREAM 5 created: " + "\n")

    //op6 - measurements_summary
    esc.createStream("measurements_substituted", "measurements_summary", op6)
    // println("STREAM 6 created: " + "\n")

    //op7
    // esc.createStream("measurements_original", "measurements_cleansed", op7)
    // println("STREAM 7 created: " + op7)

    //op8
    // esc.createStream("measurements_original", "measurements_cleansed", op8)
    // println("STREAM 8 created: " + op8)

    //op11
    // esc.createStream("measurements_original", "measurements_cleansed", op11)
    // println("STREAM 11 created: " + op11)

    //op12
    // esc.createStream("measurements_original", "measurements_cleansed", op12)
    // println("STREAM 12 created: " + op12)

    //op2 - measurements_cleansed
    //  esc.createStream("measurements_original", "measurements_cleansed", op2)
    //  println("STREAM 7 created: " + "\n")

    // Start Stream
    esc.startStream()
    println("--------Stream started successfully--------")

    // check stream data in SQLite database

    println("Enter 'Q' to stop streaming")
    while ((StdIn.readChar()).toLower.compare('q') != 0) {
      println("Continuing streaming. Enter 'Q' to stop streaming.")
    }

    //Thread.sleep(10000)

    // Stop stream
    try {
      println("Stream being stopped in Test code")
      esc.stopStream()
      println("Stream processing stopped successfully.")
    } catch {
      case e: Throwable => println("Exception while stopping stream")
    }

    // Clear (reset) EpiDataLiteStreamingContext
    esc.clear()
  }

  println("\n EpiDataLite Stream Test completed")
  println("----------------------------------------------------")

}
