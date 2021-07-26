/*
 * Copyright (c) 2015-2021 EpiData, Inc.
*/

package com.epidata.spark

import com.typesafe.config.ConfigFactory
import java.io.File
import java.sql.{ Connection, DriverManager, Timestamp, SQLException }
import java.util
import scala.io.Source
import scala.io.StdIn
import scala.util.Properties
//import scala.collection.mutable.Map

object elcTest extends App {
  val ec = new EpidataLiteContext()

  /*  ----- EpiDataLite Batch  Test ----- */

  println("\n EpiDataLite Batch Test Started")

  Class.forName("org.sqlite.JDBC");
  private val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  private val basePath = new java.io.File(".").getAbsoluteFile().getParent()
  private val dbName = conf.getString("spark.epidata.SQLite.test.dbFileName")
  private val dbUrl = "jdbc:sqlite:" + basePath + "/data/" + dbName

  // println("sqlite db url: " + dbUrl)

  private val con: Connection = DriverManager.getConnection(dbUrl)
  //  val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  //  val con = DriverManager.getConnection(conf.getString("spark.epidata.SQLite.url"))
  val stmt = con.createStatement()

  // Clear tables
  val dop_orig_command = s"DROP TABLE IF EXISTS ${com.epidata.lib.models.Measurement.DBTableName}"
  val drop_keys_command = s"DROP TABLE IF EXISTS ${com.epidata.lib.models.MeasurementsKeys.DBTableName}"
  stmt.execute(dop_orig_command)
  stmt.execute(drop_keys_command)

  // Create Tables
  val original = "play/conf/schema/measurements_original"
  val keys = "play/conf/schema/measurements_keys"
  val orig_source = Source.fromFile(original)
  val keys_source = Source.fromFile(keys)
  val create_orig = orig_source.getLines.mkString
  val create_key = keys_source.getLines.mkString

  orig_source.close()
  keys_source.close()
  //println(s"measurements_original schema is ${create_orig}")
  //println(s"measurements_keys schema is ${create_key}")
  stmt.execute(create_orig)
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

  // Insert Check
  val rs = con.prepareStatement(s"SELECT * FROM ${com.epidata.lib.models.Measurement.DBTableName}").executeQuery()
  while (rs.next()) {
    val t = com.epidata.lib.models.Measurement.rowToMeasurement(rs)
    //println(s"Insert Check: ${t.toString}")
  }
  //println()

  val results = ec.query(
    Map(
      "company" -> List("Company-2", "Company-1"),
      "site" -> List("Site-1", "Site-2"),
      "device_group" -> List("1000"),
      "tester" -> List("Station-1", "Station-2"),
      "test_name" -> List("Test-1", "Test-2")),
    beginTime,
    endTime)

  val measIter = results.iterator()
  while (measIter.hasNext()) {
    println(s"meas_orig query row: ${measIter.next()}")
  }

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
  try { rs.close() } catch { case e: SQLException => println("Error closing ResultSet") }
  try { keys_stmt.close() } catch { case e: SQLException => println("Error closing Statement") }
  try { prepare_insert.close() } catch { case e: SQLException => println("Error closing Statement") }
  try { stmt.close() } catch { case e: SQLException => println("Error closing Statement") }
  try { con.close() } catch { case e: SQLException => println("Error closing database connection") }

  println("\n EpiDataLite Batch Query Test completed")
  println("----------------------------------------------------")

  /*  ----- EpiDataLite Stream Test Started ----- */
  println("\n EpiDataLite Stream Test Started")

  val esc = new EpidataLiteStreamingContext()
  esc.init()

  //  println("Enter 'Q' to stop streaming DEBUGGING 0")
  //  while ((StdIn.readChar()).toLower.compare('q') != 0) {
  //    println("Continuing streaming. Enter 'Q' to stop streaming.")
  //  }

  // Create Transformation
  val op1 = esc.createTransformations("Identity", List("Meas-1"), Map[String, String]())
  println("transformation created: " + op1)

  val op2 = esc.createTransformations("Identity", List("Meas-1"), Map[String, String]())
  println("transformation created: " + op2)

  var list = new util.ArrayList[String]()
  list.add("Meas-1")
  val mutableMap = new util.HashMap[String, String]
  val op3 = esc.createTransformations("Identity", list, mutableMap)
  println("transformation created: " + op3)

  //  println("Enter 'Q' to stop streaming DEBUGGING 1")
  //  while ((StdIn.readChar()).toLower.compare('q') != 0) {
  //    println("Continuing streaming. Enter 'Q' to stop streaming.")
  //  }

  // Create Stream
  //  esc.createStream("measurements_original", "measurements_intermediate", op1)
  //  println("stream 1 created: " + op1)

  //  println("Enter 'Q' to stop streaming DEBUGGING 2")
  //  while ((StdIn.readChar()).toLower.compare('q') != 0) {
  //    println("Continuing streaming. Enter 'Q' to stop streaming.")
  //  }

  //  esc.createStream("measurements_intermediate", "measurements_cleansed", op2)
  //  println("stream 2 created: " + op2)

  //  println("Enter 'Q' to stop streaming DEBUGGING 3")
  //  while ((StdIn.readChar()).toLower.compare('q') != 0) {
  //    println("Continuing streaming. Enter 'Q' to stop streaming.")
  //  }

  esc.createStream("measurements_original", "measurements_cleansed", op3)
  println("stream 3 created: " + op3)

  //  println("Enter 'Q' to stop streaming DEBUGGING 4")
  //  while ((StdIn.readChar()).toLower.compare('q') != 0) {
  //    println("Continuing streaming. Enter 'Q' to stop streaming.")
  //  }

  esc.testUnit()
  print(esc.printSomething(""))

  // Start Stream
  esc.startStream()
  println("Stream started successfully")

  // check stream data in SQLite database

  println("Enter 'Q' to stop streaming")
  while ((StdIn.readChar()).toLower.compare('q') != 0) {
    println("Continuing streaming. Enter 'Q' to stop streaming.")
  }

  // Stop stream
  esc.stopStream()

  println("Stream processing stoppqed successfully.")

  println("\n EpiDataLite Stream Test completed")
  println("----------------------------------------------------")
}
