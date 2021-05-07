/*
 * Copyright (c) 2015-2021 EpiData, Inc.
*/

package com.epidata.spark

import com.typesafe.config.ConfigFactory
import java.io.File
import java.sql.{ DriverManager, Timestamp, SQLException }
import scala.io.Source

object elcTest extends App {
  val ec = new EpidataLiteContext()
  val conf = ConfigFactory.parseResources("sqlite-defaults.conf")

  Class.forName("org.sqlite.JDBC");
  val con = DriverManager.getConnection(conf.getString("spark.epidata.SQLite.url"))
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

  val sample_station1_test1 = Array("Company-1", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 22.2, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 20.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val sample_station1_test2 = Array("Company-1", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 33.3, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 30.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val sample_station2_test1 = Array("Company-1", "Site-1", "1000", "Station-2", epoch, ts, "100001", "Test-1",
    "Meas-1", "just_a_check", 44.4, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 40.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")
  val sample_station2_test2 = Array("Company-1", "Site-1", "1000", "Station-2", epoch, ts, "100001", "Test-2",
    "Meas-1", "just_a_check", 55.5, meas_value_l, "meas_value_s", meas_value_b, "degree C", "PASS", 50.0, meas_lower_limit_l, 90.0, meas_upper_limit_l, "Description", "PASS", "PASS")

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

  val prepare_insert = con.prepareStatement(insert_q.toString)
  prepare_insert.setString(1, sample_station1_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, sample_station1_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, sample_station1_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, sample_station1_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, sample_station1_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, sample_station1_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, sample_station1_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, sample_station1_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, sample_station1_test1(10).asInstanceOf[Double])
  prepare_insert.setString(12, sample_station1_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, sample_station1_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, sample_station1_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, sample_station1_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, sample_station1_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, sample_station1_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, sample_station1_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, sample_station1_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, sample_station1_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, sample_station1_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, sample_station1_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, sample_station1_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, sample_station1_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, sample_station1_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, sample_station1_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, sample_station1_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, sample_station1_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, sample_station1_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, sample_station1_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, sample_station1_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, sample_station1_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, sample_station1_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, sample_station1_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, sample_station1_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, sample_station1_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, sample_station1_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, sample_station1_test2(10).asInstanceOf[Double])
  prepare_insert.setString(12, sample_station1_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, sample_station1_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, sample_station1_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, sample_station1_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, sample_station1_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, sample_station1_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, sample_station1_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, sample_station1_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, sample_station1_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, sample_station1_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, sample_station1_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, sample_station1_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, sample_station1_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, sample_station1_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, sample_station1_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, sample_station1_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, sample_station1_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, sample_station1_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, sample_station2_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, sample_station2_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, sample_station2_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, sample_station2_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, sample_station2_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, sample_station2_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, sample_station2_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, sample_station2_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, sample_station2_test1(10).asInstanceOf[Double])
  prepare_insert.setString(12, sample_station2_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, sample_station2_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, sample_station2_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, sample_station2_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, sample_station2_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, sample_station2_test1(0).asInstanceOf[String])
  prepare_insert.setString(2, sample_station2_test1(1).asInstanceOf[String])
  prepare_insert.setString(3, sample_station2_test1(2).asInstanceOf[String])
  prepare_insert.setString(4, sample_station2_test1(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, sample_station2_test1(6).asInstanceOf[String])
  prepare_insert.setString(8, sample_station2_test1(7).asInstanceOf[String])
  prepare_insert.setString(9, sample_station2_test1(8).asInstanceOf[String])
  prepare_insert.setString(10, sample_station2_test1(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, sample_station2_test1(14).asInstanceOf[String])
  prepare_insert.setString(13, sample_station2_test1(15).asInstanceOf[String])
  prepare_insert.setString(14, sample_station2_test1(20).asInstanceOf[String])
  prepare_insert.setString(15, sample_station2_test1(21).asInstanceOf[String])
  prepare_insert.setString(16, sample_station2_test1(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, sample_station2_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, sample_station2_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, sample_station2_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, sample_station2_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, beginTime)
  prepare_insert.setString(7, sample_station2_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, sample_station2_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, sample_station2_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, sample_station2_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, sample_station2_test2(10).asInstanceOf[Double])
  prepare_insert.setString(12, sample_station2_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, sample_station2_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, sample_station2_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, sample_station2_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, sample_station2_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  prepare_insert.setString(1, sample_station2_test2(0).asInstanceOf[String])
  prepare_insert.setString(2, sample_station2_test2(1).asInstanceOf[String])
  prepare_insert.setString(3, sample_station2_test2(2).asInstanceOf[String])
  prepare_insert.setString(4, sample_station2_test2(3).asInstanceOf[String])
  prepare_insert.setInt(5, epoch(0))
  prepare_insert.setTimestamp(6, testTime)
  prepare_insert.setString(7, sample_station2_test2(6).asInstanceOf[String])
  prepare_insert.setString(8, sample_station2_test2(7).asInstanceOf[String])
  prepare_insert.setString(9, sample_station2_test2(8).asInstanceOf[String])
  prepare_insert.setString(10, sample_station2_test2(9).asInstanceOf[String])
  prepare_insert.setDouble(11, 99999.0)
  prepare_insert.setString(12, sample_station2_test2(14).asInstanceOf[String])
  prepare_insert.setString(13, sample_station2_test2(15).asInstanceOf[String])
  prepare_insert.setString(14, sample_station2_test2(20).asInstanceOf[String])
  prepare_insert.setString(15, sample_station2_test2(21).asInstanceOf[String])
  prepare_insert.setString(16, sample_station2_test2(22).asInstanceOf[String])
  prepare_insert.executeUpdate()

  // Insert Check
  val rs = con.prepareStatement(s"SELECT * FROM ${com.epidata.lib.models.Measurement.DBTableName}").executeQuery()
  while (rs.next()) {
    val t = com.epidata.lib.models.Measurement.rowToMeasurement(rs)
    println(s"Insert Check: ${t.toString}")
  }
  println()

  val results = ec.query(
    Map(
      "company" -> List("Company-1"),
      "site" -> List("Site-1"),
      "device_group" -> List("1000"),
      "tester" -> List("Station-1"),
      "test_name" -> List("Test-1")),
    beginTime,
    endTime)

  val measIter = results.iterator()
  while (measIter.hasNext()) {
    println(s"meas_orig query row: ${measIter.next()}")
  }

  // Measurement Keys
  println("----------------------------------------------------")
  val insert_keys_val = Array[String]("Company-2", "Site-2", "2000", "Station-2")
  val insert_keys_val_1 = Array[String]("Company-3", "Site-3", "3000", "Station-3")
  val insert_keys_query = s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) VALUES (?, ?, ?, ?)"
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
  try { k_rs.close() } catch { case e: SQLException => println("Error closing ResultSet") }
  try { rs.close() } catch { case e: SQLException => println("Error closing ResultSet") }
  try { keys_stmt.close() } catch { case e: SQLException => println("Error closing Statement") }
  try { prepare_insert.close() } catch { case e: SQLException => println("Error closing Statement") }
  try { stmt.close() } catch { case e: SQLException => println("Error closing Statement") }
  try { con.close() } catch { case e: SQLException => println("Error closing database connection") }

  val keysIter = k_results.iterator()
  while (keysIter.hasNext()) {
    println(s"keys query row: ${keysIter.next()}")
  }

}
