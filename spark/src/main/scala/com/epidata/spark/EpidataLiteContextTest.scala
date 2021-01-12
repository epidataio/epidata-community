package com.epidata.spark

//import com.epidata.spark.{ EpidataLiteContext, Measurement }
//import com.epidata.lib.models.Measurement
import com.datastax.driver.core.querybuilder.QueryBuilder

import java.sql.{ Date, DriverManager, Timestamp }

object elcTest extends App {
  val ec = new EpidataLiteContext()
  val con = DriverManager.getConnection("jdbc:sqlite:/Users/JFu/Desktop/empty.db")
  val stmt = con.createStatement()

  // Clear table
  val delete_meas_command = s"DELETE FROM ${com.epidata.lib.models.Measurement.DBTableName}"
  //  val delete_meask_command = s"DELETE FROM ${com.epidata.lib.models.MeasurementsKeys.DBTableName}"

  stmt.execute(delete_meas_command)
  //  con.createStatement().execute(delete_meask_command)

  // Create Table
  val create_t = s"CREATE TABLE IF NOT EXISTS ${com.epidata.lib.models.Measurement.DBTableName} (customer TEXT, customer_site TEXT, collection TEXT, dataset TEXT, epoch INT, ts DATE, key1 TEXT, key2 TEXT,key3 TEXT, meas_value DOUBLE, meas_unit TEXT, meas_status DOUBLE, meas_lower_limit DOUBLE, meas_upper_limit DOUBLE, meas_description TEXT, val1 TEXT, val2 TEXT, PRIMARY KEY (customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3))"
  stmt.execute(create_t)

  // Manual Insert
  val beginTime = new Timestamp(1428004316123L)
  val endTime = new Timestamp(1428004316123L + 10000L)
  val ts = beginTime
  val epoch = Measurement.epochForTs(beginTime)
  val insert_ts = new Date(ts.getTime)

  val to_check = Array("Company-1", "Site-1", "1000", "Station-1", epoch, ts, "100001", "Test-1",
    "Meas-1", 45.7, "degree C", "PASS", 40.0, 90.0, "Description", "PASS", "PASS")
  val columns = Array("customer", "customer_site",
    "collection", "dataset", "epoch", "ts", "key1", "key2", "key3", "meas_value", "meas_unit",
    "meas_status", "meas_lower_limit", "meas_upper_limit", "meas_description", "val1", "val2")

  //  val insert_q = s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} " +
  //    "(customer, customer_site, collection, dataset, epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, meas_upper_limit, meas_description, val1, val2)" +
  //    s"VALUES (${"Company-1"}, ${"Site-1"}, ${"1000"}, ${"Station-1"}, ${epoch}, ${insert_ts}, ${"100001"}, ${"Test-1"}, ${"Meas-1"}, ${45.7}, ${"degreeC"}, ${"PASS"}, ${40.0}, ${90.0}, ${"Description"}, ${"PASS"}, ${"PASS"})"

  var insert_q = QueryBuilder.insertInto(com.epidata.lib.models.Measurement.DBTableName)
  for (i <- columns.indices) {
    insert_q = insert_q.value(columns(i), to_check(i))
  }
  stmt.executeUpdate(insert_q.toString)

  // Insert Check
  val rs = stmt.executeQuery(QueryBuilder.select().all().from(com.epidata.lib.models.Measurement.DBTableName).toString)
  while (rs.next()) {
    val t = com.epidata.lib.models.Measurement.rowToMeasurement(rs)
    print(t.toString)
  }

  val results = ec.query(
    Map(
      "company" -> List("Company-1"),
      "site" -> List("Site-1"),
      "device_group" -> List("1000"),
      "tester" -> List("Station-1")),
    beginTime,
    endTime)

  //  print(results.length)
  con.close()
  //  for (i <- results) {
  //    print(i)
  //  }
  //  for (i <- to_check.indices) {
  //    if (results(i) != to_check(i)) {
  //      print(s"Error with ${columns(i)}")
  //    }
  //  }
}
