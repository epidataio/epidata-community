/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/
package ipython
//import java.nio.file.{Files, Paths, StandardCopyOption}
//
//import com.epidata.lib.models.util.Datatype
import com.epidata.spark.{EpidataLiteContext, Measurement}
//import java.nio.ByteBuffer
import java.sql.Timestamp

import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import matchers.should._

import scala.sys.process.Process
import java.sql.{DriverManager, SQLException, Types}

import com.typesafe.config.ConfigFactory

import scala.io.Source




object IPythonSpecLite extends App{

  try { // create a database connection

    val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
    val con = DriverManager.getConnection("jdbc:sqlite:data/epidata_development.db")
    val session = con.createStatement()
    val ts = (0 to 5).map(x => new Timestamp(1428004316123L + x * 1000))
    val epoch = ts.map(Measurement.epochForTs(_))

    //create table
    val cleansed = "play/conf/schema//measurements_cleansed"
    val keys = "play/conf/schema/measurements_keys"
    val original = "play/conf/schema/measurements_original"
    val summary = "play/conf/schema/measurements_summary"
    val users = "play/conf/schema/users"
    val sql1 = Source.fromFile(cleansed).getLines.mkString
    val sql2 = Source.fromFile(keys).getLines.mkString
    val sql3 = Source.fromFile(original).getLines.mkString
    val sql4 = Source.fromFile(summary).getLines.mkString
    val sql5 = Source.fromFile(users).getLines.mkString
    session.executeUpdate(sql1)
    session.executeUpdate(sql2)
    session.executeUpdate(sql3)
//    session.createStatement().executeUpdate(sql4)
//    session.createStatement().executeUpdate(sql5)

    session.execute(s"DELETE FROM ${com.epidata.lib.models.Measurement.DBTableName}")
    session.execute(s"DELETE FROM  ${com.epidata.lib.models.MeasurementsKeys.DBTableName}")
    session.execute(s"DELETE FROM  ${com.epidata.lib.models.MeasurementCleansed.DBTableName}")


    //double measurement 1
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(0)}, ${ts(0).getTime}, '100001', 'Test-1', 'Meas-1', 45.7, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementCleansed.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_flag, meas_method, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(0)}, ${ts(0).getTime}, '100001', 'Test-1', 'Meas-1', 45.7, 'degree C', " +
      s"'PASS', 'dummyFlag', 'dummyMethod', 40.0, 90.0, 'Description', 'PASS', 'PASS')")

    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")

    // Another double measurement. 2
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(1)}, ${ts(1).getTime}, '101001', 'Test-1', 'Meas-2', 49.1, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementCleansed.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_flag, meas_method, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(1)}, ${ts(1).getTime}, '101001', 'Test-1', 'Meas-2', 49.1, 'degree C', " +
      s"'PASS', 'dummyFlag', 'dummyMethod', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT OR IGNORE INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")

    // A double measurement with a different partition key. 3
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-2', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(1)}, ${ts(1).getTime}, '101001', 'Test-1', 'Meas-2', 49.1, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementCleansed.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_flag, meas_method, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-2', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(1)}, ${ts(1).getTime}, '101001', 'Test-1', 'Meas-2', 49.1, 'degree C', " +
      s"'PASS','dummyFlag', 'dummyMethod', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-2', 'Site-1', '1000', 'Station-1')")

    // Another double measurement with a different partition key. 4
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-3', ${epoch(1)}, ${ts(1).getTime}, '101001', 'Test-1', 'Meas-2', 49.1, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementCleansed.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_flag, meas_method, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-3', ${epoch(1)}, ${ts(1).getTime}, '101001', 'Test-1', 'Meas-2', 49.1, 'degree C', " +
      s"'PASS', 'dummyFlag', 'dummyMethod', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-3')")

    // A large long measurement. 5
    val large = 3448388841L
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value_l, meas_unit, meas_status, meas_lower_limit_l, " +
      s"meas_upper_limit_l, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(2)}, ${ts(2).getTime}, '101001', 'Test-3', 'Meas-2', $large, 'ns', " +
      s"'PASS', ${large - 1}, ${large + 1}, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT OR IGNORE INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")

    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementCleansed.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value_l, meas_unit, meas_status, meas_flag, meas_method, meas_lower_limit_l, " +
      s"meas_upper_limit_l, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(2)}, ${ts(2).getTime}, '101001', 'Test-3', 'Meas-2', $large, 'ns', " +
      s"'PASS', 'dummyFlag', 'dummyMethod', ${large - 1}, ${large + 1}, 'Description', 'PASS', 'PASS')")

    // A string measurement. 6
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value_s, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(3)}, ${ts(3).getTime}, '101001', 'Test-4', 'Meas-2', 'POWER ON', NULL, " +
      s"'PASS', NULL, NULL, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementCleansed.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value_s, meas_unit, meas_status, meas_flag, meas_method, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(3)}, ${ts(3).getTime}, '101001', 'Test-4', 'Meas-2', 'POWER ON', NULL, " +
      s"'PASS', 'dummyFlag', 'dummyMethod', NULL, NULL, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT OR IGNORE INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")

    // A binary array measurement. 7
    // using bytes array instead of bytes buffer in sqlite
    //ToDo:  Data type mismatch between iPythonSpecLite binary array and Scala Bytebuffer
    //
//    val array = new Array[Byte](1 + 4 * 8)
//    val buffer = ByteBuffer.wrap(array)
//    buffer.put(Datatype.DoubleArray.id.toByte)
//    buffer.putDouble(0.1111)
//    buffer.putDouble(0.2222)
//    buffer.putDouble(0.3333)
//    buffer.putDouble(0.4444)
//    val state = s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
//      s"epoch, ts, key1, key2, key3, meas_value_b, meas_unit, meas_status, meas_lower_limit, " +
//      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '2000', " +
//      s"'Station-1', ${epoch(4)}, ${ts(4).getTime}, '101001', 'Test-5', 'Meas-2', ? , 'V', " +
//      s"'PASS', NULL, NULL, 'Description', 'PASS', 'PASS')"
//    val stateCleansed = s"INSERT INTO ${com.epidata.lib.models.MeasurementCleansed.DBTableName} (customer, customer_site, collection, dataset, " +
//      s"epoch, ts, key1, key2, key3, meas_value_b, meas_unit, meas_status, meas_flag, meas_method, meas_lower_limit, " +
//      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
//      s"'Station-1', ${epoch(4)}, ${ts(4).getTime}, '101001', 'Test-5', 'Meas-2', ? , 'V', " +
//      s"'PASS', 'dummyFlag', 'dummyMethod', NULL, NULL, 'Description', 'PASS', 'PASS')"
//    val statement = con.prepareStatement(state)
//    val statementCleansed = con.prepareStatement(stateCleansed)
////    for(a <- buffer.array()){
////      print(a + " ")
////    }
//    statement.setObject(1, buffer.array(), java.sql.Types.BLOB)
//    statementCleansed.setObject(1, buffer.array(), java.sql.Types.BLOB)
//    statement.executeUpdate()
//    statementCleansed.executeUpdate()
//    statement.close()
//    statementCleansed.close()

    session.execute(s"INSERT OR IGNORE INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")
//    //ToDo:  Data type mismatch between iPythonSpecLite binary array and Scala Bytebuffer
//    // A binary waveform measurement. 8
//    // using bytes array instead of bytes buffer in sqlite
//    val waveform = new Array[Byte](1 + 8 + 8 + 3 * 8)
//    val buffer2 = ByteBuffer.wrap(waveform)
//    buffer2.put(Datatype.Waveform.id.toByte)
//    buffer2.putLong(ts(5).getTime)
//    buffer2.putDouble(0.1234)
//    buffer2.putDouble(0.5678)
//    buffer2.putDouble(0.9012)
//    buffer2.putDouble(0.3456)
//    val state2 = s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
//      s"epoch, ts, key1, key2, key3, meas_value_b, meas_unit, meas_status, meas_lower_limit, " +
//      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
//      s"'Station-1', ${epoch(5)}, ${ts(5).getTime}, '101001', 'Test-6', 'Meas-2', ?, 'V', " +
//      s"'PASS', NULL, NULL, 'Description', 'PASS', 'PASS')"
//    val stateCleansed2 = s"INSERT INTO ${com.epidata.lib.models.MeasurementCleansed.DBTableName} (customer, customer_site, collection, dataset, " +
//      s"epoch, ts, key1, key2, key3, meas_value_b, meas_unit, meas_status, meas_flag, meas_method, meas_lower_limit, " +
//      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
//      s"'Station-1', ${epoch(5)}, ${ts(5).getTime}, '101001', 'Test-6', 'Meas-2', ?, 'V', " +
//      s"'PASS', 'dummyFlag', 'dummyMethod', NULL, NULL, 'Description', 'PASS', 'PASS')"
//    val statement2 = con.prepareStatement(state2)
//    val statementCleansed2 = con.prepareStatement(stateCleansed2)
//    val arr2 = buffer2.array()
//        for( a <- arr2){
//          print(a);
//        }
//
//    statement2.setObject(1, buffer2.array(), java.sql.Types.BLOB)
//    statementCleansed2.setObject(1, buffer2.array(), java.sql.Types.BLOB)
//    statement2.executeUpdate()
//    statementCleansed2.executeUpdate()
//    statement2.close()
//    statementCleansed2.close()

    session.execute(s"INSERT OR IGNORE INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")





//    val rs = session.executeQuery(s"select * from ${com.epidata.lib.models.Measurement.DBTableName} where key2 = 'Test-5'")
//    while ( {
//      rs.next
//    }) { // read the result set
////      print(rs)
//      System.out.println("customer = " + rs.getString("customer"))
//      System.out.println("customer_site = " + rs.getString("customer_site"))
//      System.out.println("collection = " + rs.getString("collection"))
//      System.out.println("dataset = " + rs.getString("dataset"))
//      System.out.println("ts = " + rs.getTimestamp("ts"))
//      System.out.println("key2 = " + rs.getString("key2"))
//      //System.out.println("flag = " + rs.getString("meas_flag"))
//      System.out.println("mess_b = " + rs.getObject("meas_value_b"))
//      System.out.println()
//      val ar = rs.getBytes("meas_value_b");
//
//      for(a <- ar){
//        print(a + " ")
//      }
//
//
//    }
//   rs.close()
   session.close()
    println()
//   con.close()

    println("ts(4).getTime:", ts(4).getTime)
    println("ts(4)", ts(4))


//
   Process(List("python",
     "ipython/test/test_automated_test_lite.py")
   ).run()



  } catch {
    case e: SQLException =>
      // if the error message is "out of memory",
      // it probably means no database file is found
      System.err.println(e.getMessage)
  }
  finally {


  }
  println("Hello, World!")

}