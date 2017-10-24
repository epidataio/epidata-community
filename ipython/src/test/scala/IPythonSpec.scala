/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

import java.nio.file.{ StandardCopyOption, Files, Paths }

import com.datastax.driver.core.{ Cluster, Session }
import com.epidata.lib.models.util.Datatype
import com.epidata.spark.Measurement
import java.nio.ByteBuffer
import java.sql.Timestamp
import org.scalatest._
import scala.sys.process.Process

class IPythonSpec extends FlatSpec with BeforeAndAfter with Matchers {

  private val hostIp = "127.0.0.1"
  private val cassandraConnectionHost = hostIp
  private val cassandraKeyspaceName = "epidata_test"
  private val cassandraUser = "cassandra"
  private val cassandraPassword = "epidata"
  private val sparkMaster = "local[2]"
  private val sparkUIPort = "4043"

  private var cluster: Cluster = _
  private var session: Session = _

  private val PROCESS_SUCCESS = 0

  private val currentDir = System.getProperty("user.dir")
  private val sparkHome = sys.env("SPARK_HOME")

  private val epidataJarFileName = "epidata-spark-assembly-1.0-SNAPSHOT.jar"
  private val epidataJarFileInSpark = Paths.get(s"$sparkHome/jars/$epidataJarFileName")
  private val epidataJarFilePath = Paths.get(s"$currentDir/spark/target/scala-2.11/$epidataJarFileName")

  before {
    cluster = Cluster.builder().addContactPoint(cassandraConnectionHost).withCredentials(cassandraUser, cassandraPassword).build()
    session = cluster.connect(cassandraKeyspaceName)
    Files.copy(epidataJarFilePath, epidataJarFileInSpark, StandardCopyOption.REPLACE_EXISTING)
  }

  after {
    Files.deleteIfExists(epidataJarFileInSpark)
    session.close
    cluster.close
  }

  private def setupAutomatedTestFixtures() = {

    val ts = (0 to 5).map(x => new Timestamp(1428004316123L + x * 1000))
    val epoch = ts.map(Measurement.epochForTs(_))

    session.execute(s"TRUNCATE ${com.epidata.lib.models.Measurement.DBTableName}")
    session.execute(s"TRUNCATE ${com.epidata.lib.models.MeasurementsKeys.DBTableName}")

    // A double measurement.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(0)}, ${ts(0).getTime}, '100001', 'Test-1', 'Meas-1', 45.7, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")

    // Another double measurement.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(1)}, ${ts(1).getTime}, '101001', 'Test-1', 'Meas-2', 49.1, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")

    // A double measurement with a different partition key.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-2', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(1)}, ${ts(1).getTime}, '101001', 'Test-1', 'Meas-2', 49.1, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-2', 'Site-1', '1000', 'Station-1')")

    // Another double measurement with a different partition key.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-3', ${epoch(1)}, ${ts(1).getTime}, '101001', 'Test-1', 'Meas-2', 49.1, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-3')")

    // A large long measurement.
    val large = 3448388841L
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value_l, meas_unit, meas_status, meas_lower_limit_l, " +
      s"meas_upper_limit_l, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(2)}, ${ts(2).getTime}, '101001', 'Test-3', 'Meas-2', $large, 'ns', " +
      s"'PASS', ${large - 1}, ${large + 1}, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")

    // A string measurement.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value_s, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(3)}, ${ts(3).getTime}, '101001', 'Test-4', 'Meas-2', 'POWER ON', NULL, " +
      s"'PASS', NULL, NULL, 'Description', 'PASS', 'PASS')")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")

    // A binary array measurement.
    val array = new Array[Byte](1 + 4 * 8)
    val buffer = ByteBuffer.wrap(array)
    buffer.put(Datatype.DoubleArray.id.toByte)
    buffer.putDouble(0.1111)
    buffer.putDouble(0.2222)
    buffer.putDouble(0.3333)
    buffer.putDouble(0.4444)
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value_b, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(4)}, ${ts(4).getTime}, '101001', 'Test-5', 'Meas-2', ?, 'V', " +
      s"'PASS', NULL, NULL, 'Description', 'PASS', 'PASS')", ByteBuffer.wrap(array))
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")

    // A binary waveform measurement.
    val waveform = new Array[Byte](1 + 8 + 8 + 3 * 8)
    val buffer2 = ByteBuffer.wrap(waveform)
    buffer2.put(Datatype.Waveform.id.toByte)
    buffer2.putLong(ts(5).getTime)
    buffer2.putDouble(0.1234)
    buffer2.putDouble(0.5678)
    buffer2.putDouble(0.9012)
    buffer2.putDouble(0.3456)
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value_b, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(5)}, ${ts(5).getTime}, '101001', 'Test-6', 'Meas-2', ?, 'V', " +
      s"'PASS', NULL, NULL, 'Description', 'PASS', 'PASS')", ByteBuffer.wrap(waveform))
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', '1000', 'Station-1')")
  }

  "test_automated_test.py" should "run without error" in {

    setupAutomatedTestFixtures

    Process(List(
      "spark-submit",
      "--conf",
      s"spark.cassandra.connection.host=$cassandraConnectionHost",
      "--conf",
      s"spark.ui.port=$sparkUIPort",
      "--conf",
      s"spark.epidata.cassandraKeyspaceName=$cassandraKeyspaceName",
      "--conf",
      s"spark.cassandra.auth.username=$cassandraUser",
      "--conf",
      s"spark.cassandra.auth.password=$cassandraPassword",
      "--conf",
      s"spark.epidata.measurementClass=automated_test",
      "--driver-class-path",
      "spark/target/scala-2.11/epidata-spark-assembly-1.0-SNAPSHOT.jar",
      "--py-files",
      "ipython/dist/epidata-1.0_SNAPSHOT-py2.7.egg",
      "ipython/test/test_automated_test.py"
    ), None,
      "SPARK_CONF_DIR" -> "spark/conf",
      "PYTHONPATH" -> "python",
      "SPARK_MASTER" -> sparkMaster)
      .run()
      .exitValue() should equal(PROCESS_SUCCESS)
  }

  private def setupSensorMeasurementFixtures() = {

    val ts = (0 to 4).map(x => new Timestamp(1428004316123L + x * 1000))
    val epoch = ts.map(Measurement.epochForTs(_))

    session.execute(s"TRUNCATE ${com.epidata.lib.models.Measurement.DBTableName}")
    session.execute(s"TRUNCATE ${com.epidata.lib.models.MeasurementsKeys.DBTableName}")

    // A double measurement.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', 'Station-1', " +
      s"'Sensor-1', ${epoch(0)}, ${ts(0).getTime}, 'Event-1', 'Meas-1', '', 45.7, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', NULL, NULL)")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', 'Station-1', 'Sensor-1')")

    // Another double measurement.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', 'Station-1', " +
      s"'Sensor-1', ${epoch(1)}, ${ts(1).getTime}, 'Event-1', 'Meas-1', '', 49.1, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', NULL, NULL)")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', 'Station-1', 'Sensor-1')")

    // A long measurement.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value_l, meas_unit, meas_status, meas_lower_limit_l, " +
      s"meas_upper_limit_l, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', 'Station-1', " +
      s"'Sensor-1', ${epoch(2)}, ${ts(2).getTime}, 'Event-1', 'Meas-3', '', 51, 'degree C', " +
      s"'PASS', 42, 93, 'Description', NULL, NULL)")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', 'Station-1', 'Sensor-1')")

    // A string measurement.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value_s, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', 'Station-1', " +
      s"'Sensor-1', ${epoch(3)}, ${ts(3).getTime}, 'Event-1', 'Meas-4', '', 'POWER ON', NULL, " +
      s"'PASS', NULL, NULL, 'Description', NULL, NULL)")
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', 'Station-1', 'Sensor-1')")

    // A binary array measurement.
    val array = new Array[Byte](1 + 5 * 8)
    val buffer = ByteBuffer.wrap(array)
    buffer.put(Datatype.DoubleArray.id.toByte)
    buffer.putDouble(0.5555)
    buffer.putDouble(0.6666)
    buffer.putDouble(0.7777)
    buffer.putDouble(0.8888)
    buffer.putDouble(0.9999)
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value_b, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', 'Station-1', " +
      s"'Sensor-1', ${epoch(4)}, ${ts(4).getTime}, 'Event-1', 'Meas-5', '', ?, 'V', " +
      s"'PASS', NULL, NULL, 'Description', 'NULL', 'NULL')", ByteBuffer.wrap(array))
    session.execute(s"INSERT INTO ${com.epidata.lib.models.MeasurementsKeys.DBTableName} (customer, customer_site, collection, dataset) " +
      "VALUES ('Company-1', 'Site-1', 'Station-1', 'Sensor-1')")
  }

  "test_sensor_measurement.py" should "run without error" in {

    setupSensorMeasurementFixtures

    Process(List(
      "spark-submit",
      "--conf",
      s"spark.cassandra.connection.host=$cassandraConnectionHost",
      "--conf",
      s"spark.ui.port=$sparkUIPort",
      "--conf",
      s"spark.epidata.cassandraKeyspaceName=$cassandraKeyspaceName",
      "--conf",
      s"spark.cassandra.auth.username=$cassandraUser",
      "--conf",
      s"spark.cassandra.auth.password=$cassandraPassword",
      "--conf",
      s"spark.epidata.measurementClass=sensor_measurement",
      "--driver-class-path",
      "spark/target/scala-2.11/epidata-spark-assembly-1.0-SNAPSHOT.jar",
      "--py-files",
      "ipython/dist/epidata-1.0_SNAPSHOT-py2.7.egg",
      "ipython/test/test_sensor_measurement.py"
    ), None,
      "SPARK_CONF_DIR" -> "spark/conf",
      "PYTHONPATH" -> "python",
      "SPARK_MASTER" -> sparkMaster)
      .run()
      .exitValue() should equal(PROCESS_SUCCESS)
  }

  private def setupAnalyticsFixtures() = {

    val ts = (0 to 15).map(x => new Timestamp(1428004316123L + x * 1000))
    val epoch = ts.map(Measurement.epochForTs(_))

    session.execute(s"TRUNCATE ${com.epidata.lib.models.Measurement.DBTableName}")

    // A double measurement.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(0)}, ${ts(0).getTime}, '100001', 'Test-1', 'Meas-1', 45.7, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")

    // Another double measurement.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(1)}, ${ts(1).getTime}, '101001', 'Test-1', 'Meas-1', 49.1, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")

    // A third double measurement.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(2)}, ${ts(2).getTime}, '101001', 'Test-1', 'Meas-1', 48.8, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")

    // A measurement with a different meas_name.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(3)}, ${ts(3).getTime}, '101001', 'Test-1', 'Meas-2', 88.8, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")

    // Another measurement with the second meas_name.
    session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
      s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
      s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
      s"'Station-1', ${epoch(4)}, ${ts(4).getTime}, '101001', 'Test-1', 'Meas-2', 77.6, 'degree C', " +
      s"'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")

    // Add measurements for outliers testing.
    for (i <- 0 to 9) {
      session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
        s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
        s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
        s"'Station-2', ${epoch(i)}, ${ts(i).getTime}, '101001', 'Test-1', 'Meas-2', ${77.6 + i / 10.0}, " +
        s"'degree C', 'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    }
    val mildHighOutlier = 80.0
    val extremeHighOutlier = 100.0
    val mildLowOutlier = 76.0
    val extremeLowOutlier = 68.0
    List(mildHighOutlier, extremeHighOutlier, mildLowOutlier, extremeLowOutlier).zipWithIndex.foreach(_ match {
      case (meas_value, i) =>
        session.execute(s"INSERT INTO ${com.epidata.lib.models.Measurement.DBTableName} (customer, customer_site, collection, dataset, " +
          s"epoch, ts, key1, key2, key3, meas_value, meas_unit, meas_status, meas_lower_limit, " +
          s"meas_upper_limit, meas_description, val1, val2) VALUES ('Company-1', 'Site-1', '1000', " +
          s"'Station-2', ${epoch(i + 10)}, ${ts(i + 10).getTime}, '101001', 'Test-1', 'Meas-2', $meas_value, " +
          s"'degree C', 'PASS', 40.0, 90.0, 'Description', 'PASS', 'PASS')")
    })
  }

  "test_analytics.py" should "run without error" in {

    setupAnalyticsFixtures

    Process(List(
      "spark-submit",
      "--conf",
      s"spark.cassandra.connection.host=$cassandraConnectionHost",
      "--conf",
      s"spark.ui.port=$sparkUIPort",
      "--conf",
      s"spark.epidata.cassandraKeyspaceName=$cassandraKeyspaceName",
      "--conf",
      s"spark.cassandra.auth.username=$cassandraUser",
      "--conf",
      s"spark.cassandra.auth.password=$cassandraPassword",
      "--conf",
      s"spark.epidata.measurementClass=automated_test",
      "--driver-class-path",
      "spark/target/scala-2.11/epidata-spark-assembly-1.0-SNAPSHOT.jar",
      "--py-files",
      "ipython/dist/epidata-1.0_SNAPSHOT-py2.7.egg",
      "ipython/test/test_analytics.py"
    ), None,
      "SPARK_CONF_DIR" -> "spark/conf",
      "PYTHONPATH" -> "python",
      "SPARK_MASTER" -> sparkMaster)
      .run()
      .exitValue() should equal(PROCESS_SUCCESS)
  }
}

