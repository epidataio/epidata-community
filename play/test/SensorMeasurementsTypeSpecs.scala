/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

import cassandra.DB
import com.epidata.lib.models.{ SensorMeasurement => Model }
import com.epidata.lib.models.util.Binary
import com.epidata.lib.models.util.Datatype
import java.util.Date
import javax.xml.bind.DatatypeConverter
import models.{ MeasurementService, SensorMeasurement }
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import play.api.Application
import play.api.GlobalSettings
import play.api.libs.json.JsArray
import play.api.libs.json.Json
import play.api.mvc.Handler
import play.api.mvc.RequestHeader
import play.api.test._
import play.api.test.Helpers._
import securesocialtest.WithLoggedUser
import util.Ordering

// scalastyle:off magic.number

@RunWith(classOf[JUnitRunner])
class SensorMeasurementsTypeSpecs extends Specification {

  object Fixtures {

    val truncateSQL = s"TRUNCATE ${com.epidata.lib.models.Measurement.DBTableName}"

    def truncate = DB.cql(truncateSQL)

    def cleanUp = {
      truncate
      MeasurementService.reset
    }

    def install = {
      cleanUp
      SensorMeasurement.insert(measurement1)
      SensorMeasurement.insert(measurement2)
      SensorMeasurement.insert(measurement3)
    }

    val measurement1 = Model("company0", "site0", "station0",
      "sensor0", new Date(111000000000L), "ev0", "na0", Some("double"), 0.0, Some("un0"),
      Some("st0"), Some(0.0), Some(1.0), Some("de0"))
    val measurement2 = Model("company0", "site0", "station0",
      "sensor0", new Date(111000000001L), "ev0", "na0", Some("double"), 0.0, Some("un0"),
      Some("st0"), Some(0.0), Some(1.0), Some("de0"))
    val measurement3 = Model("company0", "site0", "station0",
      "sensor0", new Date(111000000002L), "ev0", "na0", Some("string"), "VALUE", Some("un0"),
      Some("st0"), None, None, Some("de0"))

    var measurements = new Array[String](64)

    measurements(0) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": 64.76, \"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-5\"}"
    measurements(1) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": 64.76, \"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-6\"}"
    measurements(2) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": 64.76, \"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-7\"}"
    measurements(3) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": 64.76, \"sensor\": \"tester-8\"}"
    measurements(4) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": 64, \"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-9\"}"
    measurements(5) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": 64, \"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-10\"}"
    measurements(6) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": 64, \"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-11\"}"
    measurements(7) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": 64, \"sensor\": \"tester-12\"}"
    measurements(8) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": \"64\", \"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-13\"}"
    measurements(9) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": \"64\", \"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-14\"}"
    measurements(10) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": \"64\", \"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-15\"}"
    measurements(11) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_value\": \"64\", \"sensor\": \"tester-16\"}"
    measurements(12) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-17\"}"
    measurements(13) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-18\"}"
    measurements(14) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-19\"}"
    measurements(15) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"double\", \"sensor\": \"tester-20\"}"
    measurements(16) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": 64.5,\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-21\"}"
    measurements(17) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": 64.5,\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-22\"}"
    measurements(18) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": 64.5,\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-23\"}"
    measurements(19) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": 64.5,\"sensor\": \"tester-24\"}"
    measurements(20) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": 64,\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-25\"}"
    measurements(21) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": 64,\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-26\"}"
    measurements(22) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": 64,\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-27\"}"
    measurements(23) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": 64,\"sensor\": \"tester-28\"}"
    measurements(24) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": \"64\",\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-29\"}"
    measurements(25) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": \"64\",\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-30\"}"
    measurements(26) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": \"64\",\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-31\"}"
    measurements(27) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_value\": \"64\",\"sensor\": \"tester-32\"}"
    measurements(28) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-33\"}"
    measurements(29) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-34\"}"
    measurements(30) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-35\"}"
    measurements(31) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"long\",\"sensor\": \"tester-36\"}"
    measurements(32) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": 64.5,\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-37\"}"
    measurements(33) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": 64.5,\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-38\"}"
    measurements(34) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": 64.5,\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-39\"}"
    measurements(35) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": 64.5,\"sensor\": \"tester-40\"}"
    measurements(36) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": 64,\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-41\"}"
    measurements(37) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": 64,\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-42\"}"
    measurements(38) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": 64,\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-43\"}"
    measurements(39) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": 64,\"sensor\": \"tester-44\"}"
    measurements(40) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": \"64\",\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-45\"}"
    measurements(41) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": \"64\",\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-46\"}"
    measurements(42) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": \"64\",\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-47\"}"
    measurements(43) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_value\": \"64\",\"sensor\": \"tester-48\"}"
    measurements(44) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-49\"}"
    measurements(45) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-50\"}"
    measurements(46) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-51\"}"
    measurements(47) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\", \"meas_datatype\": \"string\",\"sensor\": \"tester-52\"}"
    measurements(48) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": 64.5,\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-53\"}"
    measurements(49) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": 64.5,\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-54\"}"
    measurements(50) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": 64.5,\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-55\"}"
    measurements(51) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": 64.5,\"sensor\": \"tester-56\"}"
    measurements(52) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": 64,\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-57\"}"
    measurements(53) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": 64,\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-58\"}"
    measurements(54) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": 64,\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-59\"}"
    measurements(55) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": 64,\"sensor\": \"tester-60\"}"
    measurements(56) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": \"64\",\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-61\"}"
    measurements(57) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": \"64\",\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-62\"}"
    measurements(58) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": \"64\",\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-63\"}"
    measurements(59) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_value\": \"64\",\"sensor\": \"tester-64\"}"
    measurements(60) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_lower_limit\": -30.2, \"meas_upper_limit\": 200.2, \"sensor\": \"tester-65\"}"
    measurements(61) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_lower_limit\": -30, \"meas_upper_limit\": 200, \"sensor\": \"tester-66\"}"
    measurements(62) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"meas_lower_limit\": \"-30\", \"meas_upper_limit\": \"200\", \"sensor\": \"tester-67\"}"
    measurements(63) = "{\"meas_name\": \"Temperature\", \"company\": \"company0\", \"site\": \"site0\", \"station\": \"station-1\", \"test_name\": \"Temperature_Test\", \"meas_status\": \"PASS\", \"ts\": 1505970910038, \"event\": \"Device-1\", \"meas_unit\": \"deg F\", \"meas_description\": \"\",\"sensor\": \"tester-68\"}"

    val measurementsJson = "[" + measurements.mkString(",") + "]"
  }

  /** A fake application to ensure sensor measurement routing is enabled. */
  class FakeApp(withoutPlugins: List[String], additionalPlugins: List[String])
    extends FakeApplication(
      withoutPlugins = withoutPlugins,
      additionalPlugins = additionalPlugins,
      withGlobal = Some(new GlobalSettings {
        override def onStart(app: Application) = Global.onStart(app)

        override def onRouteRequest(req: RequestHeader): Option[Handler] =
          sensor_measurement.Routes.handlerFor(req) orElse super.onRouteRequest(req)

        override def onStop(app: Application) = Global.onStop(app)
      }))

  import WithLoggedUser._

  object FakeApp {
    def apply() = new FakeApp(
      withoutPlugins = excludedPlugins,
      additionalPlugins = includedPlugins)
  }

  "insert a sensor measurements with all combination datatype" in new WithLoggedUser(FakeApp()) {

    Fixtures.cleanUp

    val create = route(FakeRequest(
      POST,
      "/measurements",
      FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
      Json.parse(Fixtures.measurementsJson)).withCookies(cookie)).get
    status(create) must equalTo(BAD_REQUEST)

    var results = new Array[Model](70)

    results(56) = SensorMeasurement.find("company0", "site0", "station-1", "tester-56", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(68) = SensorMeasurement.find("company0", "site0", "station-1", "tester-68", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(46) = SensorMeasurement.find("company0", "site0", "station-1", "tester-46", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(20) = SensorMeasurement.find("company0", "site0", "station-1", "tester-20", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(48) = SensorMeasurement.find("company0", "site0", "station-1", "tester-48", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(44) = SensorMeasurement.find("company0", "site0", "station-1", "tester-44", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(66) = SensorMeasurement.find("company0", "site0", "station-1", "tester-66", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(14) = SensorMeasurement.find("company0", "site0", "station-1", "tester-14", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(37) = SensorMeasurement.find("company0", "site0", "station-1", "tester-37", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(49) = SensorMeasurement.find("company0", "site0", "station-1", "tester-49", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(65) = SensorMeasurement.find("company0", "site0", "station-1", "tester-65", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(52) = SensorMeasurement.find("company0", "site0", "station-1", "tester-52", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(13) = SensorMeasurement.find("company0", "site0", "station-1", "tester-13", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(58) = SensorMeasurement.find("company0", "site0", "station-1", "tester-58", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(54) = SensorMeasurement.find("company0", "site0", "station-1", "tester-54", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(50) = SensorMeasurement.find("company0", "site0", "station-1", "tester-50", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(45) = SensorMeasurement.find("company0", "site0", "station-1", "tester-45", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(26) = SensorMeasurement.find("company0", "site0", "station-1", "tester-26", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(41) = SensorMeasurement.find("company0", "site0", "station-1", "tester-41", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(62) = SensorMeasurement.find("company0", "site0", "station-1", "tester-62", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(10) = SensorMeasurement.find("company0", "site0", "station-1", "tester-10", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(29) = SensorMeasurement.find("company0", "site0", "station-1", "tester-29", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(57) = SensorMeasurement.find("company0", "site0", "station-1", "tester-57", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(42) = SensorMeasurement.find("company0", "site0", "station-1", "tester-42", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(36) = SensorMeasurement.find("company0", "site0", "station-1", "tester-36", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(17) = SensorMeasurement.find("company0", "site0", "station-1", "tester-17", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(34) = SensorMeasurement.find("company0", "site0", "station-1", "tester-34", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(61) = SensorMeasurement.find("company0", "site0", "station-1", "tester-61", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(22) = SensorMeasurement.find("company0", "site0", "station-1", "tester-22", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(32) = SensorMeasurement.find("company0", "site0", "station-1", "tester-32", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(25) = SensorMeasurement.find("company0", "site0", "station-1", "tester-25", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(21) = SensorMeasurement.find("company0", "site0", "station-1", "tester-21", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(60) = SensorMeasurement.find("company0", "site0", "station-1", "tester-60", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(9) = SensorMeasurement.find("company0", "site0", "station-1", "tester-9", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(33) = SensorMeasurement.find("company0", "site0", "station-1", "tester-33", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(6) = SensorMeasurement.find("company0", "site0", "station-1", "tester-6", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(12) = SensorMeasurement.find("company0", "site0", "station-1", "tester-12", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(18) = SensorMeasurement.find("company0", "site0", "station-1", "tester-18", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(16) = SensorMeasurement.find("company0", "site0", "station-1", "tester-16", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(24) = SensorMeasurement.find("company0", "site0", "station-1", "tester-24", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(28) = SensorMeasurement.find("company0", "site0", "station-1", "tester-28", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(30) = SensorMeasurement.find("company0", "site0", "station-1", "tester-30", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(8) = SensorMeasurement.find("company0", "site0", "station-1", "tester-8", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(53) = SensorMeasurement.find("company0", "site0", "station-1", "tester-53", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(40) = SensorMeasurement.find("company0", "site0", "station-1", "tester-40", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(38) = SensorMeasurement.find("company0", "site0", "station-1", "tester-38", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(5) = SensorMeasurement.find("company0", "site0", "station-1", "tester-5", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)
    results(64) = SensorMeasurement.find("company0", "site0", "station-1", "tester-64", new Date(1505970910037L), new Date(1505970910040L), Ordering.Unspecified)(0)

    (results(56).meas_datatype, results(56).meas_value, results(56).meas_upper_limit, results(56).meas_lower_limit) must equalTo(Some(""), 64.5, None, None)
    (results(68).meas_datatype, results(68).meas_value, results(68).meas_upper_limit, results(68).meas_lower_limit) must equalTo(Some(""), None, None, None)
    (results(46).meas_datatype, results(46).meas_value, results(46).meas_upper_limit, results(46).meas_lower_limit) must equalTo(Some("string"), "64", Some(200), Some(-30))
    (results(20).meas_datatype, results(20).meas_value, results(20).meas_upper_limit, results(20).meas_lower_limit) must equalTo(Some("double"), None, None, None)
    (results(48).meas_datatype, results(48).meas_value, results(48).meas_upper_limit, results(48).meas_lower_limit) must equalTo(Some("string"), "64", None, None)
    (results(44).meas_datatype, results(44).meas_value, results(44).meas_upper_limit, results(44).meas_lower_limit) must equalTo(Some("string"), 64, None, None)
    (results(66).meas_datatype, results(66).meas_value, results(66).meas_upper_limit, results(66).meas_lower_limit) must equalTo(Some(""), None, Some(200), Some(-30))
    (results(14).meas_datatype, results(14).meas_value, results(14).meas_upper_limit, results(14).meas_lower_limit) must equalTo(Some("double"), "64", Some(200), Some(-30))
    (results(37).meas_datatype, results(37).meas_value, results(37).meas_upper_limit, results(37).meas_lower_limit) must equalTo(Some("string"), 64.5, Some(200.2), Some(-30.2))
    (results(49).meas_datatype, results(49).meas_value, results(49).meas_upper_limit, results(49).meas_lower_limit) must equalTo(Some("string"), None, Some(200.2), Some(-30.2))
    (results(65).meas_datatype, results(65).meas_value, results(65).meas_upper_limit, results(65).meas_lower_limit) must equalTo(Some(""), None, Some(200.2), Some(-30.2))
    (results(52).meas_datatype, results(52).meas_value, results(52).meas_upper_limit, results(52).meas_lower_limit) must equalTo(Some("string"), None, None, None)
    (results(13).meas_datatype, results(13).meas_value, results(13).meas_upper_limit, results(13).meas_lower_limit) must equalTo(Some("double"), "64", Some(200.2), Some(-30.2))
    (results(58).meas_datatype, results(58).meas_value, results(58).meas_upper_limit, results(58).meas_lower_limit) must equalTo(Some(""), 64, Some(200), Some(-30))
    (results(54).meas_datatype, results(54).meas_value, results(54).meas_upper_limit, results(54).meas_lower_limit) must equalTo(Some(""), 64.5, Some(200), Some(-30))
    (results(50).meas_datatype, results(50).meas_value, results(50).meas_upper_limit, results(50).meas_lower_limit) must equalTo(Some("string"), None, Some(200), Some(-30))
    (results(45).meas_datatype, results(45).meas_value, results(45).meas_upper_limit, results(45).meas_lower_limit) must equalTo(Some("string"), "64", Some(200.2), Some(-30.2))
    (results(26).meas_datatype, results(26).meas_value, results(26).meas_upper_limit, results(26).meas_lower_limit) must equalTo(Some("long"), 64, Some(200), Some(-30))
    (results(41).meas_datatype, results(41).meas_value, results(41).meas_upper_limit, results(41).meas_lower_limit) must equalTo(Some("string"), 64, Some(200.2), Some(-30.2))
    (results(62).meas_datatype, results(62).meas_value, results(62).meas_upper_limit, results(62).meas_lower_limit) must equalTo(Some(""), "64", Some(200), Some(-30))
    (results(10).meas_datatype, results(10).meas_value, results(10).meas_upper_limit, results(10).meas_lower_limit) must equalTo(Some("double"), 64, Some(200), Some(-30))
    (results(29).meas_datatype, results(29).meas_value, results(29).meas_upper_limit, results(29).meas_lower_limit) must equalTo(Some("long"), "64", Some(200.2), Some(-30.2))
    (results(57).meas_datatype, results(57).meas_value, results(57).meas_upper_limit, results(57).meas_lower_limit) must equalTo(Some(""), 64, Some(200.2), Some(-30.2))
    (results(42).meas_datatype, results(42).meas_value, results(42).meas_upper_limit, results(42).meas_lower_limit) must equalTo(Some("string"), 64, Some(200), Some(-30))
    (results(36).meas_datatype, results(36).meas_value, results(36).meas_upper_limit, results(36).meas_lower_limit) must equalTo(Some("long"), None, None, None)
    (results(17).meas_datatype, results(17).meas_value, results(17).meas_upper_limit, results(17).meas_lower_limit) must equalTo(Some("double"), None, Some(200.2), Some(-30.2))
    (results(34).meas_datatype, results(34).meas_value, results(34).meas_upper_limit, results(34).meas_lower_limit) must equalTo(Some("long"), None, Some(200), Some(-30))
    (results(61).meas_datatype, results(61).meas_value, results(61).meas_upper_limit, results(61).meas_lower_limit) must equalTo(Some(""), "64", Some(200.2), Some(-30.2))
    (results(22).meas_datatype, results(22).meas_value, results(22).meas_upper_limit, results(22).meas_lower_limit) must equalTo(Some("long"), 64.5, Some(200), Some(-30))
    (results(32).meas_datatype, results(32).meas_value, results(32).meas_upper_limit, results(32).meas_lower_limit) must equalTo(Some("long"), "64", None, None)
    (results(25).meas_datatype, results(25).meas_value, results(25).meas_upper_limit, results(25).meas_lower_limit) must equalTo(Some("long"), 64, Some(200.2), Some(-30.2))
    (results(21).meas_datatype, results(21).meas_value, results(21).meas_upper_limit, results(21).meas_lower_limit) must equalTo(Some("long"), 64.5, Some(200.2), Some(-30.2))
    (results(60).meas_datatype, results(60).meas_value, results(60).meas_upper_limit, results(60).meas_lower_limit) must equalTo(Some(""), 64, None, None)
    (results(9).meas_datatype, results(9).meas_value, results(9).meas_upper_limit, results(9).meas_lower_limit) must equalTo(Some("double"), 64, Some(200.2), Some(-30.2))
    (results(33).meas_datatype, results(33).meas_value, results(33).meas_upper_limit, results(33).meas_lower_limit) must equalTo(Some("long"), None, Some(200.2), Some(-30.2))
    (results(6).meas_datatype, results(6).meas_value, results(6).meas_upper_limit, results(6).meas_lower_limit) must equalTo(Some("double"), 64.76, Some(200), Some(-30))
    (results(12).meas_datatype, results(12).meas_value, results(12).meas_upper_limit, results(12).meas_lower_limit) must equalTo(Some("double"), 64, None, None)
    (results(18).meas_datatype, results(18).meas_value, results(18).meas_upper_limit, results(18).meas_lower_limit) must equalTo(Some("double"), None, Some(200), Some(-30))
    (results(16).meas_datatype, results(16).meas_value, results(16).meas_upper_limit, results(16).meas_lower_limit) must equalTo(Some("double"), "64", None, None)
    (results(24).meas_datatype, results(24).meas_value, results(24).meas_upper_limit, results(24).meas_lower_limit) must equalTo(Some("long"), 64.5, None, None)
    (results(28).meas_datatype, results(28).meas_value, results(28).meas_upper_limit, results(28).meas_lower_limit) must equalTo(Some("long"), 64, None, None)
    (results(30).meas_datatype, results(30).meas_value, results(30).meas_upper_limit, results(30).meas_lower_limit) must equalTo(Some("long"), "64", Some(200), Some(-30))
    (results(8).meas_datatype, results(8).meas_value, results(8).meas_upper_limit, results(8).meas_lower_limit) must equalTo(Some("double"), 64.76, None, None)
    (results(53).meas_datatype, results(53).meas_value, results(53).meas_upper_limit, results(53).meas_lower_limit) must equalTo(Some(""), 64.5, Some(200.2), Some(-30.2))
    (results(40).meas_datatype, results(40).meas_value, results(40).meas_upper_limit, results(40).meas_lower_limit) must equalTo(Some("string"), 64.5, None, None)
    (results(38).meas_datatype, results(38).meas_value, results(38).meas_upper_limit, results(38).meas_lower_limit) must equalTo(Some("string"), 64.5, Some(200), Some(-30))
    (results(5).meas_datatype, results(5).meas_value, results(5).meas_upper_limit, results(5).meas_lower_limit) must equalTo(Some("double"), 64.76, Some(200.2), Some(-30.2))
    (results(64).meas_datatype, results(64).meas_value, results(64).meas_upper_limit, results(64).meas_lower_limit) must equalTo(Some(""), "64", None, None)
  }

}
