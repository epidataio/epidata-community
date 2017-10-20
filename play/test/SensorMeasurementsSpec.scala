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
class SensorMeasurementsSpec extends Specification {

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
      })
    )

  import WithLoggedUser._

  object FakeApp {
    def apply() = new FakeApp(
      withoutPlugins = excludedPlugins,
      additionalPlugins = includedPlugins
    )
  }

  "SensorMeasurements" should {

    "insert a sensor measurement" in new WithLoggedUser(FakeApp()) {

      Fixtures.cleanUp

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#{
            #"company": "company0",
            #"site": "site0",
            #"station": "station0",
            #"sensor": "sensor0",
            #"ts": 111000000000,
            #"event": "e",
            #"meas_name": "m",
            #"meas_value": 0.2,
            #"meas_datatype": "double",
            #"meas_unit": "u",
            #"meas_status": "s",
            #"meas_lower_limit": 0.0,
            #"meas_upper_limit": 0.5,
            #"meas_description": "d"
            #}""".stripMargin('#'))
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      SensorMeasurement.find("company0", "site0", "station0", "sensor0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified).length must equalTo(1)
    }

    "fail to insert a sensor measurement without authentication" in new WithLoggedUser(FakeApp()) {

      Fixtures.cleanUp

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#{
            #"company": "company0",
            #"site": "site0",
            #"station": "station0",
            #"sensor": "sensor0",
            #"ts": 111000000000,
            #"event": "e",
            #"meas_name": "m",
            #"meas_value": 0.2,
            #"meas_datatype": "double",
            #"meas_unit": "u",
            #"meas_status": "s",
            #"meas_lower_limit": 0.0,
            #"meas_upper_limit": 0.5,
            #"meas_description": "d"
            #}""".stripMargin('#'))
      )).get // No User cookie is provided.
      status(create) must equalTo(SEE_OTHER)
    }

    "insert a sensor measurement with out of order json fields" in new WithLoggedUser(FakeApp()) {

      Fixtures.cleanUp

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#{
            #"site": "site0",
            #"sensor": "sensor0",
            #"ts": 111000000000,
            #"event": "e",
            #"meas_lower_limit": 0.0,
            #"meas_name": "m",
            #"meas_value": 0.2,
            #"meas_datatype": "double",
            #"station": "station0",
            #"meas_unit": "u",
            #"meas_status": "s",
            #"company": "company0",
            #"meas_upper_limit": 0.5,
            #"meas_description": "d"
            #}""".stripMargin('#'))
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      SensorMeasurement.find("company0", "site0", "station0", "sensor0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified).length must equalTo(1)
    }

    "query for a measurement" in new WithLoggedUser(FakeApp()) {
      Fixtures.install

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentAsString(query) must
        equalTo(SensorMeasurement.toJson(List(Fixtures.measurement1)).toString)
    }

    "fail to query for a measurement without authentication" in new WithLoggedUser(FakeApp()) {
      Fixtures.install

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001")).get // No User cookie is provided.
      status(query) must equalTo(SEE_OTHER)
    }

    "query for some measurements" in new WithLoggedUser(FakeApp()) {
      Fixtures.install

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=111000000000&" +
        "endTime=111000000003&" +
        "sort=descending").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentAsString(query) must
        equalTo(SensorMeasurement.toJson(List(Fixtures.measurement3, Fixtures.measurement2, Fixtures.measurement1)).toString)
    }

    "query not matching any measurements" in new WithLoggedUser(FakeApp()) {
      Fixtures.install

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=111000001000&" +
        "endTime=111000001000").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentAsString(query) must
        equalTo(SensorMeasurement.toJson(List()).toString)
    }

    "validate missing fields" in new WithLoggedUser(FakeApp()) {
      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "station=station0&" +
        // field site missing
        "sensor=sensor0&" +
        "beginTime=111000001000&" +
        "endTime=111000001000").withCookies(cookie)).get
      status(query) must equalTo(BAD_REQUEST)
    }

    "validate numeric dates" in new WithLoggedUser(FakeApp()) {
      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=badtime&" + // beginTime invalid
        "endTime=111000001000").withCookies(cookie)).get
      status(query) must equalTo(BAD_REQUEST)
    }

    "validate sort options" in new WithLoggedUser(FakeApp()) {
      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=111000001000&" +
        "endTime=111000001000&" +
        "sort=badorder" // sort invalid
        ).withCookies(cookie)).get
      status(query) must equalTo(BAD_REQUEST)
    }

    "insert and find a long sensor measurement" in new WithLoggedUser(FakeApp()) {
      Fixtures.cleanUp

      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"station": "station0",
        #"sensor": "sensor0",
        #"ts": 111000000000,
        #"event": "e",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_status": "s",
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9,
        #"meas_description": "md"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = SensorMeasurement.find("company0", "site0", "station0", "sensor0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_value must beTypedEqualTo(3: Int)
      found(0).meas_lower_limit.get must beTypedEqualTo(2: Int)
      found(0).meas_upper_limit.get must beTypedEqualTo(9: Int)

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      Json.parse(contentAsString(query)).as[JsArray].value(0) must equalTo(jsonMeasurement)
    }

    "insert and find a large long sensor measurement" in new WithLoggedUser(FakeApp()) {
      Fixtures.cleanUp

      val jsonMeasurement = Json.parse(s"""#{
        #"company": "company0",
        #"site": "site0",
        #"station": "station0",
        #"sensor": "sensor0",
        #"ts": 111000000000,
        #"event": "e",
        #"meas_name": "m",
        #"meas_value": 800000000000000002, ${"" /* Too large to be represented as a Double. */ }
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_status": "s",
        #"meas_lower_limit": 800000000000000001,
        #"meas_upper_limit": 800000000000000003,
        #"meas_description": "md"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = SensorMeasurement.find("company0", "site0", "station0", "sensor0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_value must beTypedEqualTo(800000000000000002L)
      found(0).meas_lower_limit.get must beTypedEqualTo(800000000000000001L)
      found(0).meas_upper_limit.get must beTypedEqualTo(800000000000000003L)

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      Json.parse(contentAsString(query)).as[JsArray].value(0) must equalTo(jsonMeasurement)
    }

    "insert and find a string sensor measurement" in new WithLoggedUser(FakeApp()) {
      Fixtures.cleanUp

      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"station": "station0",
        #"sensor": "sensor0",
        #"ts": 111000000000,
        #"event": "e",
        #"meas_name": "m",
        #"meas_value": "STRINGVALUE",
        #"meas_datatype": "string",
        #"meas_status": "s",
        #"meas_description": "md"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = SensorMeasurement.find("company0", "site0", "station0", "sensor0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_value must beTypedEqualTo("STRINGVALUE")

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      Json.parse(contentAsString(query)).as[JsArray].value(0) must equalTo(jsonMeasurement)
    }

    "insert and find an array sensor measurement" in new WithLoggedUser(FakeApp()) {
      Fixtures.cleanUp

      // Dummy binary data for testing
      val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11).map(_.toByte)
      val encodedArray = DatatypeConverter.printBase64Binary(array)

      val jsonMeasurement = Json.parse(s"""#{
        #"company": "company0",
        #"site": "site0",
        #"station": "station0",
        #"sensor": "sensor0",
        #"ts": 111000000000,
        #"event": "e",
        #"meas_name": "m",
        #"meas_value": "${encodedArray}",
        #"meas_datatype": "double_array",
        #"meas_unit": "u",
        #"meas_status": "s",
        #"meas_description": "md"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = SensorMeasurement.find("company0", "site0", "station0", "sensor0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_value.asInstanceOf[Binary].backing must
        beTypedEqualTo(Array(Datatype.DoubleArray.id.toByte) ++ array)

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
    }

    "insert and find a waveform sensor measurement" in new WithLoggedUser(FakeApp()) {
      Fixtures.cleanUp

      // Dummy binary data for testing
      val array = Array(10, 20, 30, 40, 50, 60, 70, 80, 90, 11, 15).map(_.toByte)
      val encodedArray = DatatypeConverter.printBase64Binary(array)

      val jsonMeasurement = Json.parse(s"""#{
        #"company": "company0",
        #"site": "site0",
        #"station": "station0",
        #"sensor": "sensor0",
        #"ts": 111000000000,
        #"event": "e",
        #"meas_name": "m",
        #"meas_value": "${encodedArray}",
        #"meas_datatype": "waveform",
        #"meas_unit": "u",
        #"meas_status": "s",
        #"meas_description": "md"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = SensorMeasurement.find("company0", "site0", "station0", "sensor0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_value.asInstanceOf[Binary].backing must
        beTypedEqualTo(Array(Datatype.Waveform.id.toByte) ++ array)

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
    }

    "allow insert numeric with missing lower limit" in new WithLoggedUser(FakeApp()) {

      Fixtures.cleanUp
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"station": "station0",
        #"sensor": "sensor0",
        #"ts": 111000000000,
        #"event": "e",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_status": "s",
        #"meas_upper_limit": 9,
        #"meas_description": "md"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)
    }

    "allow insert numeric with missing upper limit" in new WithLoggedUser(FakeApp()) {

      Fixtures.cleanUp
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"station": "station0",
        #"sensor": "sensor0",
        #"ts": 111000000000,
        #"event": "e",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_status": "s",
        #"meas_lower_limit": 2,
        #"meas_description": "md"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)
    }

    "insert string with unexpected lower limit" in new WithLoggedUser(FakeApp()) {

      Fixtures.cleanUp
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"station": "station0",
        #"sensor": "sensor0",
        #"ts": 111000000000,
        #"event": "e",
        #"meas_name": "m",
        #"meas_value": "STRINGVALUE",
        #"meas_datatype": "string",
        #"meas_status": "s",
        #"meas_lower_limit": 2,
        #"meas_description": "md"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)
    }

    "insert string with unexpected upper limit" in new WithLoggedUser(FakeApp()) {

      Fixtures.cleanUp
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"station": "station0",
        #"sensor": "sensor0",
        #"ts": 111000000000,
        #"event": "e",
        #"meas_name": "m",
        #"meas_value": "STRINGVALUE",
        #"meas_datatype": "string",
        #"meas_status": "s",
        #"meas_upper_limit": 2,
        #"meas_description": "md"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)
    }

    "insert and find a sensor measurement without a description or status" in new WithLoggedUser(FakeApp()) {
      Fixtures.cleanUp

      val jsonMeasurement = Json.parse(s"""#{
        #"company": "company0",
        #"site": "site0",
        #"station": "station0",
        #"sensor": "sensor0",
        #"ts": 111000000000,
        #"event": "e",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = SensorMeasurement.find("company0", "site0", "station0", "sensor0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_description must beNone
      found(0).meas_status must beNone

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "station=station0&" +
        "sensor=sensor0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      Json.parse(contentAsString(query)).as[JsArray].value(0) must equalTo(jsonMeasurement)
    }

    "optional empty string fields are dropped" in new WithLoggedUser(FakeApp()) {

      Fixtures.cleanUp

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse(s"""#{
            #"company": "company0",
            #"site": "site0",
            #"station": "station0",
            #"sensor": "sensor0",
            #"ts": 111000000000,
            #"event": "e",
            #"meas_name": "m",
            #"meas_value": 0.2,
            #"meas_datatype": "double",
            #"meas_unit": "",              ${"" /* empty string field */ }
            #"meas_status": "",            ${"" /* empty string field */ }
            #"meas_lower_limit": 0.0,
            #"meas_upper_limit": 0.5,
            #"meas_description": ""        ${"" /* empty string field */ }
            #}""".stripMargin('#'))
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val results = SensorMeasurement.find("company0", "site0", "station0", "sensor0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)

      results.length must equalTo(1)

      // Empty string fields are interpreted as absent and dropped when
      // the record is returned.
      results(0).meas_unit must beNone
      results(0).meas_status must beNone
      results(0).meas_description must beNone
    }

  }
}
