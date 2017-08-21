/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

import cassandra.DB
import com.epidata.lib.models.{ AutomatedTest => Model }
import com.epidata.lib.models.util.Binary
import com.epidata.lib.models.util.Datatype
import java.util.Date
import javax.xml.bind.DatatypeConverter
import models.AutomatedTest
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
class AutomatedTestsSpec extends Specification {

  object Fixtures {

    val truncateSQL = s"TRUNCATE ${com.epidata.lib.models.Measurement.DBTableName}"
    def truncate = DB.cql(truncateSQL)

    def install = {
      truncate
      AutomatedTest.insert(measurement1)
      AutomatedTest.insert(measurement2)
      AutomatedTest.insert(measurement3)
    }

    val measurement1 = Model("company0", "site0", "device_group0",
      "tester0", new Date(111000000000L), "dn0", "tn0", "mn0", Some("double"), 0.0,
      Some("un0"), Some("st0"), Some(0.0), Some(1.0), Some("de0"), Some("ds0"),
      Some("ts0"))
    val measurement2 = Model("company0", "site0", "device_group0",
      "tester0", new Date(111000000001L), "dn0", "tn0", "mn0", Some("double"), 0.0,
      Some("un0"), Some("st0"), Some(0.0), Some(1.0), Some("de0"), Some("ds0"),
      Some("ts0"))
    val measurement3 = Model("company0", "site0", "device_group0",
      "tester0", new Date(111000000002L), "dn0", "tn0", "mn0", Some("string"), "STRINGVAL",
      None, Some("st0"), None, None, Some("de0"), Some("ds0"), Some("ts0"))
  }

  /**
   * A fake application to ensure automated test routing is enabled and
   * appropriate plugins are used.
   */
  class FakeApp ////(withoutPlugins: List[String], additionalPlugins: List[String])
    extends FakeApplication(
      // withoutPlugins = withoutPlugins,
      // additionalPlugins = additionalPlugins,
      withGlobal = Some(new GlobalSettings {
        override def onStart(app: Application) = Global.onStart(app)

        override def onRouteRequest(req: RequestHeader): Option[Handler] =
          automated_test.Routes.handlerFor(req) orElse super.onRouteRequest(req)

        override def onStop(app: Application) = Global.onStop(app)
      })
    )

  import WithLoggedUser._

  object FakeApp {
    def apply() = new FakeApp( // withoutPlugins = excludedPlugins,
    // additionalPlugins = includedPlugins
    )
  }

  "AutomatedTests" should {

    "insert an automated test" in new WithLoggedUser(FakeApp()) {

      Fixtures.truncate

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#{
            #"company": "company0",
            #"site": "site0",
            #"device_group": "device_group0",
            #"tester": "tester0",
            #"ts": 111000000000,
            #"device_name": "dn",
            #"test_name": "t",
            #"meas_name": "m",
            #"meas_value": 0.2,
            #"meas_datatype": "double",
            #"meas_unit": "u",
            #"meas_status": "PASS",
            #"meas_lower_limit": 0.0,
            #"meas_upper_limit": 0.5,
            #"meas_description": "md",
            #"device_status": "PASS",
            #"test_status": "PASS"
            #}""".stripMargin('#'))
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      AutomatedTest.find("company0", "site0", "device_group0", "tester0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified).length must equalTo(1)
    }

    "insert an automated test fails without authentication" in new WithLoggedUser(FakeApp()) {

      Fixtures.truncate

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#{
            #"company": "company0",
            #"site": "site0",
            #"device_group": "device_group0",
            #"tester": "tester0",
            #"ts": 111000000000,
            #"device_name": "dn",
            #"test_name": "t",
            #"meas_name": "m",
            #"meas_value": 0.2,
            #"meas_datatype": "double",
            #"meas_unit": "u",
            #"meas_status": "PASS",
            #"meas_lower_limit": 0.0,
            #"meas_upper_limit": 0.5,
            #"meas_description": "md",
            #"device_status": "PASS",
            #"test_status": "PASS"
            #}""".stripMargin('#'))
      )).get // No user cookie is provided.
      status(create) must equalTo(SEE_OTHER)
    }

    "insert an automated test with out of order json fields" in new WithLoggedUser(FakeApp()) {

      Fixtures.truncate

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#{
            #"device_group": "device_group0",
            #"meas_upper_limit": 0.5,
            #"tester": "tester0",
            #"company": "company0",
            #"device_name": "dn",
            #"meas_name": "m",
            #"meas_value": 0.2,
            #"meas_datatype": "double",
            #"meas_unit": "u",
            #"meas_status": "PASS",
            #"test_name": "t",
            #"site": "site0",
            #"meas_lower_limit": 0.0,
            #"meas_description": "md",
            #"device_status": "PASS",
            #"ts": 111000000000,
            #"test_status": "PASS"
            #}""".stripMargin('#'))
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      AutomatedTest.find("company0", "site0", "device_group0", "tester0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified).length must equalTo(1)
    }

    "reject an automated test with an improper field name" in new WithLoggedUser(FakeApp()) {
      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#{
            #"company": "company0",
            #"site": "site0",
            #"device_group": "device_group0",
            #"tester": "tester0",
            #"ts": 111000000000,
            #"device_name": "dn",
            #"test_name": "t",
            #"meas_name": "m",
            #"meas_value": 0.2,
            #"meas_datatype": "double",
            #"BAD_FIELD_NAME": "u",
            #"meas_status": "PASS",
            #"meas_lower_limit": 0.0,
            #"meas_upper_limit": 0.5,
            #"meas_description": "md",
            #"device_status": "PASS",
            #"test_status": "PASS"
            #}""".stripMargin('#'))
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)
    }

    "reject an automated test with an improper data type" in new WithLoggedUser(FakeApp()) {
      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse("""#{
            #"company": "company0",
            #"site": "site0",
            #"device_group": "device_group0",
            #"tester": "tester0",
            #"ts": 111000000000,
            #"device_name": "dn",
            #"test_name": "t",
            #"meas_name": "m",
            #"meas_value": false,
            #"meas_datatype": "double",
            #"meas_unit": "u",
            #"meas_status": "PASS",
            #"meas_lower_limit": 0.0,
            #"meas_upper_limit": 0.5,
            #"meas_description": "md",
            #"device_status": "PASS",
            #"test_status": "PASS"
            #}""".stripMargin('#'))
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)
    }

    "query for a measurement" in new WithLoggedUser(FakeApp()) {
      Fixtures.install

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get

      status(query) must equalTo(OK)
      contentType(query) must beSome.which(_ == "application/json")
      contentAsString(query) must
        equalTo(AutomatedTest.toJson(List(Fixtures.measurement1)).toString)
    }

    "query for a measurement without authentication" in new WithLoggedUser(FakeApp()) {
      Fixtures.install

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001")).get // No User cookie provided.

      status(query) must equalTo(SEE_OTHER)
    }

    "query for some measurements" in new WithLoggedUser(FakeApp()) {
      Fixtures.install

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000000000&" +
        "endTime=111000000003&" +
        "sort=descending").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentType(query) must beSome.which(_ == "application/json")
      contentAsString(query) must
        equalTo(AutomatedTest.toJson(List(Fixtures.measurement3, Fixtures.measurement2, Fixtures.measurement1)).toString)
    }

    "query not matching any measurements" in new WithLoggedUser(FakeApp()) {
      Fixtures.install

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000001000&" +
        "endTime=111000001000").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentType(query) must beSome.which(_ == "application/json")
      contentAsString(query) must
        equalTo(AutomatedTest.toJson(List()).toString)
    }

    "validate missing fields" in new WithLoggedUser(FakeApp()) {
      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        // field device group missing
        "tester=tester0&" +
        "beginTime=111000001000&" +
        "endTime=111000001000").withCookies(cookie)).get
      status(query) must equalTo(BAD_REQUEST)
    }

    "validate numeric dates" in new WithLoggedUser(FakeApp()) {
      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000001000&" +
        "endTime=badtime" // endTime invalid
        ).withCookies(cookie)).get
      status(query) must equalTo(BAD_REQUEST)
    }

    "validate sort options" in new WithLoggedUser(FakeApp()) {
      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000001000&" +
        "endTime=111000001000" +
        "sort=badorder" // sort invalid
        ).withCookies(cookie)).get
      status(query) must equalTo(BAD_REQUEST)
    }

    "insert and find a long automated test" in new WithLoggedUser(FakeApp()) {
      Fixtures.truncate

      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = AutomatedTest.find("company0", "site0", "device_group0", "tester0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_value must beTypedEqualTo(3: Int)
      found(0).meas_lower_limit.get must beTypedEqualTo(2: Int)
      found(0).meas_upper_limit.get must beTypedEqualTo(9: Int)

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentType(query) must beSome.which(_ == "application/json")
      Json.parse(contentAsString(query)).as[JsArray].value(0) must equalTo(jsonMeasurement)
    }

    "insert and find a large long automated test" in new WithLoggedUser(FakeApp()) {
      Fixtures.truncate

      val jsonMeasurement = Json.parse(s"""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 900000000000000002, ${"" /* Too large to be represented as a Double. */ }
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_lower_limit": 900000000000000001,
        #"meas_upper_limit": 900000000000000003,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = AutomatedTest.find("company0", "site0", "device_group0", "tester0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_value must beTypedEqualTo(900000000000000002L)
      found(0).meas_lower_limit.get must beTypedEqualTo(900000000000000001L)
      found(0).meas_upper_limit.get must beTypedEqualTo(900000000000000003L)

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentType(query) must beSome.which(_ == "application/json")

      Json.parse(contentAsString(query)).as[JsArray].value(0) must equalTo(jsonMeasurement)
    }

    "insert and find a string automated test" in new WithLoggedUser(FakeApp()) {
      Fixtures.truncate

      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": "STRINGVALUE",
        #"meas_datatype": "string",
        #"meas_status": "PASS",
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = AutomatedTest.find("company0", "site0", "device_group0", "tester0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_value must beTypedEqualTo("STRINGVALUE")

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentType(query) must beSome.which(_ == "application/json")
      Json.parse(contentAsString(query)).as[JsArray].value(0) must equalTo(jsonMeasurement)
    }

    "insert and find an array automated test" in new WithLoggedUser(FakeApp()) {
      Fixtures.truncate

      // Dummy binary data for testing
      val array = Array(1, 2, 3, 4, 5, 6, 7, 8).map(_.toByte)
      val encodedArray = DatatypeConverter.printBase64Binary(array)

      val jsonMeasurement = Json.parse(s"""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": "${encodedArray}",
        #"meas_datatype": "double_array",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = AutomatedTest.find("company0", "site0", "device_group0", "tester0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_value.asInstanceOf[Binary].backing must
        beTypedEqualTo(Array(Datatype.DoubleArray.id.toByte) ++ array)

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentType(query) must beSome.which(_ == "application/json")
      Json.parse(contentAsString(query)).as[JsArray].value(0) must equalTo(jsonMeasurement)
    }

    "insert and find a waveform automated test" in new WithLoggedUser(FakeApp()) {
      Fixtures.truncate

      // Dummy binary data for testing
      val array = Array(10, 20, 30, 40, 50, 60, 70, 80).map(_.toByte)
      val encodedArray = DatatypeConverter.printBase64Binary(array)

      val jsonMeasurement = Json.parse(s"""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": "${encodedArray}",
        #"meas_datatype": "waveform",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = AutomatedTest.find("company0", "site0", "device_group0", "tester0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_value.asInstanceOf[Binary].backing must
        beTypedEqualTo(Array(Datatype.Waveform.id.toByte) ++ array)

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentType(query) must beSome.which(_ == "application/json")
      Json.parse(contentAsString(query)).as[JsArray].value(0) must equalTo(jsonMeasurement)
    }

    "prevent insert with missing datatype field" in new WithLoggedUser(FakeApp()) {
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)
    }

    "prevent insert with invalid datatype field" in new WithLoggedUser(FakeApp()) {
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "BAD_DATATYPE",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)
    }

    "prevent insert number with incompatible datatype field" in new WithLoggedUser(FakeApp()) {
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "string",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)
    }

    "prevent insert string with incompatible datatype field" in new WithLoggedUser(FakeApp()) {
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": "NON-NUMBER",
        #"meas_datatype": "double",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)
    }

    "allow insert numeric with missing lower limit" in new WithLoggedUser(FakeApp()) {
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "double",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_upper_limit": 9,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
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
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "double",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_lower_limit": 2,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)
    }

    "prevent insert numeric with missing unit" in new WithLoggedUser(FakeApp()) {
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "double",
        #"meas_status": "PASS",
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)
    }

    "insert string with unexpected lower limit" in new WithLoggedUser(FakeApp()) {
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": "PASS",
        #"meas_datatype": "string",
        #"meas_status": "PASS",
        #"meas_lower_limit": 2,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
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
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": "PASS",
        #"meas_datatype": "string",
        #"meas_status": "PASS",
        #"meas_upper_limit": 2,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)
    }

    "prevent insert string with unit" in new WithLoggedUser(FakeApp()) {
      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": "PASS",
        #"meas_datatype": "string",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)
    }

    "insert and find an automated test without a description or status" in new WithLoggedUser(FakeApp()) {
      Fixtures.truncate

      val jsonMeasurement = Json.parse(s"""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_status": ""        ${"" /* empty string field */ },
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9,
        #"meas_description": ""   ${"" /* empty string field */ },
        #"device_status": ""      ${"" /* empty string field */ },
        #"test_status": ""        ${"" /* empty string field */ }
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)

      val found = AutomatedTest.find("company0", "site0", "device_group0", "tester0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(0)

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentType(query) must beSome.which(_ == "application/json")
    }

    "allow inserting a measurement with FAIL status" in new WithLoggedUser(FakeApp()) {
      Fixtures.truncate

      val jsonMeasurement = Json.parse("""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_status": "FAIL",
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9,
        #"meas_description": "md",
        #"device_status": "FAIL",
        #"test_status": "FAIL"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val found = AutomatedTest.find("company0", "site0", "device_group0", "tester0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)
      found.length must equalTo(1)
      found(0).meas_value must beTypedEqualTo(3: Int)
      found(0).meas_lower_limit.get must beTypedEqualTo(2: Int)
      found(0).meas_upper_limit.get must beTypedEqualTo(9: Int)

      val query = route(FakeRequest(GET, "/measurements_query?" +
        "company=company0&" +
        "site=site0&" +
        "device_group=device_group0&" +
        "tester=tester0&" +
        "beginTime=111000000000&" +
        "endTime=111000000001").withCookies(cookie)).get
      status(query) must equalTo(OK)
      contentType(query) must beSome.which(_ == "application/json")
      Json.parse(contentAsString(query)).as[JsArray].value(0) must equalTo(jsonMeasurement)
    }

    "validate meas_status field" in new WithLoggedUser(FakeApp()) {
      Fixtures.truncate

      val jsonMeasurement = Json.parse(s"""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_status": "pass",    ${"" /* lowercase not allowed */ }
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)
    }

    "validate device_status field" in new WithLoggedUser(FakeApp()) {
      Fixtures.truncate

      val jsonMeasurement = Json.parse(s"""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9,
        #"meas_description": "md",
        #"device_status": "badPASS", ${"" /* status suffix not allowed */ }
        #"test_status": "PASS"
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)
    }

    "validate test_status field" in new WithLoggedUser(FakeApp()) {
      Fixtures.truncate

      val jsonMeasurement = Json.parse(s"""#{
        #"company": "company0",
        #"site": "site0",
        #"device_group": "device_group0",
        #"tester": "tester0",
        #"ts": 111000000000,
        #"device_name": "dn",
        #"test_name": "t",
        #"meas_name": "m",
        #"meas_value": 3,
        #"meas_datatype": "long",
        #"meas_unit": "u",
        #"meas_status": "PASS",
        #"meas_lower_limit": 2,
        #"meas_upper_limit": 9,
        #"meas_description": "md",
        #"device_status": "PASS",
        #"test_status": "FAILbad"    ${"" /* status prefix not allowed */ }
        #}""".stripMargin('#'))

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        jsonMeasurement
      ).withCookies(cookie)).get
      status(create) must equalTo(BAD_REQUEST)
    }

    "optional empty string fields are dropped" in new WithLoggedUser(FakeApp()) {

      Fixtures.truncate

      val create = route(FakeRequest(
        POST,
        "/measurements",
        FakeHeaders(("Content-Type", Seq("text/json")) :: Nil),
        Json.parse(s"""#{
            #"company": "company0",
            #"site": "site0",
            #"device_group": "device_group0",
            #"tester": "tester0",
            #"ts": 111000000000,
            #"device_name": "dn",
            #"test_name": "t",
            #"meas_name": "m",
            #"meas_value": 0.2,
            #"meas_datatype": "double",
            #"meas_unit": "",            ${"" /* empty string field */ }
            #"meas_status": "PASS",
            #"meas_lower_limit": 0.0,
            #"meas_upper_limit": 0.5,
            #"meas_description": "",     ${"" /* empty string field */ }
            #"device_status": "PASS",
            #"test_status": "PASS"
            #}""".stripMargin('#'))
      ).withCookies(cookie)).get
      status(create) must equalTo(CREATED)

      val results = AutomatedTest.find("company0", "site0", "device_group0", "tester0",
        new Date(111000000000L), new Date(111000000001L),
        Ordering.Unspecified)

      results.length must equalTo(1)

      // Empty string fields are interpreted as absent and dropped when
      // the record is returned.
      results(0).meas_unit must beNone
      results(0).meas_description must beNone
    }
  }
}
