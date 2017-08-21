/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

import cassandra.DB
import com.epidata.lib.models.{ Measurement => Model }
import com.epidata.lib.models.util.Binary
import java.util.Date
import models.Measurement
import models.SensorMeasurement
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import play.api.test._
import play.api.test.Helpers._
import util.Ordering

// scalastyle:off magic.number

@RunWith(classOf[JUnitRunner])
class MeasurementSpec extends Specification {

  object Fixtures {
    val truncateSQL = s"TRUNCATE ${Model.DBTableName}"
    def truncate = DB.cql(truncateSQL)

    def install = {
      truncate
      models.foreach(Measurement.insert(_))
    }

    val beginTime = new Date(1428004316000L)
    val halfEpoch = 1000L * 1000L * 1000L / 2
    val time = (0 to 9).map(i => new Date(beginTime.getTime + i * halfEpoch))
    val endTime = time(9)

    val models = time.map(t =>
      Model("customer0", "customer_site0", "collection0", "dataset0",
        t, Some("key10"), Some("key20"), Some("key30"), Some("double"), 2.3, Some("meas_unit0"),
        Some("meas_status0"), Some(1.0), Some(3.3), Some("meas_description0"),
        Some("val10"), Some("val20")))
  }

  "Measurement" should {

    "find results within a single epoch" in new WithApplication {
      Fixtures.install

      // Specify beginning and ending timestamps within a single epoch.
      val beginTime = Fixtures.time(0)
      val endTime = Fixtures.time(1)
      assume(Measurement.epochForTs(beginTime) ==
        Measurement.epochForTs(endTime))

      // Check that the results match.
      Measurement.find(
        "customer0",
        "customer_site0",
        "collection0",
        "dataset0",
        beginTime,
        endTime,
        Ordering.Unspecified
      ).toSet must
        equalTo(Fixtures.models.filter(x => x.customer == "customer0" &&
          x.customer_site == "customer_site0" &&
          x.collection == "collection0" &&
          x.dataset == "dataset0" &&
          x.ts.getTime >= beginTime.getTime &&
          x.ts.getTime < endTime.getTime).toSet)
    }

    "find results within two epochs" in new WithApplication {
      Fixtures.install

      // Specify beginning and ending timestamps within two adjacent epochs.
      val beginTime = Fixtures.time(0)
      val endTime = Fixtures.time(3)
      assume(Measurement.epochForTs(beginTime) + 1 ==
        Measurement.epochForTs(endTime))

      // Check that the results match.
      Measurement.find(
        "customer0",
        "customer_site0",
        "collection0",
        "dataset0",
        beginTime,
        endTime,
        Ordering.Unspecified
      ).toSet must
        equalTo(Fixtures.models.filter(x => x.customer == "customer0" &&
          x.customer_site == "customer_site0" &&
          x.collection == "collection0" &&
          x.dataset == "dataset0" &&
          x.ts.getTime >= beginTime.getTime &&
          x.ts.getTime < endTime.getTime).toSet)
    }

    "find results within three epochs" in new WithApplication {
      Fixtures.install

      // Specify beginning and ending timestamps within nearby epochs.
      val beginTime = Fixtures.time(0)
      val endTime = Fixtures.time(5)
      assume(Measurement.epochForTs(beginTime) + 2 ==
        Measurement.epochForTs(endTime))

      // Check that the results match.
      Measurement.find(
        "customer0",
        "customer_site0",
        "collection0",
        "dataset0",
        beginTime,
        endTime,
        Ordering.Unspecified
      ).toSet must
        equalTo(Fixtures.models.filter(x => x.customer == "customer0" &&
          x.customer_site == "customer_site0" &&
          x.collection == "collection0" &&
          x.dataset == "dataset0" &&
          x.ts.getTime >= beginTime.getTime &&
          x.ts.getTime < endTime.getTime).toSet)
    }

    "order results within a single epoch" in new WithApplication {
      Fixtures.install

      // Specify beginning and ending timestamps within a single epoch.
      val beginTime = Fixtures.time(0)
      val endTime = Fixtures.time(1)
      assume(Measurement.epochForTs(beginTime) ==
        Measurement.epochForTs(endTime))

      // Check that the results match, in order.
      Measurement.find(
        "customer0",
        "customer_site0",
        "collection0",
        "dataset0",
        beginTime,
        endTime,
        Ordering.Descending
      ).toList must
        equalTo(Fixtures.models.filter(x => x.customer == "customer0" &&
          x.customer_site == "customer_site0" &&
          x.collection == "collection0" &&
          x.dataset == "dataset0" &&
          x.ts.getTime >= beginTime.getTime &&
          x.ts.getTime < endTime.getTime).sortBy(-_.ts.getTime).toList)
    }

    "order results within two epochs" in new WithApplication {
      Fixtures.install

      // Specify beginning and ending timestamps within two adjacent epochs.
      val beginTime = Fixtures.time(0)
      val endTime = Fixtures.time(3)
      assume(Measurement.epochForTs(beginTime) + 1 ==
        Measurement.epochForTs(endTime))

      // Check that the results match, in order.
      Measurement.find(
        "customer0",
        "customer_site0",
        "collection0",
        "dataset0",
        beginTime,
        endTime,
        Ordering.Descending
      ).toList must
        equalTo(Fixtures.models.filter(x => x.customer == "customer0" &&
          x.customer_site == "customer_site0" &&
          x.collection == "collection0" &&
          x.dataset == "dataset0" &&
          x.ts.getTime >= beginTime.getTime &&
          x.ts.getTime < endTime.getTime).sortBy(-_.ts.getTime).toList)
    }

    "insert and find double measurements" in new WithApplication {
      Fixtures.truncate

      val measurement = Model("customer0", "customer_site0",
        "collection0", "dataset0", Fixtures.beginTime, Some("key10"),
        Some("key20"), Some("key30"), Some("double"), 5.1, Some("meas_unit0"), Some("meas_status0"),
        Some(1.1), Some(10.1), Some("meas_description0"), Some("val10"),
        Some("val20"))

      Measurement.insert(measurement)

      val found = Measurement.find("customer0", "customer_site0",
        "collection0", "dataset0", Fixtures.beginTime, Fixtures.endTime)
      found.length must equalTo(1)
      found(0).meas_value must beTypedEqualTo(5.1)
      found(0).meas_lower_limit.get must beTypedEqualTo(1.1)
      found(0).meas_upper_limit.get must beTypedEqualTo(10.1)
    }

    "insert and find long measurement" in new WithApplication {
      Fixtures.truncate

      val measurement = Model("customer0", "customer_site0",
        "collection0", "dataset0", Fixtures.beginTime, Some("key10"),
        Some("key20"), Some("key30"), Some("long"), 5: Long, Some("meas_unit0"), Some("meas_status0"),
        Some(1: Long), Some(10: Long), Some("meas_description0"), Some("val10"),
        Some("val20"))

      Measurement.insert(measurement)

      val found = Measurement.find("customer0", "customer_site0",
        "collection0", "dataset0", Fixtures.beginTime, Fixtures.endTime)
      found.length must equalTo(1)
      found(0).meas_value must beTypedEqualTo(5: Long)
      found(0).meas_lower_limit.get must beTypedEqualTo(1: Long)
      found(0).meas_upper_limit.get must beTypedEqualTo(10: Long)
    }

    "insert and find large long measurement" in new WithApplication {
      Fixtures.truncate

      val large = 3448388841L

      val measurement = Model("customer0", "customer_site0",
        "collection0", "dataset0", Fixtures.beginTime, Some("key10"),
        Some("key20"), Some("key30"), Some("long"), large, Some("meas_unit0"), Some("meas_status0"),
        Some(large - 1), Some(large + 1), Some("meas_description0"), Some("val10"),
        Some("val20"))

      Measurement.insert(measurement)

      val found = Measurement.find("customer0", "customer_site0",
        "collection0", "dataset0", Fixtures.beginTime, Fixtures.endTime)
      found.length must equalTo(1)
      found(0).meas_value must beTypedEqualTo(large)
      found(0).meas_lower_limit.get must beTypedEqualTo(large - 1)
      found(0).meas_upper_limit.get must beTypedEqualTo(large + 1)
    }

    "insert and find string measurements" in new WithApplication {
      Fixtures.truncate

      val measurement = Model("customer0", "customer_site0",
        "collection0", "dataset0", Fixtures.beginTime, Some("key10"),
        Some("key20"), Some("key30"), Some("string"), "MEAS", None, Some("meas_status0"),
        None, None, Some("meas_description0"), Some("val10"),
        Some("val20"))

      Measurement.insert(measurement)

      val found = Measurement.find("customer0", "customer_site0",
        "collection0", "dataset0", Fixtures.beginTime, Fixtures.endTime)
      found.length must equalTo(1)
      found(0).meas_value must beTypedEqualTo("MEAS")
    }

    "insert and find blob measurements" in new WithApplication {
      Fixtures.truncate

      val bytes: Binary = new Binary(Array('b'.toByte, 'i'.toByte, 'n'.toByte, 0.toByte))

      val measurement = Model("customer0", "customer_site0",
        "collection0", "dataset0", Fixtures.beginTime, Some("key10"),
        Some("key20"), Some("key30"), Some("binary"), bytes, Some("meas_unit0"), Some("meas_status0"),
        None, None, Some("meas_description0"), Some("val10"), Some("val20"))

      Measurement.insert(measurement)

      val found = Measurement.find("customer0", "customer_site0",
        "collection0", "dataset0", Fixtures.beginTime, Fixtures.endTime)
      found.length must equalTo(1)
      found(0).meas_value.asInstanceOf[Binary].backing must beTypedEqualTo(bytes.backing)
    }

  }
}
