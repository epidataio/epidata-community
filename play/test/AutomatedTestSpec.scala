/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

import cassandra.DB
import com.epidata.lib.models.{ AutomatedTest => Model }
import com.epidata.lib.models.Measurement
import java.util.Date
import models.AutomatedTest
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import play.api.test._
import play.api.test.Helpers._
import util.Ordering

// scalastyle:off magic.number

@RunWith(classOf[JUnitRunner])
class AutomatedTestSpec extends Specification {

  object Fixtures {
    val truncateSQL = s"TRUNCATE ${Measurement.DBTableName}"
    def truncate = DB.cql(truncateSQL)

    def install = {
      truncate
      models.foreach(AutomatedTest.insert(_))
    }

    val beginTime = new Date(1428704316000L)
    val time = (1 to 10).map(x => new Date(beginTime.getTime + x * 1000))
    val endTime = time(9)

    val models =
      Model("co_a", "si_a", "dg_a", "te_a", time(0), "de_a", "te_a",
        "na_a", 0.2, Some("un_a"), Some("st_a"), Some(0.0), Some(0.5),
        Some("de_a"), Some("ds_a"), Some("ts_a")) ::
        Model("co_a", "si_a", "dg_a", "te_a", time(1), "de_a", "te_b",
          "na_a", 0.2, Some("un_a"), Some("st_a"), Some(0.0), Some(0.5),
          Some("de_a"), Some("ds_a"), Some("ts_a")) ::
          Model("co_a", "si_a", "dg_a", "te_a", time(2), "de_b", "te_b",
            "na_a", 0.2, Some("un_a"), Some("st_a"), Some(0.0), Some(0.5),
            Some("de_a"), Some("ds_a"), Some("ts_a")) ::
            Model("co_a", "si_a", "dg_a", "te_a", time(3), "de_b", "te_c",
              "na_a2", 0.2, Some("un_a"), Some("st_a"), Some(0.0), Some(0.5),
              Some("de_a"), Some("ds_a"), Some("ts_a")) ::
              Model("co_b", "si_a", "dg_a", "te_a", time(4), "de_a", "te_a",
                "na_a2", 0.2, Some("un_a"), Some("st_a"), Some(0.0), Some(0.5),
                Some("de_a"), Some("ds_a"), Some("ts_a")) ::
                Model("co_a", "si_b", "dg_a", "te_a", time(5), "de_a", "te_a",
                  "na_b", 0.2, Some("un_b"), Some("st_b"), Some(0.0), Some(0.5),
                  Some("de_b"), Some("ds_a"), Some("ts_a")) ::
                  Model("co_a", "si_a", "dg_b", "te_a", time(6), "de_a", "te_a",
                    "na_c", 0.2, Some("un_c"), Some("st_c"), Some(0.0), Some(0.5),
                    Some("de_c"), Some("ds_a"), Some("ts_a")) ::
                    Model("co_a", "si_a", "dg_a", "te_b", time(7), "de_a", "te_a",
                      "na_d", 0.2, Some("un_d"), Some("st_d"), Some(0.0), Some(0.5),
                      Some("de_d"), Some("ds_a"), Some("ts_a")) :: Nil
  }

  "AutomatedTest" should {

    "insert an AutomatedTest" in new WithApplication {

      Fixtures.truncate

      val ts = Fixtures.beginTime
      val epoch = Measurement.epochForTs(ts)

      // Value should not exist before insert.
      DB.cql(
        s"SELECT * FROM ${Measurement.DBTableName} WHERE customer = 'com' AND " +
          " customer_site = 'sit' AND collection = 'dg' AND dataset = 'tes' " +
          " AND epoch = ? AND ts = ? AND key1 = 'dev' AND key2 = 'tna' AND key3 = 'mea'",
        epoch: java.lang.Integer, ts
      ).all.size must equalTo(0)

      AutomatedTest.insert(Model("com", "sit", "dg", "tes", ts, "dev",
        "tna", "mea", 1.0, Some("uni"), Some("sta"), Some(0.0), Some(2.0), Some("des"),
        Some("dst"), Some("tst")))

      // Value should be correct after insert.
      val row =
        DB.cql(
          s"SELECT * FROM ${Measurement.DBTableName} WHERE customer = 'com' AND " +
            " customer_site = 'sit' AND collection = 'dg' AND dataset = 'tes' " +
            " AND epoch = ? AND ts = ? AND key1 = 'dev' AND key2 = 'tna' AND key3 = 'mea'",
          epoch: java.lang.Integer, ts
        ).one
      row.getString("customer") must equalTo("com")
      row.getString("customer_site") must equalTo("sit")
      row.getString("collection") must equalTo("dg")
      row.getString("dataset") must equalTo("tes")
      row.getInt("epoch") must equalTo(epoch)
      row.getTimestamp("ts") must equalTo(ts)
      row.getString("key1") must equalTo("dev")
      row.getString("key2") must equalTo("tna")
      row.getString("key3") must equalTo("mea")
      row.getDouble("meas_value") must equalTo(1.0)
      row.getString("meas_unit") must equalTo("uni")
      row.getString("meas_status") must equalTo("sta")
      row.getDouble("meas_lower_limit") must equalTo(0.0)
      row.getDouble("meas_upper_limit") must equalTo(2.0)
      row.getString("meas_description") must equalTo("des")
      row.getString("val1") must equalTo("dst")
      row.getString("val2") must equalTo("tst")
    }

    "select by company, site, device_group, and tester" in new WithApplication {
      Fixtures.install
      AutomatedTest.find(
        "co_a",
        "si_a",
        "dg_a",
        "te_a",
        Fixtures.beginTime,
        Fixtures.endTime,
        Ordering.Unspecified
      ).toSet must
        equalTo(Fixtures.models.filter(x => x.company == "co_a" &&
          x.site == "si_a" &&
          x.device_group == "dg_a" &&
          x.tester == "te_a").toSet)
    }

    "select within a time range" in new WithApplication {
      Fixtures.install
      AutomatedTest.find(
        "co_a",
        "si_a",
        "dg_a",
        "te_a",
        Fixtures.time(1),
        Fixtures.time(3),
        Ordering.Unspecified
      ).toSet must
        equalTo(Fixtures.models.filter(x => x.company == "co_a" &&
          x.site == "si_a" &&
          x.device_group == "dg_a" &&
          x.tester == "te_a" &&
          x.ts.getTime >= Fixtures.time(1).getTime &&
          x.ts.getTime < Fixtures.time(3).getTime).toSet)
    }

    "order by time, ascending" in new WithApplication {
      Fixtures.install
      AutomatedTest.find(
        "co_a",
        "si_a",
        "dg_a",
        "te_a",
        Fixtures.beginTime,
        Fixtures.endTime,
        Ordering.Ascending
      ) must
        equalTo(Fixtures.models.filter(x => x.company == "co_a" &&
          x.site == "si_a" &&
          x.device_group == "dg_a" &&
          x.tester == "te_a").sortBy(_.ts.getTime))
    }

    "order by time, descending" in new WithApplication {
      Fixtures.install
      AutomatedTest.find(
        "co_a",
        "si_a",
        "dg_a",
        "te_a",
        Fixtures.beginTime,
        Fixtures.endTime,
        Ordering.Descending
      ) must
        equalTo(Fixtures.models.filter(x => x.company == "co_a" &&
          x.site == "si_a" &&
          x.device_group == "dg_a" &&
          x.tester == "te_a").sortBy(-_.ts.getTime))
    }

  }
}
