/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

import cassandra.DB
import com.datastax.driver.core.exceptions.InvalidQueryException
import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._
import play.api.test._
import play.api.test.Helpers._
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DBSpec extends Specification {

  "DB" should {

    "throw an exception on read execution failure" in new WithApplication {
      def a = DB.cql("SELECT * FROM nonexistent_table")
      a must throwA[InvalidQueryException]
    }

    "throw an exception on write execution failure" in new WithApplication {
      def a = DB.cql("INSERT INTO nonexistent_table (id) VALUES (1)")
      a must throwA[InvalidQueryException]
    }

  }
}
