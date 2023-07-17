/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package models

import java.util.Date

import com.epidata.lib.models.{ Measurement, MeasurementCleansed, MeasurementSummary, AutomatedTest => BaseAutomatedTest, AutomatedTestCleansed => BaseAutomatedTestCleansed, AutomatedTestSummary => BaseAutomatedTestSummary }
import play.api.Logger
import play.api.libs.json._
import _root_.util.Ordering
import models.AutomatedTest.keyForMeasurementTopic
import service.{ Configs, DataService, KafkaService, ZMQProducer, ZMQInit }

import scala.collection.convert.WrapAsScala
import scala.language.implicitConversions

object AutomatedTest {

  import com.epidata.lib.models.AutomatedTest._
  import com.epidata.lib.models.AutomatedTestCleansed._
  import com.epidata.lib.models.AutomatedTestSummary._

  val logger: Logger = Logger(this.getClass())

  val name: String = "AutomatedTest"

  private def keyForMeasurementTopic(measurement: BaseAutomatedTest): String = {
    val key =
      s"""
         |${measurement.customer}${DataService.Delim}
         |${measurement.customer_site}${DataService.Delim}
         |${measurement.collection}${DataService.Delim}
         |${measurement.dataset}${DataService.Delim}
         |${measurement.epoch}
       """.stripMargin
    DataService.getMd5(key)
  }

  /**
   * Insert an automated test measurement into the database.
   * @param automatedTest The AutomatedTest measurement to insert.
   */
  def insert(automatedTest: BaseAutomatedTest, sqliteEnable: Boolean): Unit = {
    if (sqliteEnable) {
      SQLiteMeasurementService.insert(automatedTest)
    } else {
      MeasurementService.insert(automatedTest)
    }
  }

  /**
   * Insert multiple automated test measurements into the database.
   * @param automatedTests Multiple AutomatedTest measurements to insert.
   */
  def insert(automatedTests: List[BaseAutomatedTest], sqliteEnable: Boolean): Unit = {
    if (sqliteEnable) {
      SQLiteMeasurementService.bulkInsert(automatedTests.map(automatedTestToMeasurement))
    } else {
      MeasurementService.bulkInsert(automatedTests.map(automatedTestToMeasurement))
    }
  }

  /**
   * Insert an automated test cleansed measurement into the database.
   * @param automatedTestCleansed The AutomatedTestCleansed measurement to insert.
   */
  def insertCleansed(automatedTestCleansed: BaseAutomatedTestCleansed, sqliteEnable: Boolean): Unit = {
    // println("automated test cleansed: " + automatedTestCleansed)
    if (sqliteEnable) {
      SQLiteMeasurementService.insertCleansed(automatedTestCleansed)
    } else {
      // To Do
      //MeasurementService.insertCleansed(automatedTestCleansed)
    }
  }

  /**
   * Insert multiple automated test cleansed measurements into the database.
   * @param automatedTestsCleansed Multiple AutomatedTestCleansed measurements to insert.
   */
  def insertCleansed(automatedTestsCleansed: List[BaseAutomatedTestCleansed], sqliteEnable: Boolean): Unit = {
    if (sqliteEnable) {
      SQLiteMeasurementService.bulkInsertCleansed(automatedTestsCleansed.map(automatedTestCleansedToMeasurementCleansed))
    } else {
      // To Do
      //MeasurementService.bulkInsertCleansed(automatedTestsCleansed.map(automatedTestCleansedToMeasurementCleansed))
    }
  }

  /**
   * Insert an automated test measurement summary into the database.
   * @param automatedTestSummary The AutomatedTestSummary data to insert.
   */
  def insertSummary(automatedTestSummary: BaseAutomatedTestSummary, sqliteEnable: Boolean): Unit = {
    if (sqliteEnable) {
      SQLiteMeasurementService.insertSummary(automatedTestSummary)
    } else {
      // To Do
      //MeasurementService.insertSummary(automatedTestSummary)
    }
  }

  /**
   * Insert multiple automated test measurement summary data into the database.
   * @param automatedTestsSummary Multiple AutomatedTestSummary data to insert.
   */
  def insertSummary(automatedTestsSummary: List[BaseAutomatedTestSummary], sqliteEnable: Boolean): Unit = {
    if (sqliteEnable) {
      SQLiteMeasurementService.bulkInsertSummary(automatedTestsSummary.map(automatedTestSummaryToMeasurementSummary))
    } else {
      // To Do
      //MeasurementService.bulkInsertSummary(automatedTestsSummary.map(automatedTestSummaryToMeasurementSummary))
    }
  }

  def insertRecordFromKafka(str: String) = {
    BaseAutomatedTest.jsonToAutomatedTest(str) match {
      case Some(m) => insert(m, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertRecordFromZMQ(str: String): Unit = {
    BaseAutomatedTest.jsonToAutomatedTest(str) match {
      case Some(m) => insert(m, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertCleansedRecordFromZMQ(str: String): Unit = {
    // println("cleansed record from zmq: " + str + "\n")
    BaseAutomatedTestCleansed.jsonToAutomatedTestCleansed(str) match {
      case Some(mc) => insertCleansed(mc, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertSummaryRecordFromZMQ(str: String): Unit = {
    // println("summary record from zmq: " + str)
    BaseAutomatedTestSummary.jsonToAutomatedTestSummary(str) match {
      case Some(ms) => insertSummary(ms, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertDynamicRecordFromZMQ(str: String): Unit = {
    // println("dynamic record from zmq: " + str)
    try {
      BaseAutomatedTestCleansed.jsonToAutomatedTestCleansed(str) match {
        case Some(mc: BaseAutomatedTestCleansed) => {
          // println("inserting dynamic record as measurements cleansed record.\n")
          insertCleansed(mc, Configs.measDBLite)
          return
        }
        case _ => throw new Exception("Dynamic data is not of type MeasurementCleansed.")
      }
    } catch {
      case _: Throwable => {
        try {
          BaseAutomatedTestSummary.jsonToAutomatedTestSummary(str) match {
            case Some(ms: BaseAutomatedTestSummary) => {
              // println("inserting dynamic record as measurements summary record.\n")
              insertSummary(ms, Configs.measDBLite)
              return
            }
            case _ => throw new Exception("Dynamic data is not of type MeasurementSummary.")
          }
        } catch {
          case _: Throwable => {
            logger.error("Bad json format!")
          }
        }
      }
    }
  }

  /**
   * Insert a measurement into the kafka.
   * @param automatedTest The Measurement to insert.
   */
  def insertToKafka(automatedTest: BaseAutomatedTest): Unit = {
    val key = keyForMeasurementTopic(automatedTest)
    val value = BaseAutomatedTest.toJson(automatedTest)
    KafkaService.sendMessage(Measurement.KafkaTopic, key, value)
  }

  def insertToKafka(automatedTestList: List[BaseAutomatedTest]): Unit = {
    automatedTestList.foreach(m => insertToKafka(m))

    // Two way ingestion NOT supported with SQLite
    if (Configs.twoWaysIngestion && !Configs.measDBLite) {
      models.AutomatedTest.insert(automatedTestList, Configs.measDBLite)
    }
  }

  /**
   * Insert a measurement into the ZMQ.
   * @param automatedTest The Measurement to insert.
   */
  def insertToZMQ(automatedTest: BaseAutomatedTest): Unit = {
    val key = keyForMeasurementTopic(automatedTest)
    val value = BaseAutomatedTest.toJson(automatedTest)
    // logger.info("key: " + key + ", value: " + value)

    ZMQInit._ZMQProducer.push(key, value)
    ZMQInit._ZMQProducer.pub(Measurement.zmqTopic, key, value)
  }

  def insertToZMQ(automatedTestList: List[BaseAutomatedTest]): Unit = {
    automatedTestList.foreach(m => insertToZMQ(m))

    // Two way ingestion NOT supported with SQLite
    if (Configs.twoWaysIngestion && !Configs.measDBLite) {
      models.AutomatedTest.insert(automatedTestList, Configs.measDBLite)
    }
  }

  /** Convert a list of AutomatedTest measurements to a json representation. */
  def toJson(automatedTests: List[BaseAutomatedTest]) = BaseAutomatedTest.toJson(automatedTests)

  def query(
    company: String,
    site: String,
    device_group: String,
    tester: String,
    beginTime: Date,
    endTime: Date,
    size: Int = 1000,
    batch: String = "",
    ordering: Ordering.Value,
    sqliteEnable: Boolean): List[BaseAutomatedTest] = {
    if (sqliteEnable) {
      SQLiteMeasurementService.query(company, site, device_group, tester, beginTime, endTime, size, batch, ordering)
        .map(BaseAutomatedTest.measurementToAutomatedTest)
    } else {
      MeasurementService.query(company, site, device_group, tester, beginTime, endTime, size, batch, ordering)
        .map(BaseAutomatedTest.measurementToAutomatedTest)
    }
  }

  def queryCleansed(
    company: String,
    site: String,
    device_group: String,
    tester: String,
    beginTime: Date,
    endTime: Date,
    size: Int = 1000,
    batch: String = "",
    ordering: Ordering.Value,
    sqliteEnable: Boolean): List[BaseAutomatedTestCleansed] = {
    if (sqliteEnable) {
      SQLiteMeasurementService.queryCleansed(company, site, device_group, tester, beginTime, endTime, size, batch, ordering)
        .map(BaseAutomatedTestCleansed.measurementCleansedToAutomatedTestCleansed)
    } else {
      MeasurementService.queryCleansed(company, site, device_group, tester, beginTime, endTime, size, batch, ordering)
        .map(BaseAutomatedTestCleansed.measurementCleansedToAutomatedTestCleansed)
    }
  }

  def querySummary(
    company: String,
    site: String,
    device_group: String,
    tester: String,
    beginTime: Date,
    endTime: Date,
    size: Int = 1000,
    batch: String = "",
    ordering: Ordering.Value,
    sqliteEnable: Boolean): List[BaseAutomatedTestSummary] = {
    if (sqliteEnable) {
      SQLiteMeasurementService.querySummary(company, site, device_group, tester, beginTime, endTime, size, batch, ordering)
        .map(BaseAutomatedTestSummary.measurementSummaryToAutomatedTestSummary)
    } else {
      MeasurementService.querySummary(company, site, device_group, tester, beginTime, endTime, size, batch, ordering)
        .map(BaseAutomatedTestSummary.measurementSummaryToAutomatedTestSummary)
    }
  }

  /**
   * Find automated tests in the database matching the specified parameters.
   * @param company
   * @param site
   * @param device_group
   * @param tester
   * @param beginTime Beginning of query time interval, inclusive
   * @param endTime End of query time interval, exclusive
   * @param ordering Timestamp ordering of results, if specified.
   */
  @Deprecated
  def find(
    company: String,
    site: String,
    device_group: String,
    tester: String,
    beginTime: Date,
    endTime: Date,
    ordering: Ordering.Value): List[BaseAutomatedTest] =
    MeasurementService.find(company, site, device_group, tester, beginTime, endTime, ordering)
      .map(BaseAutomatedTest.measurementToAutomatedTest)

}
