/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import java.util.Date

import com.epidata.lib.models.{ Measurement, MeasurementCleansed, MeasurementSummary, AutomatedTest => BaseAutomatedTest, AutomatedTestCleansed => BaseAutomatedTestCleansed, AutomatedTestSummary => BaseAutomatedTestSummary }
import _root_.util.Ordering
import play.api.Logger
import models.AutomatedTest.keyForMeasurementTopic
import service.{ Configs, DataService, KafkaService, ZMQProducer, ZMQInit }

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
    BaseAutomatedTestCleansed.jsonToAutomatedTestCleansed(str) match {
      case Some(mc) => insertCleansed(mc, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertSummaryRecordFromZMQ(str: String): Unit = {
    BaseAutomatedTestSummary.jsonToAutomatedTestSummary(str) match {
      case Some(ms) => insertSummary(ms, Configs.measDBLite)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertToKafka(m: BaseAutomatedTest): Unit = {
    val key = keyForMeasurementTopic(m)
    val value = BaseAutomatedTest.toJson(m)
    KafkaService.sendMessage(Measurement.KafkaTopic, key, value)
  }

  def insertToKafka(list: List[BaseAutomatedTest]): Unit = {
    list.foreach(m => insertToKafka(m))
    if (Configs.twoWaysIngestion) {
      models.AutomatedTest.insert(list, Configs.measDBLite)
    }
  }

  def insertToZMQ(m: BaseAutomatedTest): Unit = {
    val key = keyForMeasurementTopic(m)
    val value = BaseAutomatedTest.toJson(m)
    ZMQInit._ZMQProducer.push(key, value)
    ZMQInit._ZMQProducer.pub(key, value)
  }

  def insertToZMQ(list: List[BaseAutomatedTest]): Unit = {
    list.foreach(m => insertToZMQ(m))
    if (Configs.twoWaysIngestion) {
      models.AutomatedTest.insert(list, Configs.measDBLite)
    }
  }

  /** Convert a list of AutomatedTest measurements to a json representation. */
  def toJson(automatedTests: List[BaseAutomatedTest]) = BaseAutomatedTest.toJson(automatedTests)

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
      .map(measurementToAutomatedTest)

}
