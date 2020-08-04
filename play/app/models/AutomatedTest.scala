/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

package models

import java.util.Date

import com.epidata.lib.models.{ AutomatedTest => Model, Measurement }
import _root_.util.Ordering
import play.api.Logger
import service.{ DataService, KafkaService, Configs }

object AutomatedTest {

  import com.epidata.lib.models.AutomatedTest._
  val logger: Logger = Logger(this.getClass())

  private def keyForMeasurementTopic(measurement: Model): String = {
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
   * @param automatedTest The AutomatedTest to insert.
   */
  def insert(automatedTest: Model): Unit = MeasurementService.insert(automatedTest)
  def insertList(automatedTests: List[Model]): Unit = MeasurementService.bulkInsert(automatedTests.map(automatedTestToMeasurement))

  def insertLite(automatedTest: Model): Unit = MeasurementServiceLite.insert(automatedTest)
  def insertListLite(automatedTests: List[Model]): Unit = MeasurementServiceLite.bulkInsert(automatedTests.map(automatedTestToMeasurement))

  def insertRecordFromKafka(str: String) = {
    Model.jsonToAutomatedTest(str) match {
      case Some(m) => insert(m)
      case _ => logger.error("Bad json format!")
    }
  }

  def insertToKafka(list: List[Model]): Unit = {
    list.foreach(m => insertToKafka(m))
    if (Configs.twoWaysIngestion) {
      models.AutomatedTest.insertList(list)
    }
  }

  def insertToKafka(m: Model): Unit = {
    val key = keyForMeasurementTopic(m)
    val value = Model.toJson(m)
    KafkaService.sendMessage(Measurement.KafkaTopic, key, value)
  }

  def toJson(automatedTests: List[Model]) = Model.toJson(automatedTests)

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
    ordering: Ordering.Value): List[Model] =
    MeasurementService.find(company, site, device_group, tester, beginTime, endTime, ordering)
      .map(measurementToAutomatedTest)

}
