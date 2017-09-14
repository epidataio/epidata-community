/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package service

import java.security.MessageDigest

import com.epidata.lib.models.SensorMeasurement
import com.epidata.lib.models.util.JsonHelpers
import models.{ SensorMeasurement => Model }
import util.EpidataMetrics

object DataService {

  val MeasurementTopic = "measurements"
  private val Delim = "_"
  private var registered_tokens: Seq[String] = List.empty
  private var saveToCassandra: Boolean = false

  def init(tokens: java.util.List[String], twoWaysIngestion: Boolean) = {
    import collection.JavaConverters._
    registered_tokens = tokens.asScala
    saveToCassandra = twoWaysIngestion
  }

  def getMd5(inputStr: String): String = {
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    md.digest(inputStr.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }
  }

  private def keyForMeasurementTopic(measurement: SensorMeasurement): String = {
    val key =
      s"""
         |${measurement.customer}${Delim}
         |${measurement.customer_site}${Delim}
         |${measurement.collection}${Delim}
         |${measurement.dataset}${Delim}
         |${measurement.epoch}
       """.stripMargin
    getMd5(key)
  }

  /**
   * Insert a measurement into the kafka.
   * @param sensorMeasurement The Measurement to insert.
   */
  def insert(sensorMeasurement: SensorMeasurement): Unit = {

    val key = keyForMeasurementTopic(sensorMeasurement)
    val value = JsonHelpers.toJson(sensorMeasurement)
    KafkaService.sendMessage(MeasurementTopic, key, value)
  }

  def insert(sensorMeasurementList: List[SensorMeasurement]): Unit = {
    sensorMeasurementList.foreach(m => insert(m))
  }

  def isValidToken(token: String): Boolean = {
    registered_tokens.contains(token)
  }

}

