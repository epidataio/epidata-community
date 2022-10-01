/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package service

import play.api.Configuration

object Configs {

  def init(config: Configuration) = {
    _keyCreation = config.getOptional[Boolean]("application.ingestion.keycreation").getOrElse(false)
    _metricEnabled = config.getOptional[Boolean]("application.metric.enabled").getOrElse(false)
    _measurementClass = config.getOptional[String]("measurement-class").get
    _twoWaysIngestion = config.getOptional[Boolean]("application.ingestion.2ways").getOrElse(false)
    _measDB = config.getOptional[String]("measurements.database").get
    _userDB = config.getOptional[String]("user.database").get
    _deviceDB = config.getOptional[String]("device.database").get
    _queueService = config.getOptional[String]("queue.service").get
    _queueSocket = config.getOptional[Int]("queue.servers").get
  }

  private var _keyCreation: Boolean = _
  private var _metricEnabled: Boolean = _
  private var _measurementClass: String = _
  private var _twoWaysIngestion: Boolean = _
  private var _measDB: String = _
  private var _userDB: String = _
  private var _deviceDB: String = _
  private var _queueService: String = _
  private var _queueSocket: Int = _

  def ingestionKeyCreation = _keyCreation
  def metricEnabled = _keyCreation
  def measurementClass = _measurementClass
  def twoWaysIngestion = _twoWaysIngestion
  def measDB = _measDB
  def userDB = _userDB
  def deviceDB = _deviceDB
  def measDBLite: Boolean = _measDB match {
    case "sqlite" => true
    case "cassandra" => false
  }
  def userDBLite: Boolean = _userDB match {
    case "sqlite" => true
    case "cassandra" => false
  }
  def deviceDBLite: Boolean = _deviceDB match {
    case "sqlite" => true
    case "cassandra" => false
  }
  def queueService = _queueService
  def queueSocket = _queueSocket

}
