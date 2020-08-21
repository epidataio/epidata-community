package service

import play.api.Configuration

object Configs {

  def init(config: Configuration) = {
    _keyCreation = config.getOptional[Boolean]("application.ingestion.keycreation").getOrElse(false)
    _metricEnabled = config.getOptional[Boolean]("application.metric.enabled").getOrElse(false)
    _measurementClass = config.getOptional[String]("measurement-class").get
    _twoWaysIngestion = config.getOptional[Boolean]("application.ingestion.2ways").getOrElse(false)
    _DBMeas = config.getOptional[Boolean]("SQLite.enable").getOrElse(false)
    _queueService = config.getOptional[String]("queue.service").get
    _queueSocket = config.getOptional[Int]("queue.servers").get
  }

  private var _keyCreation = false
  private var _metricEnabled = false
  private var _measurementClass: String = "sensor_measurement"
  private var _twoWaysIngestion = false
  private var _DBMeas = true
  private var _queueService: String = "ZMQ"
  private var _queueSocket = 0

  def ingestionKeyCreation = _keyCreation
  def metricEnabled = _keyCreation
  def measurementClass = _measurementClass
  def twoWaysIngestion = _twoWaysIngestion
  def DBMeas = _DBMeas
  def queueService = _queueService
  def queueSocket = _queueSocket
}
