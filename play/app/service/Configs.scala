package service

import play.api.Configuration

object Configs {

  def init(config: Configuration) = {
    _keyCreation = config.getOptional[Boolean]("application.ingestion.keycreation").getOrElse(false)
    _metricEnabled = config.getOptional[Boolean]("application.metric.enabled").getOrElse(false)
    _measurementClass = config.getOptional[String]("measurement-class").get
    _twoWaysIngestion = config.getOptional[Boolean]("application.ingestion.2ways").getOrElse(false)
  }

  private var _keyCreation = false
  private var _metricEnabled = false
  private var _measurementClass: String = "sensor_measurement"
  private var _twoWaysIngestion = false

  def ingestionKeyCreation = _keyCreation
  def metricEnabled = _keyCreation
  def measurementClass = _measurementClass
  def twoWaysIngestion = _twoWaysIngestion
}
