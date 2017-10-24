package service

import play.api.Configuration

object Configs {

  def init(config: Configuration) = {
    _keyCreation = config.getBoolean("application.ingestion.keycreation").getOrElse(false)
    _metricEnabled = config.getBoolean("application.metric.enabled").getOrElse(false)
    _measurementClass = config.getString("measurement-class").get
    _twoWaysIngestion = config.getBoolean("application.ingestion.2ways").getOrElse(false)
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
