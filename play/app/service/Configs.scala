package service

import play.api.Configuration

object Configs {

  def init(config: Configuration) = {
    _keyCreation = config.getBoolean("application.ingestion.keycreation").getOrElse(false)
    _metricEnabled = config.getBoolean("application.metric.enabled").getOrElse(false)
  }

  private var _keyCreation = false
  private var _metricEnabled = false

  def ingestionKeyCreation = _keyCreation
  def metricEnabled = _keyCreation
}
