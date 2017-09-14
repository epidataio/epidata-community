package util

import service.Configs

object EpidataMetrics {
  private var metricsMap: Map[String, Long] = Map[String, Long]().withDefaultValue(0L)

  // t0: millis time.
  def increment(key: String, startTime: Long) = {
    if (Configs.metricEnabled) {
      val currentTime = getCurrentTime
      val value = metricsMap(key) + (currentTime - startTime)
      metricsMap = metricsMap + (key -> value)
      incrementCount(key + "_count")
    }
  }

  def incrementCount(key: String) = {
    if (Configs.metricEnabled) {
      val value = metricsMap(key) + 1
      metricsMap = metricsMap + (key -> value)
    }
  }

  def getMetric: Map[String, Long] = metricsMap

  def getCurrentTime = System.currentTimeMillis()
}
