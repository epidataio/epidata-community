/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package controllers

import java.security.MessageDigest
import javax.inject.{ Singleton, Inject }

import com.epidata.lib.models.{ SensorMeasurement, AutomatedTest }
import play.api.http._
import play.api.routing._
import play.api.mvc.RequestHeader
import play.api.{ Configuration, Environment }

import scala.util.matching.Regex

@Singleton
class MeasurementRoutesHandler @Inject() (
  errorHandler: HttpErrorHandler,
  httpConf: HttpConfiguration,
  filters: HttpFilters,
  routes: Router,
  sensorRoutes: sensor_measurement.Routes,
  ateRoutes: automated_test.Routes)(implicit val conf: Configuration) extends DefaultHttpRequestHandler(
  routes,
  errorHandler,
  httpConf,
  filters) {

  // Routes for measurements based on customer configuration.
  private lazy val measurementRoutes =
    conf.get[String]("measurement-class") match {
      case SensorMeasurement.NAME => sensorRoutes
      case AutomatedTest.NAME => ateRoutes
    }

  // Attempt customized measurements routes first, then default routes.
  override def routeRequest(request: RequestHeader) = {
    measurementRoutes.handlerFor(request) orElse super.routeRequest(request)
  }
}
