/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

import cassandra.DB
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.epidata.lib.models.{ SensorMeasurement, AutomatedTest }
import play.api._
import play.api.mvc._
import play.api.mvc.Results._
import service.{ Configs, DataService, DataSinkService, KafkaService }
import scala.concurrent.Future

object Global extends GlobalSettings {

  override def onStart(app: Application) {

    Configs.init(app.configuration)

    // Connect to the Cassandra database.
    try {
      DB.connect(
        app.configuration.getString("cassandra.node").get,
        app.configuration
        .getConfig("pillar.epidata").get
        .getConfig(app.mode.toString.toLowerCase).get
        .getString("cassandra-keyspace-name").get,
        app.configuration.getString("cassandra.username").get,
        app.configuration.getString("cassandra.password").get
      )

      val kafkaServers = app.configuration.getString("kafka.servers").get
      KafkaService.init(kafkaServers)

      val tokens = app.configuration.getStringList("application.api.tokens").get

      DataService.init(tokens)

      if (!app.configuration.getBoolean("application.ingestion.2ways").getOrElse(false)) {
        val kafkaConsumer = new DataSinkService(kafkaServers, "data-sink-group", DataService.MeasurementTopic)
        kafkaConsumer.run()
      }

    } catch {
      case e: NoHostAvailableException =>
        throw new IllegalStateException(s"Unable to connect to cassandra server: ${e}")
    }
  }

  // Routing for /measurements is determined based on customer configuration.
  // This attribute holds the associated routes.
  private lazy val measurementRoutes =
    Play.current.configuration.getString("measurement-class").get match {
      case AutomatedTest.NAME => automated_test.Routes
      case SensorMeasurement.NAME => sensor_measurement.Routes
    }

  override def onRouteRequest(req: RequestHeader): Option[Handler] =
    // Try the customized measurements routes first, then standard routes.
    measurementRoutes.handlerFor(req) orElse super.onRouteRequest(req)

  override def onStop(app: Application) {
    DB.close
  }

  override def onBadRequest(request: RequestHeader, error: String) = {
    Future.successful(BadRequest("Bad Request: " + error))
  }

  override def onError(request: RequestHeader, throwable: Throwable) = {
    Future.successful(InternalServerError(views.html.errors.onError(throwable)))
  }

  override def onHandlerNotFound(request: RequestHeader) = {
    Future.successful(NotFound(views.html.errors.onHandlerNotFound(request)))
  }
}
