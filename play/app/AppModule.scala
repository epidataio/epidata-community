/*
 * Copyright (c) 2015-2017 EpiData, Inc.
*/

import cassandra.DB
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.epidata.lib.models.{ SensorMeasurement, AutomatedTest }
import play.api._
import play.api.{ Configuration, Environment }
import service.{ Configs, DataService, DataSinkService, KafkaService, AppEnvironment }
import providers.{ DemoProvider }
import scala.concurrent.Future
import javax.inject._
import play.api.inject.{ Binding, Module, ApplicationLifecycle }
import securesocial.core.RuntimeEnvironment

import scala.collection.JavaConverters._

class AppModule extends Module {
  def bindings(env: Environment, conf: Configuration) = Seq(
    (bind[RuntimeEnvironment].to[AppEnvironment]).eagerly(),
    (bind[ApplicationStart].toSelf).eagerly(),
    (bind[ApplicationStop].toSelf).eagerly())
}

// `ApplicationStop` object with shut-down hook
@Singleton
class ApplicationStop @Inject() (lifecycle: ApplicationLifecycle) {
  lifecycle.addStopHook { () =>
    Future.successful(DB.close)
  }
}

// `ApplicationStart` object for application start-up
@Singleton
class ApplicationStart @Inject() (env: Environment, conf: Configuration) {
  Configs.init(conf)

  // Connect to the Cassandra database.
  try {
    DB.connect(
      conf.getOptional[String]("cassandra.node").get,
      conf.getOptional[Configuration]("pillar.epidata").get
        .getOptional[Configuration](env.mode.toString.toLowerCase).get
        .getOptional[String]("cassandra-keyspace-name").get,
      conf.getOptional[Configuration]("pillar.epidata").get
        .getOptional[Configuration](env.mode.toString.toLowerCase).get
        .getOptional[String]("replicationStrategy").getOrElse("SimpleStrategy"),
      conf.getOptional[Configuration]("pillar.epidata").get
        .getOptional[Configuration](env.mode.toString.toLowerCase).get
        .getOptional[Int]("replicationFactor").getOrElse(1),
      conf.getOptional[String]("cassandra.username").get,
      conf.getOptional[String]("cassandra.password").get,
      env.getFile("conf/pillar/migrations/epidata"))

    val kafkaServers = conf.getOptional[String]("kafka.servers").get
    KafkaService.init(kafkaServers)

    val tokens = seqAsJavaList(conf.getOptional[Seq[String]]("application.api.tokens").get)

    DataService.init(tokens)

    if (!conf.getOptional[Boolean]("application.ingestion.2ways").getOrElse(false)) {
      val kafkaConsumer = new DataSinkService(kafkaServers, "data-sink-group", DataService.MeasurementTopic)
      kafkaConsumer.run()
    }

  } catch {
    case e: NoHostAvailableException =>
      throw new IllegalStateException(s"Unable to connect to cassandra server: ${e}")
  }

}
