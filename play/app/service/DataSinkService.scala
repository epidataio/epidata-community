/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package service

import java.util.concurrent._
import java.util.{ Collections, Properties }
import com.epidata.lib.models.util.JsonHelpers
import com.fasterxml.jackson.databind.JsonMappingException
import controllers.SensorMeasurements._
import models.SensorMeasurement
import play.api.Logger
import play.api.libs.json._

import kafka.consumer.KafkaStream
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }

import scala.collection.JavaConversions._

class DataSinkService(
    val brokers: String,
    val groupId: String,
    val topic: String
) extends Logging {

  val props = createConsumerConfig(brokers, groupId)
  val consumer = new KafkaConsumer[String, String](props)
  var executor: ExecutorService = null

  def shutdown() = {
    if (consumer != null)
      consumer.close();
    if (executor != null)
      executor.shutdown();
  }

  def createConsumerConfig(brokers: String, groupId: String): Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run() = {
    consumer.subscribe(Collections.singletonList(this.topic))

    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val records = consumer.poll(1000)

          for (record <- records) {
            try {
              Configs.measurementClass match {
                case com.epidata.lib.models.AutomatedTest.NAME => models.AutomatedTest.insertRecordFromKafka(record.value().toString)
                case com.epidata.lib.models.SensorMeasurement.NAME => models.SensorMeasurement.insertRecordFromKafka(record.value().toString)
                case _ =>
              }
            } catch {
              case e: JsonMappingException => Logger.error(e.getMessage)
              case _: Throwable => Logger.error("Error while insert data to cassandra from data sink service")
            }
          }
        }
      }
    })
  }
}

