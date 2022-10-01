/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package service

import org.apache.kafka.clients.producer.{ Callback, ProducerRecord, RecordMetadata, KafkaProducer }
import org.apache.kafka.common.PartitionInfo

import scala.collection.JavaConverters._
import scala.concurrent.{ Future, Promise }
import scala.util.control.NonFatal
import scala.util.{ Failure, Success, Try }

object KafkaService {

  private var producer: KafkaProducer[String, String] = null

  def init(servers: String) = {
    val props = new java.util.Properties()
    props.put("bootstrap.servers", servers)
    props.put("client.id", "KafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    producer = new KafkaProducer[String, String](props)
  }

  def sendMessage(topic: String, key: String, value: String) = {
    val record = new ProducerRecord[String, String](topic, key, value)
    producer.send(record)
  }

  def send(topic: String, key: String, value: String): Future[RecordMetadata] = {
    val record = new ProducerRecord[String, String](topic, key, value)
    send(record)
  }

  /**
   * Asynchronously send a record to a topic, providing a `Future` to contain the result of the operation.
   *
   * @param record `ProducerRecord` to sent
   * @return the results of the sent records as a `Future`
   */
  def send(record: ProducerRecord[String, String]): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    try {
      producer.send(record, producerCallback(promise))
    } catch {
      case NonFatal(e) => promise.failure(e)
    }

    promise.future
  }

  /**
   * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
   *
   * @param record `ProducerRecord` to sent
   * @param callback callback that is called when the send has been acknowledged
   */
  def sendWithCallback(record: ProducerRecord[String, String])(callback: Try[RecordMetadata] => Unit): Unit = {
    producer.send(record, producerCallback(callback))
  }

  /**
   * Make all buffered records immediately available to send and wait until records have been sent.
   *
   */
  def flush(): Unit =
    producer.flush()

  /**
   * Get the partition metadata for the give topic.
   *
   */
  def partitionsFor(topic: String): List[PartitionInfo] =
    producer.partitionsFor(topic).asScala.toList

  /**
   * Close this producer.
   *
   */
  def close(): Unit =
    producer.close()

  private def producerCallback(promise: Promise[RecordMetadata]): Callback =
    producerCallback(result => promise.complete(result))

  private def producerCallback(callback: Try[RecordMetadata] => Unit): Callback = {
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        val result =
          if (exception == null) Success(metadata)
          else Failure(exception)
        callback(result)
      }
    }
  }

}
