/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import play.api.Configuration

import scala.collection.mutable
import org.zeromq.ZMQ

// case class Message(topic: Object, key: Object, value: Object)

object ZMQInit {
  var _ZMQProducer: ZMQProducer.type = _
  var _ZMQService: ZMQService.type = _
  var context: ZMQ.Context = _

  def init(config: Configuration) = {
    /**
     * ZMQProducer: pushPort: 5550, pubPort: 5551
     * ZMQPullDataSink: pullPort: 5550,
     * ZMQCleansedDataSink: cleansedSubPort: 5552
     * ZMQSummaryDataSink: summarySubPort: 5553
     * ZMQDynamicDataSink: dynamicSubPort: 5554
     * ZMQService executes ZMQStream and ZMQDataSink as threads
     */

    this.context = ZMQ.context(1)

    _ZMQService = ZMQService.init(
      context,
      config.getOptional[Int]("queue.servers").get.toString,
      (config.getOptional[Int]("queue.servers").get + 2).toString,
      (config.getOptional[Int]("queue.servers").get + 3).toString,
      (config.getOptional[Int]("queue.servers").get + 4).toString)

    _ZMQService.start()

    _ZMQProducer = ZMQProducer.init(
      context,
      config.getOptional[Int]("queue.servers").get.toString,
      (config.getOptional[Int]("queue.servers").get + 1).toString)

  }

  def clear() = {
    _ZMQService.stop()
    _ZMQProducer.clear()
    context.term()
  }
}
