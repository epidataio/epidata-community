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
  val context = ZMQ.context(1)

  def init(config: Configuration) = {
    /**
     * ZMQProducer: pushPort: 5550, pubPort: 5551
     * ZMQDataSink: pullPort: 5550, subPort: 5551
     * ZMQService executes ZMQStream and ZMQDataSink as threads
     */

    //_ZMQService = ZMQService
    //_ZMQService.start(config)
    //_ZMQProducer = ZMQProducer.init(config.getOptional[Int]("queue.servers").get.toString, (config.getOptional[Int]("queue.servers").get + 1).toString)

    _ZMQService = ZMQService.init(
      context,
      config.getOptional[Int]("queue.servers").get.toString,
      (config.getOptional[Int]("queue.servers").get + 2).toString)
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
