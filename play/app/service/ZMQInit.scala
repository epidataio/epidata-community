/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import play.api.Configuration

import scala.collection.mutable

case class Message(topic: Object, key: Object, value: Object)

object ZMQInit {
  var _ZMQProducer: ZMQProducer.type = _
  var _ZMQService: ZMQService.type = _

  def init(config: Configuration) = {
    /**
     * ZMQProducer: pushPort: 5550, pubPort: 5551
     * //  ZMQStream: subPort: 5551, pubPort: 5552
     * ZMQDataSink: pullPort: 5550, subPort: 5551
     * ZMQService executes ZMQStream and ZMQDataSink as threads
     */

    _ZMQService = ZMQService

    _ZMQService.startThreads(config)

    _ZMQProducer = ZMQProducer.init(config.getOptional[Int]("queue.servers").get.toString, (config.getOptional[Int]("queue.servers").get + 1).toString)

    //_streamQueue = mutable.Queue[Message]()
  }
  //var _streamQueue: mutable.Queue[Message] = _
  //def streamQueue = _streamQueue

  def clear() = {
    _ZMQService.stop()
    _ZMQProducer.clear()
  }
}
