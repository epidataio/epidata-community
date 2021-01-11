/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import play.api.Configuration

import scala.collection.mutable

case class Message(topic: Object, key: Object, value: Object)

object ZMQInit {

  def init(config: Configuration) = {
    /**
     * ZMQProducer: pushPort: 5550, pubPort: 5551
     * ZMQStream: subPort: 5551, pubPort: 5551
     * ZMQDataSink: pullPort: 5550, subPort: 5551
     * ZMQService executes ZMQStream and ZMQDataSink as threads
     */
    _ZMQProducer = ZMQProducer.init(config.getOptional[Int]("queue.servers").get.toString, (config.getOptional[Int]("queue.servers").get + 1).toString)
    //_ZMQService.startThreads(config)

    //_streamQueue = mutable.Queue[Message]()
  }

  private var _ZMQProducer: ZMQProducer.type = _
  //private var _ZMQService: ZMQService.type = _

  //var _streamQueue: mutable.Queue[Message] = _

  def ZMQProducer = _ZMQProducer
  //def ZMQSinkService = _ZMQService

  //def streamQueue = _streamQueue
}
