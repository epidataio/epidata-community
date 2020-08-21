package service

import play.api.Configuration

object ZMQInit {

  def init(config: Configuration) = {
    //pushPort: 5550, pubPort: 5551
    _ZMQProducer = ZMQService.init(config.getOptional[Int]("queue.servers").get.toString, (config.getOptional[Int]("queue.servers").get + 1).toString)
    //pullPort: 0000, subPort: 5551, pubPort: 5552
    _ZMQSparkStreaming = ZMQDataSink.init("0000", (config.getOptional[Int]("queue.servers").get + 1).toString, (config.getOptional[Int]("queue.servers").get + 2).toString)
    //pullPort: 5550, pubPort: 5552
    _ZMQSinkService = ZMQDataSink.init(config.getOptional[Int]("queue.servers").get.toString, (config.getOptional[Int]("queue.servers").get + 2).toString)
  }

  private var _ZMQProducer: ZMQService.type = _
  private var _ZMQSparkStreaming: ZMQDataSink.type = _
  private var _ZMQSinkService: ZMQDataSink.type = _

  def ZMQProducer = _ZMQProducer
  def ZMQSparkStreaming = _ZMQSparkStreaming
  def ZMQSinkService = _ZMQSinkService
}
