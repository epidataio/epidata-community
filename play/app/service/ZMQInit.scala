package service

import play.api.Configuration

case class Message(topic: String, message: String)

object ZMQInit {

  def init(config: Configuration) = {
    //pushPort: 5550, pubPort: 5551
    _ZMQProducer = ZMQService.init(config.getOptional[Int]("queue.servers").get.toString, (config.getOptional[Int]("queue.servers").get + 1).toString)
    //subPort: 5551, pubPort: 5551
    _ZMQStreamService = ZMQStreamService.init((config.getOptional[Int]("queue.servers").get + 1).toString, (config.getOptional[Int]("queue.servers").get + 1).toString)
    //pullPort: 5550, pubPort: 5551
    _ZMQSinkService = ZMQDataSink.init(config.getOptional[Int]("queue.servers").get.toString, (config.getOptional[Int]("queue.servers").get + 1).toString)
  }

  private var _ZMQProducer: ZMQService.type = _
  private var _ZMQStreamService: ZMQStreamService.type = _
  private var _ZMQSinkService: ZMQDataSink.type = _

  def ZMQProducer = _ZMQProducer
  def ZMQStream = _ZMQStreamService
  def ZMQSinkService = _ZMQSinkService
}
