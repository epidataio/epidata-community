package service

import service.ZMQService
import service.ZMQDataSink
import service.Configs

object ZMQInit {
  private var _ZMQProducer: ZMQService.type = _
  private var _ZMQDataSink: ZMQDataSink.type = _
  private var _ZMQSparkStreaming: ZMQDataSink.type = _

  def ZMQProducer: ZMQService.type = _ZMQProducer
  def ZMQDataSink: ZMQDataSink.type = _ZMQDataSink
  def ZMQSparkStreaming: ZMQDataSink.type = _ZMQSparkStreaming

  def init() = {
    //pushPort: 5550, //pubPort: 5551
    _ZMQProducer = ZMQService.init(Configs.queueSocket.toString, (Configs.queueSocket + 1).toString)
    //subPort: 5551, //pubPort: 5552
    _ZMQSparkStreaming = ZMQDataSink.init((Configs.queueSocket + 1).toString, (Configs.queueSocket + 2).toString)
    //pushPort: 5550, //pubPort: 5552
    _ZMQDataSink = ZMQDataSink.init(Configs.queueSocket.toString, (Configs.queueSocket + 2).toString)
  }
}
