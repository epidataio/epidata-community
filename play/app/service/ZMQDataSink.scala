package service

import org.zeromq.ZMQ

object ZMQDataSink {
  var pullSocket: ZMQ.Socket = _
  var subSocket: ZMQ.Socket = _
  var forwardMessage: ZMQ.Socket = _

  def init(pushPort: String, forwardPort: String): ZMQDataSink.type = {
    //creating ZMQ context which will be used for PUB and PUSH
    val context = ZMQ.context(1)

    //using context to create PUSH and PUB models and binding them to sockets
    pullSocket = context.socket(ZMQ.PULL)
    pullSocket.bind("tcp://127.0.0.1:" + pushPort)

    subSocket = context.socket(ZMQ.SUB)
    subSocket.connect("tcp://127.0.0.1:" + forwardPort)
//    subSocket.subscribe("Raw".getBytes(ZMQ.CHARSET))
//    subSocket.subscribe("Summary".getBytes(ZMQ.CHARSET))
//    subSocket.subscribe("Cleansed".getBytes(ZMQ.CHARSET))
    subSocket.subscribe("Processed".getBytes(ZMQ.CHARSET))
    this
  }

  def pull() = {
    val message = pullSocket.recvStr()
    println("Pulled Message: " + message /*+ " Topic: " + Thread.currentThread().getName*/ )
    Message("measurements", message)
  }

  def sub() = {
    val topic = subSocket.recvStr()
    val message = subSocket.recvStr()
    println("Subscribed Message: " + message + " Topic: " + topic)
    Message(topic, message)
  }

  def end(): Unit = {
    subSocket.close()
    subSocket.close()
  }
}
