/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import controllers.Assets.JSON
import org.json.simple.JSONObject
import org.zeromq.ZMQ

object ZMQStream {
  var subSocket: ZMQ.Socket = _
  var forwardSocket: ZMQ.Socket = _

  def init(pubPort: String, forwardPort: String): ZMQStream.type = {
    val context = ZMQ.context(1)

    subSocket = context.socket(ZMQ.SUB)
    subSocket.connect("tcp://127.0.0.1:" + pubPort)
    subSocket.subscribe("measurements".getBytes(ZMQ.CHARSET))
    subSocket.subscribe("passBack".getBytes(ZMQ.CHARSET))

    forwardSocket = context.socket(ZMQ.PUB)
    forwardSocket.bind("tcp://127.0.0.1:" + forwardPort)
    this
  }

  def receive(): Unit = {
    val topic = subSocket.recvStr() //measurements or passBack
    val messageObject = new JSONObject(subSocket.recvStr()) //JSON formatted Message {"topic":[topic]"key":[key],"value":[message]}
    ZMQInit.streamQueue.enqueue(new Message(messageObject.get("topic"), messageObject.get("key"), messageObject.get("value")))
  }

  def processAndPublish(): Unit = {
    val processedMessage: Message = epidataLiteStreaming(ZMQInit.streamQueue.dequeue)
    if (processedMessage.topic == "passBack") {
      forwardSocket.sendMore("passBack")
      val msg: String = JSON.format(processedMessage)
      forwardSocket.send(msg.getBytes(), 0)
      println(msg)
    } else {
      forwardSocket.sendMore("cleansed")
      val msg: String = JSON.format(processedMessage)
      forwardSocket.send(msg.getBytes(), 0)
      println(msg)
    }
  }
}
