/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import org.json.simple.JSONObject
import org.zeromq.ZMQ

object ZMQDataSink {
  var pullSocket: ZMQ.Socket = _
  var subSocket: ZMQ.Socket = _
  var forwardMessage: ZMQ.Socket = _

  def init(pushPort: String, pullPort: String): ZMQDataSink.type = {
    //creating ZMQ context which will be used for PUB and PUSH
    val context = ZMQ.context(1)

    //using context to create PUSH and PUB models and binding them to sockets
    pullSocket = context.socket(ZMQ.PULL)
    pullSocket.bind("tcp://127.0.0.1:" + pushPort)

    subSocket = context.socket(ZMQ.SUB)
    subSocket.connect("tcp://127.0.0.1:" + pullPort)
    subSocket.subscribe("measurements_substituted".getBytes(ZMQ.CHARSET))
    subSocket.subscribe("measurement_cleansed".getBytes(ZMQ.CHARSET))
    subSocket.subscribe("measurements_summary".getBytes(ZMQ.CHARSET))
    this
  }

  def pull() = {
    val messageObject = new JSONObject(pullSocket.recvStr())
    new Message(messageObject.get("topic"), messageObject.get("key"), messageObject.get("value"))
  }

  def sub() = {
    val topic = subSocket.recvStr()
    val messageObject = new JSONObject(subSocket.recvStr())
    new Message(messageObject.get("topic"), messageObject.get("key"), messageObject.get("value"))
  }

  def end(): Unit = {
    subSocket.close()
    subSocket.close()
  }
}
