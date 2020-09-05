/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import controllers.Assets.JSON
import org.zeromq.ZMQ

object ZMQService {
  var pushSocket: ZMQ.Socket = _
  var pubSocket: ZMQ.Socket = _
  var context: ZMQ.Context = _

  def init(pushPort: String, pubPort: String): ZMQService.type = {
    //creating ZMQ context which will be used for PUB and PUSH
    context = ZMQ.context(1)
    //using context to create PUSH and PUB models and binding them to sockets
    pushSocket = context.socket(ZMQ.PUSH)
    pushSocket.connect("tcp://127.0.0.1:" + pushPort)

    pubSocket = context.socket(ZMQ.PUB)
    pubSocket.bind("tcp://127.0.0.1:" + pubPort)
    this
  }

  def push(key: String, value: String): Unit = {
    /**
     * Below we are setting a topic and pushing the data
     */
    val message: String = JSON.format(Message(key, value))
    pushSocket.send(message.getBytes(), 0)
    println("Pushed: " + message)
  }

  def pub(key: String, value: String): Unit = {
    /**
     * Below we are setting a topic and publishing the data
     */
    //setting the topic as measurements
    pubSocket.sendMore("measurements")
    //sending the message
    val message: String = JSON.format(Message(key, value))
    pubSocket.send(message.getBytes(ZMQ.CHARSET), 0)
    println("Published: " + message)
  }

  def end(): Unit = {
    pushSocket.close()
    pubSocket.close()
  }
}
