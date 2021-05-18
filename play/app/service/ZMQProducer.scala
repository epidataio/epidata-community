/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import org.json.simple.{ JSONArray, JSONObject }
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import controllers.Assets.JSON
import play.api.libs.json.Json
import org.zeromq.ZMQ

object ZMQProducer {
  var pushSocket: ZMQ.Socket = _
  var pubSocket: ZMQ.Socket = _
  var context: ZMQ.Context = _

  var pushPort: String = _
  var pubPort: String = _

  def init(pushPort: String, pubPort: String): ZMQProducer.type = {
    //creating ZMQ context which will be used for PUB and PUSH
    this.pushPort = pushPort
    this.pubPort = pubPort
    context = ZMQ.context(1)

    //using context to create PUSH and PUB models and binding them to sockets
    pushSocket = context.socket(ZMQ.PUSH)
    pushSocket.connect("tcp://127.0.0.1:" + this.pushPort)

    println("ZMQProducer Init called")

    pubSocket = context.socket(ZMQ.PUB)
    pubSocket.bind("tcp://127.0.0.1:" + this.pubPort)
    this
  }

  /**
   * Encapsulating key and value as Message object and pushing data to DataSink
   */
  def push(key: String, value: String): Unit = {
    //println("ZMQProducer push called")
    val map = new JLinkedHashMap[String, String]()

    //setting the topic as measurements_original
    map.put("topic", "measurements_original")
    map.put("key", key)
    map.put("value", value)
    val message: String = JSONObject.toJSONString(map)

    //push the message
    pushSocket.send(message.getBytes(ZMQ.CHARSET), 0)
    println("Pushed: " + message + "\n")
  }

  /**
   * Encapsulating key and value as Message object and publishing data to Stream
   */
  def pub(key: String, value: String): Unit = {
    //println("ZMQProducer pub called")

    //setting the topic as measurements_original
    pubSocket.sendMore("measurements_original")

    //create the message
    val map = new JLinkedHashMap[String, String]()
    map.put("topic", "measurements_original")
    map.put("key", key)
    map.put("value", value)
    val message: String = JSONObject.toJSONString(map)

    //publish the message
    pubSocket.send(message.getBytes(ZMQ.CHARSET), 0)
    println("Published: " + message + "\n")
  }

  def clear(): Unit = {
    println("ZMQProducer clear called")

    try {
      //      pushSocket.send(STOP_MESSAGE, 0)
      pushSocket.unbind("tcp://127.0.0.1:" + this.pushPort)
      pushSocket.close()
    } catch {
      case e: Throwable => println(e)
    }

    try {
      //      pubSocket.send(STOP_MESSAGE, 0)
      pubSocket.unbind("tcp://127.0.0.1:" + this.pubPort)
      pubSocket.close()
    } catch {
      case e: Throwable => println(e)
    }
  }

}
