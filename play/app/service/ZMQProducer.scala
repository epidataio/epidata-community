/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import org.json.simple.{ JSONArray, JSONObject }
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import controllers.Assets.JSON
import com.epidata.lib.models.util.JsonHelpers._
import com.epidata.lib.models.util.Message
import play.api.libs.json.Json
import org.zeromq.ZMQ
import play.api.Logger

object ZMQProducer {
  var pushSocket: ZMQ.Socket = _
  var pubSocket: ZMQ.Socket = _
  var context: ZMQ.Context = _

  var pushPort: String = _
  var pubPort: String = _

  val logger: Logger = Logger(this.getClass())

  def init(context: ZMQ.Context, pushPort: String, pubPort: String): ZMQProducer.type = {
    //creating ZMQ context which will be used for PUB and PUSH
    this.pushPort = pushPort
    this.pubPort = pubPort
    //context = ZMQ.context(1)
    this.context = context

    //using context to create PUSH and PUB models and binding them to sockets
    pushSocket = this.context.socket(ZMQ.PUSH)
    pushSocket.connect("tcp://127.0.0.1:" + this.pushPort)

    println("ZMQProducer initialized")

    pubSocket = this.context.socket(ZMQ.PUB)
    pubSocket.bind("tcp://127.0.0.1:" + this.pubPort)
    this
  }

  /**
   * Encapsulating key and value as Message object and pushing data to DataSink
   */
  def push(key: String, value: String): Unit = {
    //println("ZMQProducer push called")

    //create the message
    val message: String = messageToJson(Message(key, value))

    //push the message
    pushSocket.send(message.getBytes(ZMQ.CHARSET), 0)
    //println("Pushed: " + message + "\n")
  }

  /**
   * Encapsulating key and value as Message object and publishing data to Stream
   */
  def pub(topic: String, key: String, value: String): Unit = {
    //println("ZMQProducer pub called")

    //setting the topic as measurements_original
    pubSocket.sendMore(topic)

    //create the message
    val message: String = messageToJson(Message(key, value))

    //publish the message
    pubSocket.send(message.getBytes(ZMQ.CHARSET), 0)
    //println("Published: " + message + "\n")
  }

  def clear(): Unit = {
    try {
      //      pushSocket.send(STOP_MESSAGE, 0)
      pushSocket.send("$TERM")
      pushSocket.unbind("tcp://127.0.0.1:" + this.pushPort)
      pushSocket.close()
      println("DataSource push service closed successfully")
    } catch {
      case e: Throwable => println("Exception while closing ZMQ push socket", e)
    }

    try {
      //      pubSocket.send(STOP_MESSAGE, 0)
      pubSocket.send("$TERM")
      pubSocket.unbind("tcp://127.0.0.1:" + this.pubPort)
      pubSocket.close()
      println("DataSource pub service closed successfully")
    } catch {
      case e: Throwable => println("Exception while closing ZMQ Pub socket", e)
    }
  }

}
