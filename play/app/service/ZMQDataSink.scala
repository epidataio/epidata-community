/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import org.json.simple.{ JSONArray, JSONObject }
import org.json.simple.parser.{ ParseException, JSONParser }
import com.epidata.lib.models.util.JsonHelpers._
import com.epidata.lib.models.util.Message
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }

import controllers.Assets.JSON
import play.api.libs.json.Json
import org.zeromq.ZMQ
import org.zeromq.ZMQException
import play.api.Logger

object ZMQDataSink {
  var context: ZMQ.Context = _
  var pullSocket: ZMQ.Socket = _
  var subSocket: ZMQ.Socket = _
  var forwardMessage: ZMQ.Socket = _
  val subTopicOriginal: String = "measurements_original"
  val subTopicCleansed: String = "measurements_cleansed"
  val subTopicSummary: String = "measurements_summary"

  //private val context: ZMQ.Context = ZMQ.context(1)

  val logger: Logger = Logger(this.getClass())

  def init(context: ZMQ.Context, pullPort: String, subPort: String): ZMQDataSink.type = {
    //creating ZMQ context which will be used for PUB and PUSH
    //val context = ZMQ.context(1)
    this.context = context

    println("ZMQDataSink init called")

    //using context to create PUSH and PUB models and binding them to sockets
    //pullSocket = context.socket(ZMQ.PULL)
    //pullSocket.bind("tcp://127.0.0.1:" + pullPort)
    //println("pull port: " + pullPort)

    //subSocket = context.socket(ZMQ.SUB)
    //subSocket.connect("tcp://127.0.0.1:" + subPort)
    //println("sub port: " + subPort)

    //subSocket.subscribe(subTopicOriginal.getBytes(ZMQ.CHARSET))
    //subSocket.subscribe(subTopicCleansed.getBytes(ZMQ.CHARSET))
    //subSocket.subscribe(subTopicSummary.getBytes(ZMQ.CHARSET))
    this
  }

  def initPull(pullPort: String): Unit = {
    //val context = zmqContext
    //using context to create PUSH model and binding it to socket
    pullSocket = context.socket(ZMQ.PULL)
    pullSocket.setLinger(0)
    pullSocket.bind("tcp://127.0.0.1:" + pullPort)
    //println("pull port: " + pullPort)
  }

  def initSub(subPort: String): Unit = {
    //val context = zmqContext
    //using context to create PUB model and binding it to socket
    subSocket = context.socket(ZMQ.SUB)
    subSocket.setLinger(0)
    subSocket.connect("tcp://127.0.0.1:" + subPort)
    //println("sub port: " + subPort)

    subSocket.subscribe(subTopicOriginal.getBytes(ZMQ.CHARSET))
    subSocket.subscribe(subTopicCleansed.getBytes(ZMQ.CHARSET))
    subSocket.subscribe(subTopicSummary.getBytes(ZMQ.CHARSET))
  }

  def pull(): Message = {
    println("ZMQDataSink pull called")

    try {
      val receivedString = pullSocket.recvStr()
      //println("pulled string: " + receivedString + "\n")

      //    val parser = new JSONParser()
      //    val messageObject = parser.parse(receivedString).asInstanceOf[Message]
      val messageObject = jsonToMessage(receivedString)
      messageObject
    } catch {
      case e: Throwable => {
        println("Unhandled exception in DataSink pull service")
        throw e
      }
    }
  }

  def sub(): Message = {
    println("ZMQDataSink sub called.")

    try {
      val topic = subSocket.recvStr()
      val receivedString = subSocket.recvStr()

      //println("subscribe topic: " + topic)
      //println("subscribed received string: " + receivedString + "\n")

      //    val parser = new JSONParser()
      //    val messageObject = parser.parse(receivedString).asInstanceOf[Message]

      val messageObject = jsonToMessage(receivedString)
      messageObject
    } catch {
      case e: Throwable => {
        println("Unhandled exception in DataSink sub service")
        throw e
      }
    }
  }

  def clearPull(pullPort: String, subPort: String): Unit = {
    println("DataSink pull service disconnecting from DataSource")
    try {
      //pullSocket.send(STOP_MESSAGE, 0);
      pullSocket.setLinger(0)
      pullSocket.unbind("tcp://127.0.0.1:" + pullPort)
      pullSocket.close()
      println("DataSink pull service closed successfully")
    } catch {
      case e: Throwable => println("Exception while closing DataSink pull service", e.getMessage)
    }
  }

  def clearSub(pullPort: String, subPort: String): Unit = {
    println("DataSink sub service disconnecting from DataSource")

    try {
      //subSocket.send(STOP_MESSAGE, 0);
      //subSocket.unsubscribe(subTopicOriginal.getBytes(ZMQ.CHARSET))
      //subSocket.unsubscribe(subTopicCleansed.getBytes(ZMQ.CHARSET))
      //subSocket.unsubscribe(subTopicSummary.getBytes(ZMQ.CHARSET))
      subSocket.setLinger(0)
      subSocket.disconnect("tcp://127.0.0.1:" + subPort)
      subSocket.close()
      println("DataSink subscription service closed successfully")
    } catch {
      case e: Throwable => println("Exception while closing DataSink sub service", e.getMessage)
    }
  }

}
