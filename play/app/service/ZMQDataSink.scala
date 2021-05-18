/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import org.json.simple.{ JSONArray, JSONObject }
import org.json.simple.parser.{ ParseException, JSONParser }
import com.epidata.lib.models.util.JsonHelpers._
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }

import controllers.Assets.JSON
import play.api.libs.json.Json
import org.zeromq.ZMQ

object ZMQDataSink {
  var pullSocket: ZMQ.Socket = _
  var subSocket: ZMQ.Socket = _
  var forwardMessage: ZMQ.Socket = _
  val subTopicOriginal: String = "measurements_original"
  val subTopicCleansed: String = "measurements_cleansed"
  val subTopicSummary: String = "measurements_summary"

  def init(pushPort: String, pullPort: String): ZMQDataSink.type = {
    //creating ZMQ context which will be used for PUB and PUSH
    val context = ZMQ.context(1)

    println("ZMQDataSink init called")

    //using context to create PUSH and PUB models and binding them to sockets
    pullSocket = context.socket(ZMQ.PULL)
    pullSocket.bind("tcp://127.0.0.1:" + pushPort)
    println("pull port: " + pushPort)

    subSocket = context.socket(ZMQ.SUB)
    subSocket.connect("tcp://127.0.0.1:" + pullPort)
    println("sub port: " + pullPort)

    subSocket.subscribe(subTopicOriginal.getBytes(ZMQ.CHARSET))
    subSocket.subscribe(subTopicCleansed.getBytes(ZMQ.CHARSET))
    subSocket.subscribe(subTopicSummary.getBytes(ZMQ.CHARSET))
    this
  }

  def pull(): JMap[String, String] = {
    //println("ZMQDataSink pull called")
    val receivedString = pullSocket.recvStr()
    println("pulled string: " + receivedString + "\n")

    val parser = new JSONParser()
    val jSONObject = parser.parse(receivedString).asInstanceOf[JMap[String, String]]
    jSONObject
  }

  def sub(): JMap[String, String] = {
    //println("ZMQDataSink sub called.")
    val topic = subSocket.recvStr()
    val receivedString = subSocket.recvStr()

    println("subscribe topic: " + topic)
    println("subscribed received string: " + receivedString + "\n")

    val parser = new JSONParser()
    val jSONObject = parser.parse(receivedString).asInstanceOf[JMap[String, String]]
    jSONObject
  }

  def clearPull(pushPort: String, pullPort: String): Unit = {
    //println("ZMQDataSink clear pull called")

    try {
      pullSocket.unbind("tcp://127.0.0.1:" + pushPort)
      pullSocket.close()
    } catch {
      case e: Throwable => println(e)
    }
  }

  def clearSub(pushPort: String, pullPort: String): Unit = {
    //println("ZMQDataSink clear subscribe called")

    try {
      subSocket.unsubscribe(subTopicOriginal.getBytes(ZMQ.CHARSET))
      subSocket.unsubscribe(subTopicCleansed.getBytes(ZMQ.CHARSET))
      subSocket.unsubscribe(subTopicSummary.getBytes(ZMQ.CHARSET))
      subSocket.unbind("tcp://127.0.0.1:" + pullPort)
      subSocket.close()
    } catch {
      case e: Throwable => println(e)
    }
  }

}
