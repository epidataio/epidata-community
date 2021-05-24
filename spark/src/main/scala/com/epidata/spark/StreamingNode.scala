/*
* Copyright (c) 2020 EpiData, Inc.
*/
package com.epidata.spark

import org.json.simple.{ JSONArray, JSONObject }
import org.json.simple.parser.{ ParseException, JSONParser }
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import com.epidata.spark.ops.Transformation
import play.api.libs.json._
import play.api.libs.json.Reads._
import org.zeromq.ZMQ
import com.epidata.lib.models.{ Measurement => BaseMeasurement, SensorMeasurement => BaseSensorMeasurement, AutomatedTest => BaseAutomatedTest }
import scala.io.StdIn

class StreamingNode {
  var subSocket: ZMQ.Socket = _ //add as parameter
  var publishSocket: ZMQ.Socket = _ //add as parameter
  var subscribePort: String = _
  var publishPort: String = _
  var subscribeTopic: String = _
  var publishTopic: String = _
  var transformation: Transformation = _

  def init(
    context: ZMQ.Context,
    receivePort: String,
    sendPort: String,
    receiveTopic: String,
    sendTopic: String,
    receiveTimeout: Integer,
    transformation: Transformation): StreamingNode = {

    subscribePort = receivePort
    publishPort = sendPort
    subscribeTopic = receiveTopic
    publishTopic = sendTopic

    //println("StreamingNode init called.")
    //println("subscribePort: " + subscribePort + ", publishPort: " + publishPort)
    //println("subscribeTopic: " + subscribeTopic + ", publishTopic: " + publishTopic)

    subSocket = context.socket(ZMQ.SUB)
    //subSocket.setReceiveTimeOut(receiveTimeout)
    subSocket.connect("tcp://127.0.0.1:" + subscribePort)
    subSocket.subscribe(subscribeTopic.getBytes(ZMQ.CHARSET))

    publishSocket = context.socket(ZMQ.PUB)
    publishSocket.bind("tcp://127.0.0.1:" + publishPort)

    this.transformation = transformation

    //println("Streaming Node initiated. Subscribe topic: " + subscribeTopic + ", publish topic: " + publishTopic)

    this
  }

  def receive(): Unit = {
    //println("StreamingNode receive method called")
    println("receive port: " + subscribePort)

    val topic = subSocket.recvStr()
    println("receive topic: " + topic)
    val parser = new JSONParser()
    val receivedString = subSocket.recvStr()
    println("received message: " + receivedString + "\n")
    receivedString match {
      case _: String => {
        val jSONObject = parser.parse(receivedString).asInstanceOf[JMap[String, String]]

        //println("received data: " + jSONObject + "\n")

        val map = new JLinkedHashMap[String, String]()
        map.put("topic", publishTopic)
        map.put("key", jSONObject.get("key"))
        map.put("value", jSONObject.get("value"))

        //println("publish data: " + map + "\n")
        publish(map)
      }
      case _ => println("receive string is null. \n")
    }
  }

  def publish(processedMap: JMap[String, String]): Unit = {
    //val processedMessage: Message = epidataLiteStreamingContext(ZMQInit.streamQueue.dequeue)
    //println("Streamingnode publish method called")

    publishSocket.sendMore(this.publishTopic)
    val msg: String = JSONObject.toJSONString(processedMap)
    publishSocket.send(msg.getBytes(), 0)

    println("publish topic: " + this.publishTopic + ", publish port: " + publishPort)
    println("published message: " + msg + "\n")
  }

  def clear(): Unit = {
    try {
      subSocket.unsubscribe(subscribeTopic.getBytes(ZMQ.CHARSET))
      subSocket.unbind("tcp://127.0.0.1:" + subscribePort)
      subSocket.close()
      println("subSocket closed successfully")
    } catch {
      case e: Throwable => println(e)
    }

    try {
      //      publishSocket.send(ZMQ.STOP_MESSAGE, 0)
      publishSocket.unbind("tcp://127.0.0.1:" + publishPort)
      publishSocket.close()
      println("publishSocket closed successfully")
    } catch {
      case e: Throwable => println(e)
    }

  }
}
