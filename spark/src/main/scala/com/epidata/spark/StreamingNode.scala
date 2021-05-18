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

object StreamingNode {
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
    transformation: Transformation): StreamingNode.type = {

    subscribePort = receivePort
    publishPort = sendPort
    subscribeTopic = receiveTopic
    publishTopic = sendTopic

    println("StreamingNode init called.")
    println("subscribePort: " + subscribePort + ", publishPort: " + publishPort)
    println("subscribeTopic: " + subscribeTopic + ", publishTopic: " + publishTopic)
    println("Enter 'Q' to continue.")
    while ((StdIn.readChar()).toLower.compare('q') != 0) {
      println("subSocket being connected. Enter 'Q' to continue.")
    }

    subSocket = context.socket(ZMQ.SUB)
    //subSocket.setReceiveTimeOut(receiveTimeout)
    subSocket.connect("tcp://127.0.0.1:" + subscribePort)
    subSocket.subscribe(subscribeTopic.getBytes(ZMQ.CHARSET))

    println("subSocket connected. Enter 'Q' to continue.")
    while ((StdIn.readChar()).toLower.compare('q') != 0) {
      println("subSocket connected. Enter 'Q' to continue.")
    }

    publishSocket = context.socket(ZMQ.PUB)
    publishSocket.bind("tcp://127.0.0.1:" + publishPort)

    println("pubSocket connected. Enter 'Q' to continue.")
    while ((StdIn.readChar()).toLower.compare('q') != 0) {
      println("pubSocket connected. Enter 'Q' to continue.")
    }

    this.transformation = transformation

    println("Streaming Node initiated. Subscribe topic: " + subscribeTopic + ", publish topic: " + publishTopic)

    this
  }

  //  def init(context: ZMQ.context,
  //           receivePort: String,
  //           publishPort: String,
  //           receiveTopic: Array[String],
  //           publishTopic: String,
  //           transformation: Transformation): StreamingNode.type = {
  //    subSocket = context.socket(ZMQ.SUB)
  //    subSocket.connect("tcp://127.0.0.1:" + receivePort)
  //    for (topic <- receiveTopic) {
  //      subSocket.subscribe(topic.getBytes(ZMQ.CHARSET))
  //    }
  //
  //    publishSocket = context.socket(ZMQ.PUB)
  //    publishSocket.bind("tcp://127.0.0.1:" + publishPort)
  //
  //    this.publishTopic = publishTopic
  //
  //    this.transformation = transformation
  //    this
  //  }
  //
  //  def init(context: ZMQ.context,
  //           receivePort: String,
  //           publishPort: String,
  //           receiveTopic: Array[String],
  //           publishTopic: String,
  //           transformation: Transformation): StreamingNode.type = {
  //    subSocket = context.socket(ZMQ.SUB)
  //    subSocket.connect("tcp://127.0.0.1:" + receivePort)
  //    for (topic <- receiveTopic) {
  //      subSocket.subscribe(topic.getBytes(ZMQ.CHARSET))
  //    }
  //
  //    publishSocket = context.socket(ZMQ.PUB)
  //    publishSocket.bind("tcp://127.0.0.1:" + publishPort)
  //
  //    this.publishTopic = publishTopic
  //
  //    this.transformation = transformation
  //    this
  //  }
  //
  //  def init(context: ZMQ.context,
  //           receivePort: String,
  //           publishPort: String,
  //           receiveTopic: Array[String],
  //           publishTopic: String,
  //           transformation: util.ArrayList[Transformation]): StreamingNode.type = {
  //    subSocket = context.socket(ZMQ.SUB)
  //    subSocket.connect("tcp://127.0.0.1:" + receivePort)
  //    for (topic <- receiveTopic) {
  //      subSocket.subscribe(topic.getBytes(ZMQ.CHARSET))
  //    }
  //
  //    publishSocket = context.socket(ZMQ.PUB)
  //    publishSocket.bind("tcp://127.0.0.1:" + publishPort)
  //
  //    this.publishTopic = publishTopic
  //
  //    for (transform <- transformation) {
  //      this.transformation.add(transform)
  //    }
  //    this
  //  }

  def receive(): Unit = {
    println("StreamingNode receive method called")

    val topic = subSocket.recvStr() //measurements or passBack
    //val messageObject = new JSONObject(subSocket.recvStr()) //JSON formatted Message {"topic":[topic]"key":[key],"value":[message]}

    //    val messageObject = (Json.parse(subSocket.recvStr()) \ "key_value").as[Map[String, String]]

    val parser = new JSONParser()
    val receivedString = subSocket.recvStr()
    println("received string: " + receivedString)
    receivedString match {
      case _: String => {
        val jSONObject = parser.parse(receivedString).asInstanceOf[JMap[String, String]]

        //    val jSONObject = parser.parse(subSocket.recvStr()).asInstanceOf[JMap[String, String]]

        println("received on topic: " + topic)
        println("received data: " + jSONObject)

        //    publish(Map(
        //      "topic" -> publishTopic,
        //      "key" -> messageObject("key"),
        //      "value" -> BaseSensorMeasurement.toJson(this.transformation.apply(messageObject("value").asInstanceOf[List[BaseMeasurement]]))
        //    ))

        val map = new JLinkedHashMap[String, String]()
        map.put("topic", publishTopic)
        map.put("key", jSONObject.get("key"))
        map.put("value", jSONObject.get("value"))
        //    BaseSensorMeasurement.jsonToSensorMeasurement(jSONObject.get("value")) match {
        //      case Some(sensorMeasurement) => insert(sensorMeasurement, Configs.measDBLite)
        //      case _ => logger.error("Bad json format!")
        //    }
        //    map.put("value", BaseSensorMeasurement.toJson(this.transformation.apply(jSONObject.get("value").asInstanceOf[List[BaseMeasurement]])))

        //    val message: String = JSONObject.toJSONString(map)
        publish(map)
        println("published map: " + map + "\n")
      }
      case _ => println("receive string is null. \n")
    }
  }

  //  "value" -> Json.stringify(Json.toJson(this.transformation.apply(BaseSensorMeasurement.jsonToSensorMeasurement(messageObject("value")), Map)))))

  def publish(processedMap: JMap[String, String]): Unit = {
    //val processedMessage: Message = epidataLiteStreamingContext(ZMQInit.streamQueue.dequeue)
    println("Streamingnode publish method called")

    publishSocket.sendMore(this.publishTopic)

    println("publish topic: " + this.publishTopic)

    //val msg: String = Json.stringify(Json.toJson(processedMessage))
    //JSON.format(processedMessage)

    val msg: String = JSONObject.toJSONString(processedMap)

    publishSocket.send(msg.getBytes(), 0)
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
