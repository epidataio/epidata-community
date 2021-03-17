/*
* Copyright (c) 2020 EpiData, Inc.
*/
package com.epidata.spark

import com.epidata.spark.ops.Transformation
import play.api.libs.json._
import play.api.libs.json.Reads._
import org.zeromq.ZMQ
import com.epidata.lib.models.{ Measurement => BaseMeasurement, SensorMeasurement => BaseSensorMeasurement, AutomatedTest => BaseAutomatedTest }

object StreamingNode {
  var subSocket: ZMQ.Socket = _ //add as parameter
  var forwardSocket: ZMQ.Socket = _ //add as parameter
  var publishTopic: String = _
  var transformation: Transformation = _

  def init(
    context: ZMQ.Context,
    receivePort: String,
    publishPort: String,
    receiveTopic: String,
    publishTopic: String,
    transformation: Transformation): StreamingNode.type = {
    subSocket = context.socket(ZMQ.SUB)
    subSocket.connect("tcp://127.0.0.1:" + receivePort)
    subSocket.subscribe(receiveTopic.getBytes(ZMQ.CHARSET))

    forwardSocket = context.socket(ZMQ.PUB)
    forwardSocket.bind("tcp://127.0.0.1:" + publishPort)

    this.publishTopic = publishTopic

    this.transformation = transformation

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
  //    forwardSocket = context.socket(ZMQ.PUB)
  //    forwardSocket.bind("tcp://127.0.0.1:" + publishPort)
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
  //    forwardSocket = context.socket(ZMQ.PUB)
  //    forwardSocket.bind("tcp://127.0.0.1:" + publishPort)
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
  //    forwardSocket = context.socket(ZMQ.PUB)
  //    forwardSocket.bind("tcp://127.0.0.1:" + publishPort)
  //
  //    this.publishTopic = publishTopic
  //
  //    for (transform <- transformation) {
  //      this.transformation.add(transform)
  //    }
  //    this
  //  }

  def receive(): Unit = {
    val topic = subSocket.recvStr() //measurements or passBack
    //val messageObject = new JSONObject(subSocket.recvStr()) //JSON formatted Message {"topic":[topic]"key":[key],"value":[message]}
    val messageObject = (Json.parse(subSocket.recvStr()) \ "key_value").as[Map[String, String]]
    publish(Map(
      "topic" -> publishTopic,
      "key" -> messageObject("key"),
      "value" -> BaseSensorMeasurement.toJson(this.transformation.apply(messageObject("value").asInstanceOf[List[BaseMeasurement]]))))
  }

  //  "value" -> Json.stringify(Json.toJson(this.transformation.apply(BaseSensorMeasurement.jsonToSensorMeasurement(messageObject("value")), Map)))))

  def publish(processedMessage: Map[String, String]): Unit = {
    //val processedMessage: Message = epidataLiteStreamingContext(ZMQInit.streamQueue.dequeue)
    forwardSocket.sendMore(this.publishTopic)
    val msg: String = Json.stringify(Json.toJson(processedMessage))
    //JSON.format(processedMessage)
    forwardSocket.send(msg.getBytes(), 0)
  }

}
