package com.epidata.spark

import java.util

import org.json.simple.{ JSONArray, JSONObject }
import org.json.simple.parser.{ JSONParser, ParseException }
import java.util.{ LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, Map => JMap }
import scala.collection.mutable.Stack

import com.epidata.spark.ops.Transformation
import play.api.libs.json._
import play.api.libs.json.Reads._
import org.zeromq.ZMQ
import com.epidata.lib.models.{ AutomatedTest => BaseAutomatedTest, Measurement => BaseMeasurement, SensorMeasurement => BaseSensorMeasurement }

import scala.io.StdIn

class StreamingGateway {
  var subSocket: ZMQ.Socket = _ //add as parameter
  var publishSocket: ZMQ.Socket = _ //add as parameter

  var subscribePort: List[String] = _ //now a List
  var publishPort: String = _

  var subscribeTopic: List[String] = _ //now a List
  var publishTopic: String = _
  var transformation: Transformation = _

  def init(
    context: ZMQ.Context,
    receivePort: List[String], //now a List
    sendPort: String,
    receiveTopic: List[String], //now a List
    sendTopic: String,
    receiveTimeout: Integer,
    transformation: Transformation): StreamingGateway = {

    subscribePort = receivePort
    publishPort = sendPort

    subscribeTopic = receiveTopic
    publishTopic = sendTopic

    //println("StreamingNode init called.")
    //println("subscribePort: " + subscribePort + ", publishPort: " + publishPort)
    //println("subscribeTopic: " + subscribeTopic + ", publishTopic: " + publishTopic)

    subSocket = context.socket(ZMQ.SUB)
    //subSocket.setReceiveTimeOut(receiveTimeout)
    for (port <- subscribePort) {
      subSocket.connect("tcp://127.0.0.1:" + port)
    }
    for (topic <- subscribeTopic) {
      subSocket.subscribe(topic.getBytes(ZMQ.CHARSET))
    }

    publishSocket = context.socket(ZMQ.PUB)
    publishSocket.bind("tcp://127.0.0.1:" + publishPort)

    this.transformation = transformation

    //println("Streaming Node initiated. Subscribe topic: " + subscribeTopic + ", publish topic: " + publishTopic)

    this
  }

}
