/*
* Copyright (c) 2020 EpiData, Inc.
*/
package com.epidata.spark

import java.util

import org.json.simple.{ JSONArray, JSONObject }
import org.json.simple.parser.{ JSONParser, ParseException }
import java.util.{ LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, Map => JMap }
import com.epidata.lib.models.SensorMeasurement

import scala.collection.mutable._
import scala.collection.mutable.ArrayBuffer
import com.epidata.spark.ops.Transformation
import play.api.libs.json._
import play.api.libs.json.Reads._
import org.zeromq.ZMQ
import com.epidata.lib.models.{ AutomatedTest => BaseAutomatedTest, Measurement => BaseMeasurement, SensorMeasurement => BaseSensorMeasurement }

class StreamingNode {
  var subSocket: ZMQ.Socket = _ //add as parameter
  var publishSocket: ZMQ.Socket = _ //add as parameter

  var subscribePorts: List[String] = _ //allows Node to subscribe to various ports
  var subscribeTopics: List[String] = _ //allows Node to listen on various topics

  var publishPort: String = _
  var publishTopic: String = _

  var transformation: Transformation = _

  //NEW
  /*
  Creating array of Queues (here on out referenced to as a Buffer) that hold type BaseMeasurements.
  Size of array is determined by number of Topics.
   */
  var streamBuffers: Array[Queue[String]] = _
  /*
  Each Buffer can have a unique buffer size.
  For dev purposes all Node Buffers will have default size of 2.
   */
  var bufferSizes: List[Int] = _
  //NEW

  //temp
  var TempID = 0

  def init(
    context: ZMQ.Context,
    //NEW
    receivePorts: List[String], //allows Node to subscribe to various ports
    receiveTopics: List[String], //allows Node to listen on various topics
    bufferSizes: List[Int],
    //NEW
    sendPort: String,
    sendTopic: String,
    receiveTimeout: Integer,
    transformation: Transformation): StreamingNode = {

    /*
    Copying parameters to instance variables.
     */
    subscribePorts = receivePorts
    subscribeTopics = receiveTopics

    publishPort = sendPort
    publishTopic = sendTopic

    subSocket = context.socket(ZMQ.SUB)

    //NEW
    /*
    Iterating through all port numbers and binding subscribe socket.
     */
    for (port <- subscribePorts) { subSocket.connect("tcp://127.0.0.1:" + port) }
    /*
    Iterating through all topics and adding subscribe socket.
     */
    for (topic <- subscribeTopics) { subSocket.subscribe(topic.getBytes(ZMQ.CHARSET)) }
    //NEW

    publishSocket = context.socket(ZMQ.PUB)
    publishSocket.bind("tcp://127.0.0.1:" + publishPort)

    this.transformation = transformation

    //NEW
    /*
    Iterating through bufferSizes to
     */
    var i = 0
    for (i <- 0 until bufferSizes.length) { streamBuffers(i) = new Queue[String] }
    //NEW

    this
  }

  def receive(): List[String] = {
    TempID += 1
    println("Receiving from:")
    for (port <- subscribePorts) {
      println("port: " + port)
    }
    println("---------------:")

    val topic = subSocket.recvStr()
    println("receive topic: " + topic)
    val parser = new JSONParser()
    val receivedString = subSocket.recvStr()
    println("received message: " + receivedString + "\n")
    receivedString match {
      case _: String => {
        val jSONObject = parser.parse(receivedString).asInstanceOf[JMap[String, String]]

        //        val map = new JLinkedHashMap[String, String]()
        //        map.put("topic", publishTopic)
        //        map.put("key", jSONObject.get("key"))
        //        map.put("value", jSONObject.get("value"))

        //NEW
        val index = subscribeTopics.indexOf(topic) //getting index of corresponding Buffer in streamBuffers
        streamBuffers(index).enqueue(jSONObject.get("value")) //adding current message value to buffer
        if (streamBuffers(index).size == bufferSizes(index)) { //checking to see if size of Buffer reached max
          val list = streamBuffers(index).toList //if desired buffer size is achieved -> convert Queued measurements to list
          streamBuffers(index).clear() //clear buffer
          return list //transform list of measurements

        }
        //NEW
        List("SKIP")
      }
      case _ =>
        println("receive string is null. \n")
        List("SKIP")
    }
  }

  //NEW
  def transform(map: List[String]): ListBuffer[JLinkedHashMap[String, String]] = {
    println("Performing Transformation on: ")
    for (measurement <- map) {
      println("Meas: " + measurement)
    }
    //val transformResults = transformation.apply(map) //temp remove and pass through JSON value as identity transformation

    var resultsAsMap = new ListBuffer[JLinkedHashMap[String, String]]

    for (result <- map /*transformResults*/ ) {
      val transformationMap = new JLinkedHashMap[String, String]()
      transformationMap.put("topic", publishTopic)
      transformationMap.put("key", TempID.toString /*create key method from Play*/ )
      transformationMap.put("value", result)

      resultsAsMap += transformationMap
    }

    resultsAsMap
    //iterate through list of basemeasuremnt to make key value pairs
  }
  //NEW

  def publish(processedMapList: ListBuffer[JLinkedHashMap[String, String]]): Unit = {
    //val processedMessage: Message = epidataLiteStreamingContext(ZMQInit.streamQueue.dequeue)
    //println("Streamingnode publish method called")

    for (map <- processedMapList) {
      publishSocket.sendMore(this.publishTopic)
      val msg: String = JSONObject.toJSONString(map)
      publishSocket.send(msg.getBytes(), 0)

      println("publish topic: " + this.publishTopic + ", publish port: " + publishPort)
      println("published message: " + msg + "\n")
    }
  }

  def clear(): Unit = {
    try {
      for (topic <- subscribeTopics) {
        subSocket.unsubscribe(topic.getBytes(ZMQ.CHARSET))
      }
      for (port <- subscribePorts) {
        subSocket.unbind("tcp://127.0.0.1:" + port)
      }
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
