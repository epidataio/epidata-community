/*
* Copyright (c) 2020 EpiData, Inc.
*/
package com.epidata.spark

import java.util

import org.json.simple.JSONObject
import org.json.simple.parser.JSONParser
import java.util.{LinkedHashMap => JLinkedHashMap, Map => JMap}

import scala.collection.mutable._
import com.epidata.spark.ops.Transformation
import org.zeromq.ZMQ

import scala.collection.mutable

class StreamingNode {
  var subSocket: ZMQ.Socket = _ //add as parameter
  var publishSocket: ZMQ.Socket = _ //add as parameter

  var subscribePorts: List[String] = _ //allows Node to subscribe to various ports
  var subscribeTopics: List[String] = _ //allows Node to listen on various topics

  var publishPort: String = _
  var publishTopic: String = _

  var transformation: Transformation = _

  var streamBuffers: Array[Queue[String]] = _
  var bufferSizes: List[Int] = _

  var TempID = 0

  def init(
    context: ZMQ.Context,

    receivePorts: List[String], //allows Node to subscribe to various ports
    receiveTopics: List[String], //allows Node to listen on various topics
    bufferSizes: List[Int],

    sendPort: String,
    sendTopic: String,
    receiveTimeout: Integer,
    transformation: Transformation): StreamingNode = {

    subscribePorts = receivePorts
    subscribeTopics = receiveTopics

    publishPort = sendPort
    publishTopic = sendTopic

    subSocket = context.socket(ZMQ.SUB)

    for (port <- subscribePorts) { subSocket.connect("tcp://127.0.0.1:" + port) }
    for (topic <- subscribeTopics) { subSocket.subscribe(topic.getBytes(ZMQ.CHARSET)) }

    publishSocket = context.socket(ZMQ.PUB)
    publishSocket.bind("tcp://127.0.0.1:" + publishPort)

    this.transformation = transformation

    streamBuffers = new Array[Queue[String]](bufferSizes.length)

    for (i <- 0 until bufferSizes.length) { streamBuffers(i) = new Queue[String] }

    this.bufferSizes = bufferSizes

    this
  }

  def receive(): List[String] = {
    println("\n\nReceiving----------------------------------------------")
    println("\nReceiving from--------------:")
    for (port <- subscribePorts) {
      println("port: " + port)
    }
    println("-----------------------:")

    val topic = subSocket.recvStr()
    println("receive topic: " + topic + "----------------------------------------------------------------------------------")
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

        val index = subscribeTopics.indexOf(topic) //getting index of corresponding Buffer in streamBuffers
        print("index: " + index + " in range " + subscribeTopics.length)
        //print("Obj or Null: " + streamBuffers(index))
        streamBuffers(index).enqueue(jSONObject.get("value")) //adding current message value to buffer
        if (streamBuffers(index).size == bufferSizes(index)) { //checking to see if size of Buffer reached max
          printf("\n\n DEQUEUING WHEN (" + index + ") BUFFER SIZE IS: " + streamBuffers(index).size + "\n\n")
          val list = streamBuffers(index).toList //if desired buffer size is achieved -> convert Queued measurements to list
          streamBuffers(index).clear() //clear buffer
          return list //transform list of measurements

        }
        List("SKIP") //telling streaming context to skip transformation/publish step until stack is filled
      }
      case _ =>
        println("receive string is null. \n")
        List("SKIP")
    }
  }

  def transform(map: List[String]): ListBuffer[JLinkedHashMap[String, String]] = {
    println("\n\nTransforming----------------------------------------------")
    println("Performing Transformation on-----------------------------------------------------------: ")
    for (measurement <- map) {
      println("$$$$Meas: " + measurement + "\n")
    }
    val transformResults = transformation.apply(map)

    var resultsAsMap = new ListBuffer[JLinkedHashMap[String, String]]

    for (result <- transformResults) {
      val transformationMap = new JLinkedHashMap[String, String]()
      transformationMap.put("topic", publishTopic)
      transformationMap.put("key", TempID.toString /*create key method from Play*/ )
      transformationMap.put("value", result)

      resultsAsMap += transformationMap
    }

    resultsAsMap
    //iterate through list of basemeasuremnt to make key value pairs
  }

  def publish(processedMapList: ListBuffer[JLinkedHashMap[String, String]]): Unit = {
    //val processedMessage: Message = epidataLiteStreamingContext(ZMQInit.streamQueue.dequeue)
    //println("Streamingnode publish method called")
    println("\n\nPublishing----------------------------------------------")

    for (map <- processedMapList) {
      TempID += 1
      publishSocket.sendMore(this.publishTopic)
      val msg: String = JSONObject.toJSONString(map)
      publishSocket.send(msg.getBytes(), 0)

      println("\npublish topic: " + this.publishTopic + ", publish port: " + publishPort)
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
