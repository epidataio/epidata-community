/*
* Copyright (c) 2020 EpiData, Inc.
*/
package com.epidata.spark

import java.util

import org.json.simple.{ JSONObject, JSONArray }
import org.json.simple.parser.JSONParser
import java.util.{ LinkedHashMap => JLinkedHashMap, Map => JMap, List => JList }
import com.epidata.lib.models.util.JsonHelpers._
import com.epidata.lib.models.util.Message
import com.epidata.lib.models.{
  Measurement => BaseMeasurement,
  MeasurementCleansed => BaseMeasurementCleansed,
  MeasurementSummary => BaseMeasurementSummary,
  SensorMeasurement => BaseSensorMeasurement,
  SensorMeasurementCleansed => BaseSensorMeasurementCleansed,
  SensorMeasurementSummary => BaseSensorMeasurementSummary,
  AutomatedTest => BaseAutomatedTest,
  AutomatedTestCleansed => BaseAutomatedTestCleansed,
  AutomatedTestSummary => BaseAutomatedTestSummary
}
import scala.collection.mutable._
import com.epidata.spark.ops.Transformation
import org.zeromq.ZMQ
import com.typesafe.config.{ Config, ConfigFactory }
import java.security.MessageDigest

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

  var outputBuffer: Queue[String] = _

  private val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  val measurementClass: String = conf.getString("spark.epidata.measurementClass")

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

    for (i <- 0 until bufferSizes.length) { streamBuffers(i) = new Queue[String]() }

    this.bufferSizes = bufferSizes

    outputBuffer = new Queue[String]()

    this
  }

  def getMd5(inputStr: String): String = {
    val md: MessageDigest = MessageDigest.getInstance("MD5")
    md.digest(inputStr.getBytes()).map(0xFF & _).map { "%02x".format(_) }.foldLeft("") { _ + _ }
  }

  private def keyForAutomatedTest(measurement: JLinkedHashMap[String, Object]): String = {
    val key =
      s"""
             |${measurement.get("company")}${"_"}
             |${measurement.get("site")}${"_"}
             |${measurement.get("device_group")}${"_"}
             |${measurement.get("tester")}${"_"}
             |${measurement.get("ts")}
             |${measurement.get("device_name")}
             |${measurement.get("test_name")}
             |${measurement.get("meas_name")}
           """.stripMargin
    getMd5(key)
  }

  private def keyForSensorMeasurement(measurement: JLinkedHashMap[String, Object]): String = {
    val key =
      s"""
             |${measurement.get("company")}${"_"}
             |${measurement.get("site")}${"_"}
             |${measurement.get("station")}${"_"}
             |${measurement.get("sensor")}${"_"}
             |${measurement.get("ts")}
             |${measurement.getOrDefault("event", "")}
             |${measurement.get("meas_name")}
           """.stripMargin
    getMd5(key)
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
        // val jSONObject = parser.parse(receivedString).asInstanceOf[JMap[String, String]]

        val measurement: String = jsonToMessage(receivedString).value

        val index = subscribeTopics.indexOf(topic) //getting index of corresponding Buffer in streamBuffers
        print("index: " + index + " in range " + subscribeTopics.length)
        //print("Obj or Null: " + streamBuffers(index))
        //        streamBuffers(index).enqueue(jSONObject.get("value")) //adding current message value to buffer
        streamBuffers(index).enqueue(measurement) //adding current message value to buffer
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

  def transform(list: List[String]): Unit = {
    println("\n\nTransforming----------------------------------------------")
    println("Performing Transformation on-----------------------------------------------------------: ")
    for (measurement <- list) {
      println("$$$$Meas: " + measurement + "\n")
    }

    /*    val transformResults = transformation.apply(map)

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
*/

    import scala.collection.JavaConversions._

    measurementClass match {
      case com.epidata.lib.models.AutomatedTest.NAME => {
        val measList = new ListBuffer[JLinkedHashMap[String, Object]]()
        for (json <- list) {
          measList += BaseAutomatedTest.jsonToJLinkedHashMap(json)
        }
        println("measurement list: " + measList + "\n")

        println("transformation type: " + transformation + "\n")

        import scala.collection.JavaConversions._

        val resultsList = transformation.apply(measList)
        println("result list: " + resultsList + "\n")

        for (result <- resultsList) {
          val key = keyForAutomatedTest(result)
          println("key: " + key + "\n")
          val value = JSONObject.toJSONString(result)
          println("value: " + value + "\n")
          val message: String = messageToJson(Message(key, value))
          println("message: " + message + "\n")
          outputBuffer.enqueue(message)
        }
        println("outputBuffer: " + outputBuffer + "\n")
      }

      case com.epidata.lib.models.SensorMeasurement.NAME => {
        val measList = new ListBuffer[JLinkedHashMap[String, Object]]()
        for (json <- list) {
          val temp = BaseSensorMeasurement.jsonToJLinkedHashMap(json)
          measList += temp
        }
        println("measurement list: " + measList + "\n")

        println("transformation type: " + transformation + "\n")

        val resultsList = transformation.apply(measList)
        println("result List: " + resultsList + "\n")

        for (result <- resultsList) {
          val key = keyForSensorMeasurement(result)
          val value = JSONObject.toJSONString(result)
          val message: String = messageToJson(Message(key, value))
          outputBuffer.enqueue(message)
        }
        println("outputBuffer: " + outputBuffer + "\n")
      }
    }

  }

  //  def publish(processedMapList: ListBuffer[JLinkedHashMap[String, String]]): Unit = {
  def publish(): Unit = {
    //val processedMessage: Message = epidataLiteStreamingContext(ZMQInit.streamQueue.dequeue)
    //println("Streamingnode publish method called")
    println("\n\nPublishing----------------------------------------------")

    val processedMessages: List[String] = outputBuffer.dequeueAll(_ => true).toList
    //    for (map <- processedMapList) {
    for (message <- processedMessages) {
      TempID += 1
      publishSocket.sendMore(this.publishTopic)
      //      val msg: String = JSONObject.toJSONString(map)
      publishSocket.send(message.getBytes(), 0)

      println("\npublish topic: " + this.publishTopic + ", publish port: " + publishPort)
      println("published message: " + message + "\n")
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
