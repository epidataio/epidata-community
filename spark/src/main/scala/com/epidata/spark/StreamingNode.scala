/*
* Copyright (c) 2020 EpiData, Inc.
*/
package com.epidata.spark

import java.security.MessageDigest
import java.util

import org.json.simple.{ JSONArray, JSONObject }
import org.json.simple.parser.JSONParser
import org.zeromq.ZMQ

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue
import java.util.{ LinkedHashMap => JLinkedHashMap, List => JList, Map => JMap }

import com.epidata.lib.models.util.JsonHelpers._
import com.epidata.lib.models.util.Message
import com.epidata.spark.ops.Transformation
import com.epidata.lib.models.{ AutomatedTest => BaseAutomatedTest, AutomatedTestCleansed => BaseAutomatedTestCleansed, AutomatedTestSummary => BaseAutomatedTestSummary, Measurement => BaseMeasurement, MeasurementCleansed => BaseMeasurementCleansed, MeasurementSummary => BaseMeasurementSummary, SensorMeasurement => BaseSensorMeasurement, SensorMeasurementCleansed => BaseSensorMeasurementCleansed, SensorMeasurementSummary => BaseSensorMeasurementSummary }
import com.typesafe.config.ConfigFactory

import scala.io.StdIn

class StreamingNode {
  var subSocket: ZMQ.Socket = _ //add as parameter
  var publishSocket: ZMQ.Socket = _ //add as parameter

  var subscribePorts: ListBuffer[String] = _ //allows Node to subscribe to various ports
  var subscribeTopics: ListBuffer[String] = _ //allows Node to listen on various topics

  var publishPort: String = _
  var publishTopic: String = _

  var transformation: Transformation = _

  var streamBuffers: Array[Queue[String]] = _
  var bufferSizes: ListBuffer[Integer] = _

  var outputBuffer: Queue[String] = _

  private val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  val measurementClass: String = conf.getString("spark.epidata.measurementClass")

  def init(
    context: ZMQ.Context,

    receivePorts: ListBuffer[String], //allows Node to subscribe to various ports
    receiveTopics: ListBuffer[String], //allows Node to listen on various topics
    bufferSizes: ListBuffer[Integer],

    sendPort: String,
    sendTopic: String,
    receiveTimeout: Integer,
    transformation: Transformation): StreamingNode = {
    if (receivePorts.length > bufferSizes.length) { //more ports than buffer sizes make a pattern
      val pattern = new ListBuffer[Integer]()
      pattern.appendAll(bufferSizes)
      while (receivePorts.length > bufferSizes.length) {
        bufferSizes.appendAll(pattern) //append pattern until buffersizes is same size or greater than recieve ports
      }
    }

    if (receivePorts.length < bufferSizes.length) { //more buffer sizes than ports so some will be ignored
      val difference = bufferSizes.length - receivePorts.length
      bufferSizes.remove((bufferSizes.length - difference), difference) //append pattern until buffersizes is same size or greater than recieve ports
    }

    //println("Sizes: " + receivePorts.length + ", " + bufferSizes.length)

    subscribePorts = receivePorts
    subscribeTopics = receiveTopics

    publishPort = sendPort
    publishTopic = sendTopic

    subSocket = context.socket(ZMQ.SUB)

    for (port <- subscribePorts) { subSocket.connect("tcp://127.0.0.1:" + port) }
    for (topic <- subscribeTopics) { subSocket.subscribe(topic.getBytes(ZMQ.CHARSET)) }

    publishSocket = context.socket(ZMQ.PUB)
    println("LINE 84: " + publishPort)
    publishSocket.bind("tcp://127.0.0.1:" + publishPort)

    this.transformation = transformation

    streamBuffers = new Array[Queue[String]](bufferSizes.length)

    for (i <- 0 until bufferSizes.length) { streamBuffers(i) = new Queue[String]() }

    outputBuffer = new Queue[String]

    this.bufferSizes = bufferSizes

    println("\n\nStreamNode Configs----------------------------------------------")
    println("Sub Topic: " + subscribeTopics)
    println("Sub Port: " + subscribePorts)
    println("Pub Topic: " + publishTopic)
    println("Pub Port: " + publishPort)
    println("Stream Buffers: " + streamBuffers)
    println("Stream Buffer Sizes: " + bufferSizes)
    println("StreamNode Configs----------------------------------------------")
    this
  }

  def receive() = {
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
        val measurement: String = jsonToMessage(receivedString).value

        val index = subscribeTopics.indexOf(topic) //getting index of corresponding Buffer in streamBuffers
        //println("index: " + index + " in range " + subscribeTopics.length)
        //print("Obj or Null: " + streamBuffers(index))
        streamBuffers(index).enqueue(measurement) //adding current message value to buffer

        if (streamBuffers(index).size >= bufferSizes(index)) { //checking to see if size of Buffer reached max
          printf("\n\n DEQUEUING WHEN (" + index + ") BUFFER SIZE IS: " + streamBuffers(index).size + "\n\n")
          val list = streamBuffers(index).toList //if desired buffer size is achieved -> convert Queued measurements to list
          streamBuffers(index).clear() //clear buffer
          transform(list)
        }
      }
      case _ =>
        throw new IllegalArgumentException("Message recieved is null. Expected String type")
    }

  }

  def transform(list: List[String]): Unit = {
    println("\n\nTransforming----------------------------------------------")
    println("Performing Transformation on-----------------------------------------------------------: ")

    for (measurement <- list) {
      println("$$$$Meas: " + measurement + "\n")
    }

    import scala.collection.JavaConversions._

    val measList = new ListBuffer[JLinkedHashMap[String, Object]]()
    for (json <- list) {
      measList += jsonToMap(json)
    }
    println("measurement list: " + measList + "\n")

    println("transformation type: " + transformation + "\n")

    val resultsList = transformation.apply(measList)
    println("result List: " + resultsList + "\n")

    for (result <- resultsList) {
      val key = keyForSensorMeasurement(result)
      val value = mapToJson(result)
      val message: String = messageToJson(Message(key, value))
      outputBuffer.enqueue(message)
    }
    // println("outputBuffer: " + outputBuffer + "\n")

  }

  def publish( /*processedMapList: ListBuffer[JLinkedHashMap[String, String]]*/ ): Unit = {
    //val processedMessage: Message = epidataLiteStreamingContext(ZMQInit.streamQueue.dequeue)
    //println("Streamingnode publish method called")
    println("\n\nPublishing---------------------------------------------- $$$ " + !outputBuffer.isEmpty)

    while (!outputBuffer.isEmpty) {
      val msg = outputBuffer.dequeue()
      println("published message: " + msg + "\n")
      publishSocket.sendMore(this.publishTopic)
      publishSocket.send(msg.getBytes(), 0)
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

}
