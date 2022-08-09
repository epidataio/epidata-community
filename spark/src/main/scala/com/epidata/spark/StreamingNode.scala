/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.spark

import java.security.MessageDigest
import java.util

import org.json.simple.{ JSONArray, JSONObject }
import org.json.simple.parser.JSONParser
import org.zeromq.ZMQ
import org.zeromq.ZMQException

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue
import java.util.{ LinkedHashMap => JLinkedHashMap, List => JList, Map => JMap }

import com.epidata.lib.models.util.JsonHelpers._
import com.epidata.lib.models.util.Message
import com.epidata.spark.ops.Transformation
import com.epidata.lib.models.{ AutomatedTest => BaseAutomatedTest, AutomatedTestCleansed => BaseAutomatedTestCleansed, AutomatedTestSummary => BaseAutomatedTestSummary, Measurement => BaseMeasurement, MeasurementCleansed => BaseMeasurementCleansed, MeasurementSummary => BaseMeasurementSummary, SensorMeasurement => BaseSensorMeasurement, SensorMeasurementCleansed => BaseSensorMeasurementCleansed, SensorMeasurementSummary => BaseSensorMeasurementSummary }
import com.typesafe.config.ConfigFactory
import scala.io.StdIn
import scala.util.control.Breaks._
import java.util.logging._

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

  var logger: Logger = _

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
    transformation: Transformation,
    logger: Logger): StreamingNode = {
    this.logger = logger
    if (receivePorts.length > bufferSizes.length) { //more ports than buffer sizes make a pattern
      val pattern = new ListBuffer[Integer]()
      pattern.appendAll(bufferSizes)
      while (receivePorts.length > bufferSizes.length) {
        bufferSizes.appendAll(pattern) //append pattern until buffersizes is same size or greater than receive ports
      }
    }

    if (receivePorts.length < bufferSizes.length) { //more buffer sizes than ports so some will be ignored
      val difference = bufferSizes.length - receivePorts.length
      bufferSizes.remove((bufferSizes.length - difference), difference) //append pattern until buffersizes is same size or greater than receive ports
    }

    //println("Sizes: " + receivePorts.length + ", " + bufferSizes.length)

    subscribePorts = receivePorts
    subscribeTopics = receiveTopics

    publishPort = sendPort
    publishTopic = sendTopic

    println("subscribePorts: " + subscribePorts + ", subscribeTopics: " + subscribeTopics + ", publishPort: " + publishPort + ", publishTopic: " + publishTopic)

    subSocket = context.socket(ZMQ.SUB)
    subSocket.setLinger(10)
    logger.log(Level.INFO, "subSocket created")

    for (port <- subscribePorts) { subSocket.connect("tcp://127.0.0.1:" + port) }
    for (topic <- subscribeTopics) { subSocket.subscribe(topic.getBytes(ZMQ.CHARSET)) }

    publishSocket = context.socket(ZMQ.PUB)
    publishSocket.setLinger(0)
    logger.log(Level.INFO, "pubSocket created")

    // println("LINE 84: " + publishPort)
    publishSocket.bind("tcp://127.0.0.1:" + publishPort)
    logger.log(Level.INFO, "pubSocket bound")

    this.transformation = transformation

    streamBuffers = new Array[Queue[String]](bufferSizes.length)

    for (i <- 0 until bufferSizes.length) { streamBuffers(i) = new Queue[String]() }

    outputBuffer = new Queue[String]

    this.bufferSizes = bufferSizes

    println("\nStreamNode Configs----------------------------------------------")
    println("Sub Topic: " + subscribeTopics)
    println("Sub Port: " + subscribePorts)
    println("Pub Topic: " + publishTopic)
    println("Pub Port: " + publishPort)
    println("Stream Buffers: " + streamBuffers)
    println("Stream Buffer Sizes: " + bufferSizes)
    println("StreamNode Configs----------------------------------------------")

    logger.log(Level.INFO, "node init completed")
    this
  }

  def initSub(
    context: ZMQ.Context,

    receivePorts: ListBuffer[String], //allows Node to subscribe to various ports
    receiveTopics: ListBuffer[String], //allows Node to listen on various topics
    bufferSizes: ListBuffer[Integer],

    receiveTimeout: Integer,
    transformation: Transformation,
    logger: Logger): StreamingNode = {
    this.logger = logger
    if (receivePorts.length > bufferSizes.length) { //more ports than buffer sizes make a pattern
      val pattern = new ListBuffer[Integer]()
      pattern.appendAll(bufferSizes)
      while (receivePorts.length > bufferSizes.length) {
        bufferSizes.appendAll(pattern) //append pattern until buffersizes is same size or greater than receive ports
      }
    }

    if (receivePorts.length < bufferSizes.length) { //more buffer sizes than ports so some will be ignored
      val difference = bufferSizes.length - receivePorts.length
      bufferSizes.remove((bufferSizes.length - difference), difference) //append pattern until buffersizes is same size or greater than receive ports
    }

    //println("Sizes: " + receivePorts.length + ", " + bufferSizes.length)

    subscribePorts = receivePorts
    subscribeTopics = receiveTopics

    println("subscribePorts: " + subscribePorts + ", subscribeTopics: " + subscribeTopics)

    subSocket = context.socket(ZMQ.SUB)
    subSocket.setLinger(1)
    logger.log(Level.INFO, "subSocket created")

    for (port <- subscribePorts) { subSocket.connect("tcp://127.0.0.1:" + port) }
    for (topic <- subscribeTopics) { subSocket.subscribe(topic.getBytes(ZMQ.CHARSET)) }

    this.transformation = transformation

    streamBuffers = new Array[Queue[String]](bufferSizes.length)

    for (i <- 0 until bufferSizes.length) { streamBuffers(i) = new Queue[String]() }

    this.bufferSizes = bufferSizes

    println("\nStreamNode Configs----------------------------------------------")
    println("Sub Topic: " + subscribeTopics)
    println("Sub Port: " + subscribePorts)
    println("Stream Buffers: " + streamBuffers)
    println("Stream Buffer Sizes: " + bufferSizes)
    println("StreamNode Configs----------------------------------------------")

    logger.log(Level.INFO, "node initSub completed")
    this
  }

  def initPub(
    context: ZMQ.Context,

    sendPort: String,
    sendTopic: String,
    logger: Logger): StreamingNode = {
    this.logger = logger

    publishPort = sendPort
    publishTopic = sendTopic
    logger.log(Level.INFO, "publishPort: " + publishPort + ", publishTopic: " + publishTopic)

    publishSocket = context.socket(ZMQ.PUB)
    publishSocket.setLinger(0)
    logger.log(Level.INFO, "pubSocket created")

    // println("LINE 84: " + publishPort)
    publishSocket.bind("tcp://127.0.0.1:" + publishPort)
    logger.log(Level.INFO, "pubSocket bound")

    outputBuffer = new Queue[String]

    println("\nStreamNode Configs----------------------------------------------")
    println("Pub Topic: " + publishTopic)
    println("Pub Port: " + publishPort)
    println("StreamNode Configs----------------------------------------------")

    logger.log(Level.INFO, "node initPub completed")
    this
  }

  def receive() = {
    logger.log(Level.INFO, "node receive called")
    println("\n\nReceiving----------------------------------------------")
    println("\nReceiving from--------------:")
    for (port <- subscribePorts) {
      println("port: " + port)
    }
    println("-----------------------:")

    var topic: String = ""
    val parser = new JSONParser()
    var receivedString: String = ""

    try {
      breakable {
        while (!Thread.currentThread().isInterrupted()) {
          logger.log(Level.INFO, "node recvStr being called for topic")
          topic = subSocket.recvStr(ZMQ.NOBLOCK)
          //Thread.sleep(1000)
          if ((topic == "") || (topic == null)) {
            logger.log(Level.INFO, "Node recvStr called. No topic received.")
            Thread.sleep(5000)
          } else {
            logger.log(Level.INFO, "Node recvStr called. Topic received: " + topic)
            break
          }
        }
      }

      if (!Thread.currentThread().isInterrupted()) {
        try {
          logger.log(Level.INFO, "node recvStr being called for message")
          receivedString = subSocket.recvStr(ZMQ.NOBLOCK)
          logger.log(Level.INFO, "message received: " + receivedString)
          //Thread.sleep(1000)
        } catch {
          case e: ZMQException if ZMQ.Error.ETERM == e.getErrorCode() => {
            logger.log(Level.INFO, "No message received so far.")
          }
          case e: Throwable => {
            logger.log(Level.WARNING, "Exception on StreamingNode line 239: " + e)
            Thread.currentThread.interrupt()
          }
        }
      }

      //      val receivedString = subSocket.recvStr(ZMQ.DONTBLOCK)
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

            publish()
          }
        }
        case e =>
          throw new IllegalArgumentException("Message received is null. Expected String type. " + e)
      }
    } catch {
      case e: ZMQException if ZMQ.Error.ETERM == e.getErrorCode() => {
        logger.log(Level.WARNING, "Processors interrupted via ZMQException")
        Thread.currentThread.interrupt()
      }
      case e: InterruptedException => {
        logger.log(Level.INFO, "Processor service interrupted via InterruptedException")
        Thread.currentThread.interrupt()
      }
      case e: Throwable => {
        logger.log(Level.WARNING, "Exception at StreamingNode line 276: " + e)
        Thread.currentThread.interrupt()
      }
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
    println("input measurement list: " + measList + "\n")
    println("transformation type: " + transformation + "\n")

    val resultsList = transformation.apply(measList)

    println("output measurement list (result): " + resultsList + "\n")

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
    println("\n\nPublishing---------------------------------------------- $$$ " + !outputBuffer.isEmpty + "\n")

    while (!outputBuffer.isEmpty) {
      val msg = outputBuffer.dequeue()
      logger.log(Level.INFO, "publised topic: " + this.publishTopic + ", published message: " + msg + "\n")
      publishSocket.sendMore(this.publishTopic)
      publishSocket.send(msg.getBytes(), 0)
    }
  }

  def clear(): Unit = {
    try {
      for (topic <- subscribeTopics) {
        subSocket.unsubscribe(topic.getBytes(ZMQ.CHARSET))
      }
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during subscriber unsubscribe. " + e.toString())
    }

    try {
      //      publishSocket.unbind("tcp://127.0.0.1:" + publishPort)
      publishSocket.unbind(publishSocket.getLastEndpoint())
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during publisher unbind. " + e.toString())
    }

    try {
      for (port <- subscribePorts) {
        //        subSocket.unbind("tcp://127.0.0.1:" + port)
        subSocket.unbind(subSocket.getLastEndpoint())
      }
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during subscriber unbind. " + e.toString())
    }

    try {
      //      publishSocket.send(ZMQ.STOP_MESSAGE, 0)
      publishSocket.close()
      println("publishSocket closed successfully: " + publishSocket)
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during publisher close. " + e.toString())
    }

    try {
      subSocket.close()
      logger.log(Level.INFO, "subSocket closed successfully: " + subSocket)
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during subscriber close. " + e.toString())
    }

  }

  def clearPub(): Unit = {
    logger.log(Level.INFO, "node clearPub called")

    try {
      //      publishSocket.unbind("tcp://127.0.0.1:" + publishPort)
      publishSocket.unbind(publishSocket.getLastEndpoint())
      logger.log(Level.INFO, "publishSocket unbinded: " + publishSocket)
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during publisher unbind. " + e.toString())
    }

    try {
      publishSocket.close()
      logger.log(Level.INFO, "publishSocket closed successfully: " + publishSocket)
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during publisher close. " + e.toString())
    }
  }

  def clearSub(): Unit = {
    logger.log(Level.INFO, "node clearSub called")
    try {
      for (topic <- subscribeTopics) {
        subSocket.unsubscribe(topic.getBytes(ZMQ.CHARSET))
        logger.log(Level.INFO, "subSocket unsubscribed: " + subSocket)
      }
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during subscriber unsubscribe. " + e.toString())
    }

    try {
      for (port <- subscribePorts) {
        //        subSocket.unbind("tcp://127.0.0.1:" + port)
        subSocket.disconnect(subSocket.getLastEndpoint())
        logger.log(Level.INFO, "subSocket unbound: " + subSocket)
      }
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during subscriber unbind. " + e.toString())
    }

    try {
      subSocket.close()
      logger.log(Level.INFO, "subSocket closed: " + subSocket)
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during subscriber close. " + e.toString())
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
