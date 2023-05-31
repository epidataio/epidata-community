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
import scala.collection.mutable.MutableList
import java.util.{ LinkedHashMap => JLinkedHashMap, List => JList, Map => JMap }

import com.epidata.lib.models.util.JsonHelpers._
import com.epidata.lib.models.util.Message
import com.epidata.spark.ops.Transformation
import com.epidata.lib.models.{ AutomatedTest => BaseAutomatedTest, AutomatedTestCleansed => BaseAutomatedTestCleansed, AutomatedTestSummary => BaseAutomatedTestSummary, Measurement => BaseMeasurement, MeasurementCleansed => BaseMeasurementCleansed, MeasurementSummary => BaseMeasurementSummary, SensorMeasurement => BaseSensorMeasurement, SensorMeasurementCleansed => BaseSensorMeasurementCleansed, SensorMeasurementSummary => BaseSensorMeasurementSummary }
import com.typesafe.config.ConfigFactory
import scala.io.StdIn
import scala.util.control.Breaks._
import java.util.logging._
import java.nio.file.Paths

class StreamingNode(logger: Logger) {
  println("Streaming Node class")
  var subSocket: ZMQ.Socket = _ //add as parameter
  var publishSocket: ZMQ.Socket = _ //add as parameter

  var subscribePorts: ListBuffer[String] = _ //allows Node to subscribe to various ports
  var subscribeTopics: ListBuffer[String] = _ //allows Node to listen on various topics

  var publishPort: String = _
  var publishTopic: String = _

  var transformation: Transformation = _

  // var inputBuffers: Array[Queue[String]] = _
  var inputBuffers: Array[MutableList[String]] = _
  var bufferSizes: ListBuffer[Integer] = _
  var bufferOverlapSizes: ListBuffer[Integer] = _

  // var outputBuffer: Queue[String] = _
  var outputBuffer: MutableList[String] = _

  var this.logger: Logger = logger

  private val basePath = scala.util.Properties.envOrElse("EPIDATA_HOME", "")
  if (basePath.equals("") || (basePath == null)) {
    throw new IllegalStateException("EPIDATA_HOME environment variable not set")
  } else
    println("base path:" + basePath)

  private val conf = ConfigFactory.parseFile(new java.io.File(Paths.get(basePath, "conf", "sqlite-defaults.conf").toString())).resolve()
  println("conf value: " + conf)

  val measurementClass: String = conf.getString("spark.epidata.measurementClass")
  // println("measurement class: " + measurementClass)

  def initSub(
    context: ZMQ.Context,
    receivePorts: ListBuffer[String], //allows Node to subscribe to various ports
    receiveTopics: ListBuffer[String], //allows Node to listen on various topics
    bufferSizes: ListBuffer[Integer],
    bufferOverlapSizes: ListBuffer[Integer],
    receiveTimeout: Integer,
    transformation: Transformation): StreamingNode = {
    if (receivePorts.length > bufferSizes.length) { //more ports than buffer sizes make a pattern
      val bufferPattern = new ListBuffer[Integer]()
      bufferPattern.appendAll(bufferSizes)
      while (receivePorts.length > bufferSizes.length) {
        bufferSizes.appendAll(bufferPattern) //append pattern until buffersizes is same size or greater than receive ports
      }
    }

    if (receivePorts.length < bufferSizes.length) { //more buffer sizes than ports so some will be ignored
      val difference = bufferSizes.length - receivePorts.length
      bufferSizes.remove((bufferSizes.length - difference), difference) //append pattern until buffersizes is same size or greater than receive ports
    }

    if (receivePorts.length > bufferOverlapSizes.length) { //more ports than buffer overlap sizes make a pattern
      val bufferOverlapPattern = new ListBuffer[Integer]()
      bufferOverlapPattern.appendAll(bufferOverlapSizes)
      while (receivePorts.length > bufferOverlapSizes.length) {
        bufferOverlapSizes.appendAll(bufferOverlapPattern) //append pattern until buffer overlap sizes is same size or greater than receive ports
      }
    }

    if (receivePorts.length < bufferOverlapSizes.length) { //more buffer overlap sizes than ports so some will be ignored
      val overlapDifference = bufferOverlapSizes.length - receivePorts.length
      bufferOverlapSizes.remove((bufferOverlapSizes.length - overlapDifference), overlapDifference) //append pattern until buffer overlap sizes is same size or greater than receive ports
    }

    // logger.log(Level.INFO, "Sizes: " + receivePorts.length + ", " + bufferSizes.length)

    subscribePorts = receivePorts
    subscribeTopics = receiveTopics

    // logger.log(Level.FINE, "subscribePorts: " + subscribePorts + ", subscribeTopics: " + subscribeTopics)

    subSocket = context.socket(ZMQ.SUB)
    subSocket.setLinger(1)
    logger.log(Level.FINE, "Stream processor subSocket created")

    for (port <- subscribePorts) { subSocket.connect("tcp://127.0.0.1:" + port) }
    for (topic <- subscribeTopics) { subSocket.subscribe(topic.getBytes(ZMQ.CHARSET)) }

    this.transformation = transformation

    // inputBuffers = new Array[Queue[String]](bufferSizes.length)
    inputBuffers = new Array[MutableList[String]](bufferSizes.length)

    for (i <- 0 until bufferSizes.length) { inputBuffers(i) = new MutableList[String]() }
    // for (i <- 0 until bufferSizes.length) { inputBuffers(i) = new Queue[String]() }
    logger.log(Level.FINE, "Stream processor input buffers created")

    this.bufferSizes = bufferSizes
    this.bufferOverlapSizes = bufferOverlapSizes

    // outputBuffer = new Queue[String]
    // println("created outputBuffer")

    logger.log(Level.FINE, "Stream processor initSub completed")
    this
  }

  def initPub(
    context: ZMQ.Context,

    sendPort: String,
    sendTopic: String): StreamingNode = {

    publishPort = sendPort
    publishTopic = sendTopic
    // logger.log(Level.INFO, "publishPort: " + publishPort + ", publishTopic: " + publishTopic)
    try {
      publishSocket = context.socket(ZMQ.PUB)
      publishSocket.setLinger(0)
      // logger.log(Level.FINE, "Stream processor pubSocket created")

      publishSocket.bind("tcp://127.0.0.1:" + publishPort)
      // logger.log(Level.FINE, "Stream processor pubSocket bound")

      // outputBuffer = new Queue[String]
      outputBuffer = new MutableList[String]

      logger.log(Level.FINE, "Stream processor initPub completed")
    } catch {
      case e: Throwable => {
        logger.log(Level.WARNING, e.getMessage)
      }
    }
    this
  }

  def receive() = {
    logger.log(Level.FINE, "Stream processor receive method called")

    var topic: String = ""
    val parser = new JSONParser()
    var receivedString: String = ""

    try {
      breakable {
        while (!Thread.currentThread().isInterrupted()) {
          logger.log(Level.FINE, "Stream processor recvStr being called for topic")
          topic = subSocket.recvStr(ZMQ.NOBLOCK)
          //Thread.sleep(1000)
          if ((topic == "") || (topic == null)) {
            logger.log(Level.FINE, "Stream processor recvStr called. No topic received.")
            Thread.sleep(5000)
          } else {
            logger.log(Level.FINE, "Stream processor recvStr called. Topic received: " + topic)
            break
          }
        }
      }

      if (!Thread.currentThread().isInterrupted()) {
        try {
          logger.log(Level.FINE, "Stream processor recvStr being called for message")
          receivedString = subSocket.recvStr(ZMQ.NOBLOCK)
          // logger.log(Level.FINE, "Stream processor - message received: " + receivedString)
          // Thread.sleep(1000)
        } catch {
          case e: ZMQException if ZMQ.Error.ETERM == e.getErrorCode() => {
            logger.log(Level.FINE, "Stream processor - no message received.")
          }
          case e: Throwable => {
            logger.log(Level.WARNING, "Exception - Stream process exception while receiving message. " + e)
            Thread.currentThread.interrupt()
          }
        }
      }

      //      val receivedString = subSocket.recvStr(ZMQ.DONTBLOCK)
      // logger.log(Level.INFO, "received message: " + receivedString + "\n")

      receivedString match {
        case _: String => {
          val measurement: String = jsonToMessage(receivedString).value

          val index = subscribeTopics.indexOf(topic) //getting index of corresponding Buffer in inputBuffers
          //logger.log(Level.INFO, "index: " + index + " in range " + subscribeTopics.length)
          //print("Obj or Null: " + inputBuffers(index))

          //append (enqueue) current message value to the input buffer
          // inputBuffers(index).enqueue(measurement)
          inputBuffers(index) += measurement

          // logger.log(Level.INFO, "input buffers at index " + index + ": " + inputBuffers(index))

          if (inputBuffers(index).size >= bufferSizes(index)) { //checking to see if size of Buffer reached max
            logger.log(Level.INFO, "Dequeuing from input buffer " + index + ". Input buffer size: " + inputBuffers(index).size)

            val list = inputBuffers(index).toList //if desired buffer size is achieved -> convert Queued measurements to list
            // logger.log(Level.INFO, "list: " + list)

            // logger.log(Level.INFO, "inputBuffer size before drop() method: " + inputBuffers(index).length)
            // logger.log(Level.INFO, "inputBuffer before drop() method: " + inputBuffers(index))

            // remove (dequeue) non-overlapping elements from the list
            inputBuffers(index) = inputBuffers(index).drop(bufferSizes(index) - bufferOverlapSizes(index))
            // inputBuffers(index).clear() //clear buffer

            // logger.log(Level.INFO, "inputBuffer size after drop() method: " + inputBuffers(index).length)
            // logger.log(Level.INFO, "inputBuffer after drop() method: " + inputBuffers(index))

            // logger.log(Level.INFO, "updated input buffer at index " + index + ": " + inputBuffers(index))

            transform(list)

            publish()
          }
        }
        case e =>
          throw new IllegalArgumentException("Message received is null. Expected String type. " + e)
      }
    } catch {
      case e: ZMQException if ZMQ.Error.ETERM == e.getErrorCode() => {
        logger.log(Level.WARNING, "Stream processors interrupted via ZMQException")
        Thread.currentThread.interrupt()
      }
      case e: InterruptedException => {
        logger.log(Level.INFO, "Stream processor service interrupted via InterruptedException")
        Thread.currentThread.interrupt()
      }
      case e: Throwable => {
        logger.log(Level.WARNING, "Stream processor raised exception while receiving message. " + e)
        Thread.currentThread.interrupt()
      }
    }
  }

  def transform(list: List[String]): Unit = {
    logger.log(Level.FINE, "Stream processor - performing Transformation")

    // import scala.collection.JavaConversions._

    val measList = new java.util.ArrayList[java.util.Map[String, Object]]()
    for (json <- list) {
      measList.add(jsonToMap(json))
    }

    val resultsListJava = transformation.apply(measList)

    // import scala.collection.JavaConversions._
    import scala.collection.JavaConverters._
    val resultsList = resultsListJava.asScala.to[ListBuffer]

    for (result <- resultsList) {
      val key = keyForSensorMeasurement(result)
      val value = mapToJson(result)
      val message: String = messageToJson(Message(key, value))

      // enqueue message to output buffer
      // outputBuffer.enqueue(message)

      outputBuffer += message
    }

    logger.log(Level.FINE, "Stream processor - transformation performed")
  }

  def publish( /*processedMapList: ListBuffer[JLinkedHashMap[String, String]]*/ ): Unit = {
    //val processedMessage: Message = epidataLiteStreamingContext(ZMQInit.streamQueue.dequeue)
    logger.log(Level.FINE, "Stream processor publish method called")

    while (!outputBuffer.isEmpty) {
      // dequeue message from output buffer

      // val msg = outputBuffer.dequeue()
      val msg = outputBuffer.get(0).get
      outputBuffer = outputBuffer.drop(1)

      // logger.log(Level.INFO, "Publised topic: " + this.publishTopic + ", published message: " + msg)
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
      logger.log(Level.INFO, "Stream processor publishSocket closed successfully: " + publishSocket)
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during publisher close. " + e.toString())
    }

    try {
      subSocket.close()
      logger.log(Level.INFO, "Stream processor subSocket closed successfully: " + subSocket)
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during subscriber close. " + e.toString())
    }

  }

  def clearPub(): Unit = {
    logger.log(Level.FINE, "node clearPub called")

    try {
      //      publishSocket.unbind("tcp://127.0.0.1:" + publishPort)
      publishSocket.unbind(publishSocket.getLastEndpoint())
      logger.log(Level.INFO, "Stream processor publishSocket unbinded: " + publishSocket)
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during publisher unbind. " + e.toString())
    }

    try {
      publishSocket.close()
      logger.log(Level.INFO, "Stream processor publishSocket closed successfully: " + publishSocket)
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during publisher close. " + e.toString())
    }
  }

  def clearSub(): Unit = {
    logger.log(Level.FINE, "Stream processor clearSub called")
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
        logger.log(Level.INFO, "Stream processor subSocket unbound: " + subSocket)
      }
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during subscriber unbind. " + e.toString())
    }

    try {
      subSocket.close()
      logger.log(Level.INFO, "Stream processor subSocket closed: " + subSocket)
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

  private def keyForSensorMeasurement(measurement: java.util.Map[String, Object]): String = {
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
