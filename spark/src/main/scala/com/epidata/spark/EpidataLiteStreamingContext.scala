/*
 * Copyright (c) 2015-2023 EpiData, Inc.
*/
package com.epidata.spark

import java.util
import java.util.concurrent.{ Executors, ExecutorService, TimeUnit, Future }

import org.zeromq.ZMQ
import org.zeromq.ZMQException
import com.epidata.spark.ops._
import org.apache.spark.sql.{ DataFrame, SQLContext }
import com.fasterxml.jackson.databind.JsonMappingException
import scala.collection.mutable.{ HashMap, Map => MutableMap }
import scala.io.StdIn
import scala.util.control.Breaks._
import java.io.FileInputStream
import java.io.IOException
import java.nio.file.Paths
import java.util.logging._
import py4j.GatewayServer
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }
import scala.collection.mutable.ListBuffer

class EpidataLiteStreamingContext(epidataConf: EpiDataConf = EpiDataConf("", "")) {
  var startPort: Integer = 5551
  var cleansedPort: Integer = 5552
  var summaryPort: Integer = 5553
  var dynamicPort: Integer = 5554
  var intermediatePort: Integer = 5555
  var shutdownPort: Integer = 4999
  var shutdownTopic: String = "SHUTDOWN"
  var shutdownMessage: String = "SHUTDOWN"
  var shutdownSocket: ZMQ.Socket = _
  var processors: ListBuffer[StreamingNode] = _
  var streamAuditor: EpidataLiteStreamValidation = _
  var context: ZMQ.Context = _
  val receiveTimeout: Integer = -1
  var topicMap: MutableMap[String, Integer] = _

  var initSubFlag: Boolean = _
  var initPubFlag: Boolean = _
  var startNodesFlag: Boolean = _
  var stopStreamFlag: Boolean = _

  val logger = Logger.getLogger("EpiDataLiteStreamingContext Logger")
  logger.setLevel(Level.FINE)
  //  logger.addHandler(new ConsoleHandler)

  private var poolSize: Int = 1
  private var executorService: ExecutorService = _

  //adding custom handler
  private val basePath = scala.util.Properties.envOrElse("EPIDATA_HOME", "")
  if (basePath.equals("") || basePath == null) {
    throw new IllegalStateException("EPIDATA_HOME environment variable not set")
  }

  private val conf = ConfigFactory.parseFile(new java.io.File(Paths.get(basePath, "conf", "sqlite-defaults.conf").toString())).resolve()
  private val logFilePath = Paths.get(basePath, "log", conf.getString("spark.epidata.streamLogFileName")).toString()

  val fileHandler = new FileHandler(logFilePath)
  logger.addHandler(fileHandler)

  private val measurementClass = epidataConf.model match {
    case m if m.trim.isEmpty => scala.util.Properties.envOrElse("EPIDATA_MEASUREMENT_MODEL", conf.getString("spark.epidata.measurementClass"))
    case m: String => m
  }
  // logger.log(Level.INFO, "measurement class: " + measurementClass)

  // bufferSize for stream processing
  var bufferSize: Integer = _
  var bufferOverlapSize: Integer = _

  // Auxiliary constructor for Java and Python
  def this() = {
    this(EpiDataConf("", ""))
    this.logger.log(Level.INFO, "EpiDataLiteStreamingContext object created.")
  }

  def init(): Unit = {
    // logger.log(Level.FINE, "EpiDataLiteStreamingContext is being initialized.")
    context = ZMQ.context(1)
    processors = ListBuffer()
    topicMap = MutableMap[String, Integer]()
    topicMap.put("measurements_original", startPort)
    topicMap.put("measurements_cleansed", cleansedPort)
    topicMap.put("measurements_summary", summaryPort)
    topicMap.put("measurements_dynamic", dynamicPort)
    topicMap.put(shutdownTopic, shutdownPort)

    streamAuditor = new EpidataLiteStreamValidation()
    streamAuditor.init(logger)

    configureShutdownMessenger(context)

    // bufferSize initialized based on configuration settings
    bufferSize = conf.getInt("spark.epidata.streamDefaultBufferSize")
    bufferOverlapSize = conf.getInt("spark.epidata.streamDefaultBufferOverlap")

    initSubFlag = false
    initPubFlag = false
    startNodesFlag = false
    stopStreamFlag = false

    logger.log(Level.INFO, "EpiDataLiteStreamingContext initialized.")

    //    addShutdownHook()
  }

  /** Interface for Java and Python. */
  def createTransformation(
    opName: String,
    meas_names: java.util.List[String],
    params: java.util.Map[String, String]): Transformation = {
    logger.log(Level.FINE, "createTransformation API method invoked.")

    import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
    //import scala.collection.JavaConverters
    import scala.collection.JavaConversions._

    // logger.log(Level.INFO, "opName: " + opName + ", meas_names: " + meas_names + ", params: " + params)

    val sBuffer = asScalaBuffer(meas_names)

    // logger.log(Level.INFO, "opName: " + opName + ", sBuffer: " + sBuffer.toList + ", params: " + params.toMap)

    createTransformation(opName, sBuffer.toList, params.toMap)
  }

  def createTransformation(opName: String, meas_names: List[String], params: Map[String, Any]): Transformation = {
    // logger.log(Level.FINE, "createTransformation method invoked. Transformation name: " + opName.toString() + ", measurments' list: " + meas_names + ", parameters: " + params)

    // create and return a transformation object
    val op: Transformation = opName match {
      case "Identity" => new Identity(Some(meas_names))
      case "FillMissingValue" => new FillMissingValue(meas_names, params.getOrElse("method", "rolling").asInstanceOf[String], params.getOrElse("s", 3).asInstanceOf[Int])
      case "OutlierDetector" => new OutlierDetector("meas_value", params.getOrElse("method", "quartile").asInstanceOf[String])
      case "MeasStatistics" => new MeasStatistics(measurementClass, meas_names, params.getOrElse("method", "standard").asInstanceOf[String])
      case "InverseTranspose" => new InverseTranspose(meas_names)
      case "NAs" => new NAs()
      case "Outliers" => new Outliers(meas_names, params.get("mpercentage").get.asInstanceOf[Int], params.getOrElse("method", "delete").asInstanceOf[String])
      case "Resample" => new Resample(meas_names, params.get("time_interval").get.asInstanceOf[Int], params.get("timeunit").get.asInstanceOf[String])
      case "Transpose" => new Transpose(meas_names)
      case _ => new Identity(Some(meas_names))
    }

    logger.log(Level.FINE, "Transformation created. Name: " + op.name + ", measurments' list: " + meas_names + ", parameters: " + params)

    op
  }

  def createStream(sourceTopic: String, destinationTopic: String, operation: Transformation): Unit = {
    createStream(ListBuffer(sourceTopic), ListBuffer(bufferSize), ListBuffer(bufferOverlapSize), destinationTopic, operation)
  }

  def createStream(sourceTopics: ListBuffer[String], destinationTopic: String, operation: Transformation): Unit = {
    createStream(sourceTopics, ListBuffer(bufferSize), ListBuffer(bufferOverlapSize), destinationTopic, operation)
  }

  def createStream(sourceTopic: String, bufferSize: Integer, bufferOverlapSize: Integer, destinationTopic: String, operation: Transformation): Unit = {
    createStream(ListBuffer(sourceTopic), ListBuffer(bufferSize), ListBuffer(bufferOverlapSize), destinationTopic, operation)
  }

  def createStream(sourceTopics: ListBuffer[String], bufferSize: Integer, bufferOverlapSize: Integer, destinationTopic: String, operation: Transformation): Unit = {
    createStream(sourceTopics, ListBuffer(bufferSize), ListBuffer(bufferOverlapSize), destinationTopic, operation)
  }

  def createStream(sourceTopic: String, bufferSizes: ListBuffer[Integer], bufferOverlapSizes: ListBuffer[Integer], destinationTopic: String, operation: Transformation): Unit = {
    createStream(ListBuffer(sourceTopic), bufferSizes, bufferOverlapSizes, destinationTopic, operation)
  }

  def createStream(sourceTopics: ListBuffer[String], bufferSizes: ListBuffer[Integer], bufferOverlapSizes: ListBuffer[Integer], destinationTopic: String, operation: Transformation): Unit = {
    logger.log(Level.FINE, "createStream method invoked. Source topic: " + sourceTopics.toString() + ", buffer sizes: " + bufferSizes.toString() + ", buffer overlap sizes:" + bufferOverlapSizes.toString() + ", destination topic: " + destinationTopic.toString() + ", transformation: " + operation.name)
    var streamSourcePort: ListBuffer[String] = ListBuffer()

    sourceTopics += shutdownTopic
    for (topic <- sourceTopics) {
      if (topicMap.get(topic) != None) {
        streamSourcePort += topicMap.get(topic).toString.replace("Some(", "").dropRight(1)
      } else {
        throw new IllegalArgumentException("Source Topic is not recognized. Please correct the Source Topic or define it by making it a Destination Topic first.")
      }
    }

    // logger.log(Level.INFO, "Stream source ports added: " + streamSourcePort.toString())

    topicMap.get(destinationTopic) match {
      case None => {
        topicMap.put(destinationTopic, intermediatePort)
        intermediatePort += 1
        // logger.log(Level.INFO, "New Stream Destination Topic created.")
      }
      case _ => {
        // logger.log(Level.INFO, "Stream Destination Topic already exists.")
      }
    }

    val streamDestinationPort = topicMap.get(destinationTopic) match {
      case Some(port) => port.toString
      case None => {
        // logger.log(Level.WARNING, "Destination Topic is not recognized.")
        throw new IllegalArgumentException("Destination Topic is not recognized.")
      }
    }

    streamAuditor.addProcessor(
      streamSourcePort,
      sourceTopics,
      bufferSizes,
      bufferOverlapSizes,
      streamDestinationPort,
      destinationTopic,
      operation)

    logger.log(Level.INFO, "Stream Processing Node configured successfully. Source topics: " + sourceTopics + ", transformation: " + operation.name + ", destination topic: " + destinationTopic)
  }

  def startStream(): Unit = {
    stopStreamFlag = false

    val processorConfigs: ListBuffer[MutableMap[String, Any]] = streamAuditor.validate(topicMap, intermediatePort)
    // logger.log(Level.FINE, "Stream processing nodes are being created.")

    poolSize = processorConfigs.size
    executorService = Executors.newFixedThreadPool(poolSize)
    // logger.log(Level.INFO, "Thread pool size: " + poolSize)

    // logger.log(Level.FINE, "Steam processing nodes are being started.")
    for (processorConfig <- processorConfigs) {
      // logger.log(Level.FINE, "Stream processing node configuration: " + processorConfig.toString())
      executorService.submit(new Runnable {
        override def run(): Unit = {
          // logger.log(Level.FINE, "New streaming node being created based on its configuration")
          // logger.log(Level.FINE, "Stream processing node so far: " + processors.toString())

          val processor: StreamingNode = new StreamingNode(logger)
          logger.log(Level.FINE, "Stream processing node has been created: " + processor.toString())
          processors += processor

          // logger.log(Level.FINE, "Processing node's initSub being called")
          processorConfig.get("transformation") match {
            case Some(operation: Transformation) => {
              // logger.log(Level.FINE, "Stream processing node initSub being called with Transformation object")
              processor.initSub(
                context,
                processorConfig.get("receivePorts") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("receiveTopics") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("bufferSizes") match { case Some(list: ListBuffer[Integer]) => list },
                processorConfig.get("bufferOverlapSizes") match { case Some(list: ListBuffer[Integer]) => list },
                receiveTimeout,
                operation)
              // logger.log(Level.FINE, "Stream processing node's initSub called successfully with Transformation object")
              // logger.log(Level.INFO, "New stream processing node initialized with transformation object. Stream processing node object: " + processor.toString())
            }
            case Some(operation: String) => {
              // logger.log(Level.FINE, "Stream processing node's initSub being called with transformation string")
              processor.initSub(
                context,
                processorConfig.get("receivePorts") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("receiveTopics") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("bufferSizes") match { case Some(list: ListBuffer[Integer]) => list },
                processorConfig.get("bufferOverlapSizes") match { case Some(list: ListBuffer[Integer]) => list },
                receiveTimeout,
                createTransformation(operation.toString, List(), Map[String, String]()))
              // logger.log(Level.FINE, "Stream processing node's initSub called successfully with transformation string")
              // logger.log(Level.INFO, "New stream processing node initialized with transformation string. Stream processing node object: " + processor.toString())
            }
            case _: Throwable => {
              // logger.log(Level.WARNING, "Unrecognized transformation type")
              throw new Exception("Unrecognized transformation type")
            }
          }

          // logger.log(Level.FINE, "Stream processing node's initPub being called")
          processor.initPub(
            context,
            processorConfig.get("sendPort") match { case Some(list: String) => list },
            processorConfig.get("sendTopic") match { case Some(list: String) => list })
          // logger.log(Level.FINE, "Stream processing node's initPub called successfully")

          breakable {
            while ((!Thread.currentThread().isInterrupted()) && (!stopStreamFlag)) {
              try {
                // logger.log(Level.FINE, "Stream processing node waiting to receive data")
                processor.receive()
              } catch {
                case e: ZMQException if ZMQ.Error.ETERM == e.getErrorCode() => {
                  //              case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode
                  logger.log(Level.WARNING, "Stream processing nodes interrupted via ZMQException")
                  Thread.currentThread.interrupt()
                  break
                }
                case e: InterruptedException => {
                  logger.log(Level.WARNING, "Stream processing nodes interrupted via InterruptedException")
                  Thread.currentThread.interrupt()
                  break
                }
                case e: JsonMappingException => {
                  logger.log(Level.WARNING, "JsonMappingException: " + e.getMessage)
                  throw new Exception(e.getMessage)
                  break
                }
                case e: Throwable => {
                  logger.log(Level.WARNING, "Unexpected Exception. " + e.getMessage)
                  Thread.currentThread.interrupt()
                  break
                }
              }
              // logger.log(Level.FINE, "Waiting for interrupt or stopStreamFlag")
            }
          }

          // logger.log(Level.FINE, "Stream processing node's clearPub being called")
          processor.clearPub()
          // logger.log(Level.FINE, "Stream processing node's clearPub called successfully")

          // logger.log(Level.FINE, "Stream processing node's clearSub being called")
          processor.clearSub()
          // logger.log(Level.FINE, "Stream processing node's clearSub called successfully")

          logger.log(Level.INFO, "Stream processing node " + processor.toString() + " has been cleared.")
        }
      })
    }

    logger.log(Level.INFO, "EpiData stream started successfully.")
  }

  def stopStream(): Unit = {
    logger.log(Level.FINE, "Streams are being stopped")
    stopStreamFlag = true

    try {
      sendShutdownMessage()
      Thread.sleep(2000)

      // logger.log(Level.INFO, "Messaging stream is being terminated")
      // context.term()
      // logger.log(Level.INFO, "Messaging stream terminated successfully.")
    } catch {
      case e: InterruptedException =>
        logger.log(Level.WARNING, "InterruptedException during stream shutdown", e.getMessage)
        executorService.shutdownNow()
        Thread.currentThread().interrupt();
        context.term()
        logger.log(Level.INFO, "Streams stopped successfully.")
      case e: Throwable =>
        logger.log(Level.WARNING, "Exception during stream termination: " + e.getMessage)
    }

    clearShutdownMessenger()

    logger.log(Level.INFO, "Streams stopped successfully.")
  }

  def clear(): Unit = {
    try {
      //logger.log(Level.INFO, "EpiDataLiteStreamingContext is being cleared (reset)")
      context.term()
      logger.log(Level.INFO, "EpiDataLiteStreamingContext cleared (reset) successfully.")
    } catch {
      case e: Throwable =>
        logger.log(Level.WARNING, "Exception during stream termination: " + e.getMessage)
    }
  }

  private def configureShutdownMessenger(context: ZMQ.Context): Unit = {
    try {
      shutdownSocket = context.socket(ZMQ.PUB)
      shutdownSocket.setLinger(0)
      shutdownSocket.bind("tcp://127.0.0.1:" + shutdownPort)

      //logger.log(Level.FINE, "Stream shutdown messenger configured")
    } catch {
      case e: Throwable => {
        logger.log(Level.WARNING, "Exception during shutdown messenger configuration: " + e.getMessage)
      }
    }
  }

  private def sendShutdownMessage(): Unit = {
    shutdownSocket.sendMore(shutdownTopic)
    shutdownSocket.send(shutdownMessage.getBytes(), 0)
    logger.log(Level.INFO, "Shutdown message sent.")
  }

  private def clearShutdownMessenger(): Unit = {
    try {
      shutdownSocket.unbind(shutdownSocket.getLastEndpoint())
      // logger.log(Level.INFO, "Stream processor shutdownSocket unbinded: " + shutdownSocket)
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during shutdown messenger unbind. " + e.toString())
    }

    try {
      shutdownSocket.close()
      // logger.log(Level.INFO, "Stream processor shutdownSocket closed successfully: " + shutdownSocket)
    } catch {
      case e: Throwable => logger.log(Level.WARNING, "Exception during shutdown messenger close. " + e.toString())
    }

    logger.log(Level.INFO, "Shutdown messenger cleared.")
  }

  //  def addShutdownHook(): Unit = {
  //    Runtime.getRuntime().addShutdownHook(new Thread { () => stopStream() })
  //  }
}

object EpiDataLiteEntryPoint {

  //  val logger = Logger.getLogger("EpiDataLiteEntryPoint Logger")
  //  logger.setLevel(Level.FINE)
  //  logger.addHandler(new ConsoleHandler)

  val ec = new EpidataLiteContext()
  // logger.log(Level.INFO, "Java Gateway - EpiDataLiteContext created")

  val esc = new EpidataLiteStreamingContext()
  // logger.log(Level.INFO, "Java Gateway - EpiDataLiteStreamingContext created")

  def returnEpiDataLiteContext(): EpidataLiteContext = {
    // logger.log(Level.INFO, "returning EpiDataLiteContext")
    return ec
  }

  def returnEpiDataLiteStreamingContext(): EpidataLiteStreamingContext = {
    // logger.log(Level.INFO, "returning EpiDataLiteStreamingContext")
    return esc
  }

  def main(args: Array[String]): Unit = {
    val server = new GatewayServer(EpiDataLiteEntryPoint, 25550)
    //    val server = new GatewayServer(EpiDataLiteEntryPoint)
    // logger.log(Level.INFO, "Running EpiData's Java GatewayServer")
    server.start()
  }
}
