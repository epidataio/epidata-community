/*
 * Copyright (c) 2015-2022 EpiData, Inc.
*/
package com.epidata.spark

import java.util
import java.util.concurrent.{ Executors, ExecutorService, TimeUnit, Future }

import org.zeromq.ZMQ
import org.zeromq.ZMQException
import com.epidata.spark.ops.{ FillMissingValue, Identity, MeasStatistics, OutlierDetector, Transformation }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import com.fasterxml.jackson.databind.JsonMappingException
import scala.collection.mutable.{ HashMap, Map => MutableMap }
import scala.io.StdIn
import scala.util.control.Breaks._
//import scala.collection.JavaConverters
//-------------------logger package--------
import java.io.FileInputStream
import java.io.IOException
import java.util.logging._
import py4j.GatewayServer
//import java.util.logging.ConsoleHandler
//import java.util.logging.FileHandler
//import java.util.logging.Handler
//import java.util.logging.Level
//import java.util.logging.LogManager
//import java.util.logging.Logger
//--------------------------------------------
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable.ListBuffer

class EpidataLiteStreamingContext(epidataConf: EpiDataConf = EpiDataConf("", "")) {
  var startPort: Integer = 5551
  var cleansedPort: Integer = 5552
  var summaryPort: Integer = 5553
  var dynamicPort: Integer = 5554
  var processors: ListBuffer[StreamingNode] = _
  val streamAuditor = new EpidataStreamValidation()
  //var _runStream: Boolean = _
  var context: ZMQ.Context = _
  val receiveTimeout: Integer = -1
  var topicMap: MutableMap[String, Integer] = _
  var intermediatePort: Integer = 5555

  var initSubFlag = false
  var initPubFlag = false
  var startNodesFlag: Boolean = false
  var stopStreamFlag: Boolean = false

  val logger = Logger.getLogger("EpiDataLiteStreamingContext Logger")
  logger.setLevel(Level.FINE)
  //  logger.addHandler(new ConsoleHandler)

  private var poolSize: Int = 1
  private var executorService: ExecutorService = _

  //adding custom handler
  private val conf = ConfigFactory.parseResources("sqlite-defaults.conf").resolve()
  private val basePath = new java.io.File(".").getAbsoluteFile().getParentFile().getParent()

  private var logFilePath = conf.getString("spark.epidata.logFilePath")
  if ((logFilePath == "") || (logFilePath == null)) {
    logFilePath = basePath + "/log/" + conf.getString("spark.epidata.streamLogFileName")
  } else {
    logFilePath = logFilePath + conf.getString("spark.epidata.streamLogFileName")
  }
  println("log file path: " + logFilePath)
  val fileHandler = new FileHandler(logFilePath)
  logger.addHandler(fileHandler)
  //logger.log(Level.INFO, "File handler added")

  //default bufferSize based on configuration settings
  var bufferSize: Integer = conf.getInt("spark.epidata.streamDefaultBufferSize")

  // Auxiliary constructor for Java and Python
  def this() = {
    this(EpiDataConf("", ""))
    this.logger.log(Level.INFO, "EpiDataLiteStreamingContext object created with settings: " + epidataConf.model + ", " + epidataConf.dbUrl)
  }

  def config(): Unit = {}

  def init(): Unit = {
    logger.log(Level.INFO, "EpiDataLiteStreamingContext is being initialized.")
    //ec.start_streaming()
    context = ZMQ.context(1)
    //_runStream = true
    processors = ListBuffer()
    topicMap = MutableMap[String, Integer]()
    topicMap.put("measurements_original", startPort)
    topicMap.put("measurements_cleansed", cleansedPort)
    topicMap.put("measurements_summary", summaryPort)
    topicMap.put("measurements_dynamic", dynamicPort)
    streamAuditor.init()

    logger.log(Level.INFO, "EpiDataLiteStreamingContext initialized.")

    //    addShutdownHook()
  }

  def createTransformation(opName: String, meas_names: List[String], params: Map[String, Any]): Transformation = {
    logger.log(Level.INFO, "Transformation " + opName.toString() + " is being created.")

    // create and return a transformation object
    opName match {
      case "Identity" => new Identity()

      case "FillMissingValue" => new FillMissingValue(meas_names, params.getOrElse("method", "rolling").asInstanceOf[String], params.getOrElse("s", 3).asInstanceOf[Int])
      //case "OutlierDetector" => new OutlierDetector("meas_value", params.get("method"))
      case "MeasStatistics" => new MeasStatistics(meas_names, params.getOrElse("method", "standard").asInstanceOf[String])
      case _ => new Identity()
    }

    //    logger.log(Level.INFO, "Transformation " + opName.toString + " created.")
    //    opName
  }

  /** Interface for Java and Python. */
  def createTransformation(
    opName: String,
    meas_names: java.util.List[String],
    params: java.util.Map[String, String]): Transformation = {
    // logger.log(Level.INFO, "createTransformation method invoked.")
    import scala.collection.JavaConversions._
    val sBuffer = asScalaBuffer(meas_names)
    createTransformation(opName, sBuffer.toList, params.toMap)
  }

  def createStream(sourceTopic: String, destinationTopic: String, operation: Transformation): Unit = {
    logger.log(Level.INFO, "createStream method invoked. Source topic: " + sourceTopic.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    createStream(ListBuffer(sourceTopic), ListBuffer(bufferSize), destinationTopic, operation)
  }

  def createStream(sourceTopic: ListBuffer[String], destinationTopic: String, operation: Transformation): Unit = {
    logger.log(Level.INFO, "createStream method invoked. Source topics: " + sourceTopic.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    createStream(sourceTopic, ListBuffer(bufferSize), destinationTopic, operation)
  }

  def createStream(sourceTopic: String, bufferSize: Integer, destinationTopic: String, operation: Transformation): Unit = {
    logger.log(Level.INFO, "createStream method invoked. Source topic: " + sourceTopic.toString() + ", buffer size: " + bufferSize.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    createStream(ListBuffer(sourceTopic), ListBuffer(bufferSize), destinationTopic, operation)
  }

  def createStream(sourceTopic: ListBuffer[String], bufferSize: Integer, destinationTopic: String, operation: Transformation): Unit = {
    logger.log(Level.INFO, "createStream method invoked. Source topic: " + sourceTopic.toString() + ", buffer size: " + bufferSize.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    createStream(sourceTopic, ListBuffer(bufferSize), destinationTopic, operation)
  }

  def createStream(sourceTopic: String, bufferSizes: ListBuffer[Integer], destinationTopic: String, operation: Transformation): Unit = {
    logger.log(Level.INFO, "createStream method invoked. Source topic: " + sourceTopic.toString() + ", buffer sizes: " + bufferSizes.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    createStream(ListBuffer(sourceTopic), bufferSizes, destinationTopic, operation)
  }

  def createStream(sourceTopic: ListBuffer[String], bufferSizes: ListBuffer[Integer], destinationTopic: String, operation: Transformation): Unit = {
    //logger(Level.INFO, "sourcetopic:  " + sourceTopic)
    //logger.log(Level.INFO, "destinationTopic:  " + destinationTopic)
    //logger.log(Level.INFO, "transformation:  " + operation)
    //-------------------------------------------------------
    logger.log(Level.INFO, "createStream method invoked. Source topic: " + sourceTopic.toString() + ", buffer size: " + bufferSizes.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    var streamSourcePort: ListBuffer[String] = ListBuffer()
    for (topic <- sourceTopic) {
      if (topicMap.get(topic) != None) {
        streamSourcePort += topicMap.get(topic).toString.replace("Some(", "").dropRight(1)
      } else {
        throw new IllegalArgumentException("Source Topic is not recognized.")
      }
    }

    logger.log(Level.INFO, "Stream source ports added: " + streamSourcePort.toString())

    topicMap.get(destinationTopic) match {
      case None => {
        topicMap.put(destinationTopic, intermediatePort)
        intermediatePort += 1
        logger.log(Level.INFO, "Stream destination port added.")
      }
      case _ => {
        logger.log(Level.INFO, "Stream destination port already exists.")
      }
    }

    val streamDestinationPort = topicMap.get(destinationTopic) match {
      case Some(port) => port.toString
      case None => {
        logger.log(Level.WARNING, "Destination Topic is not recognized.")
        throw new IllegalArgumentException("Destination Topic is not recognized.")
      }
    }

    streamAuditor.addProcessor(
      streamSourcePort,
      sourceTopic,
      bufferSizes,
      streamDestinationPort,
      destinationTopic,
      operation)

    logger.log(Level.INFO, "Stream processing node created successfully.")
  }

  def startStream(): Unit = {
    stopStreamFlag = false

    val processorConfigs: ListBuffer[MutableMap[String, Any]] = streamAuditor.validate(topicMap, intermediatePort)
    logger.log(Level.INFO, "Streams being started. \nStreaming node configs: " /* + processorConfigs.toString() */ )

    def createProcessor(processorConfig: MutableMap[String, Any]): StreamingNode = {
      logger.log(Level.INFO, "createProcessor method invoked.")
      processorConfig.get("transformation") match {
        case Some(operation: Transformation) => {
          val processor: StreamingNode = new StreamingNode().init(
            context,
            processorConfig.get("receivePorts") match { case Some(list: ListBuffer[String]) => list },
            processorConfig.get("receiveTopics") match { case Some(list: ListBuffer[String]) => list },
            processorConfig.get("bufferSizes") match { case Some(list: ListBuffer[Integer]) => list },
            processorConfig.get("sendPort") match { case Some(list: String) => list },
            processorConfig.get("sendTopic") match { case Some(list: String) => list },
            receiveTimeout,
            operation,
            logger)
          logger.log(Level.INFO, "New streaming node " /* + processor.toString() */ + " initialized with transformation object")
          processor
        }
        case Some(operation: String) => {
          val processor: StreamingNode = new StreamingNode().init(
            context,
            processorConfig.get("receivePorts") match { case Some(list: ListBuffer[String]) => list },
            processorConfig.get("receiveTopics") match { case Some(list: ListBuffer[String]) => list },
            processorConfig.get("bufferSizes") match { case Some(list: ListBuffer[Integer]) => list },
            processorConfig.get("sendPort") match { case Some(list: String) => list },
            processorConfig.get("sendTopic") match { case Some(list: String) => list },
            receiveTimeout,
            createTransformation(operation.toString, List(), Map[String, String]()),
            logger)
          logger.log(Level.INFO, "New streaming node " /* + processor.toString() */ + " initialized with transformation string")
          processor
        }
        case _: Throwable => {
          logger.log(Level.INFO, "Unrecognized transformation type")
          throw new Exception("Unrecognized trasnformaiton type")
        }
      }
    }

    //    poolSize = processors.size
    poolSize = processorConfigs.size
    executorService = Executors.newFixedThreadPool(poolSize)

    logger.log(Level.INFO, "Steam processors are being started.")
    for (processorConfig <- processorConfigs) {
      executorService.submit(new Runnable {
        override def run(): Unit = {
          //          val processor: StreamingNode = createProcessor(processorConfig)
          logger.log(Level.INFO, "processor is being created")
          val processor: StreamingNode = new StreamingNode()
          logger.log(Level.INFO, "processor created: " + processor)
          processors += processor

          logger.log(Level.INFO, "processor initSub being called")
          processorConfig.get("transformation") match {
            case Some(operation: Transformation) => {
              processor.initSub(
                context,
                processorConfig.get("receivePorts") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("receiveTopics") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("bufferSizes") match { case Some(list: ListBuffer[Integer]) => list },
                receiveTimeout,
                operation,
                logger)
              logger.log(Level.INFO, "processor initSub called successfully")
              logger.log(Level.INFO, "New streaming node " + processor.toString() + " initialized with transformation object")
            }
            case Some(operation: String) => {
              val processor: StreamingNode = new StreamingNode()
              processor.initSub(
                context,
                processorConfig.get("receivePorts") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("receiveTopics") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("bufferSizes") match { case Some(list: ListBuffer[Integer]) => list },
                receiveTimeout,
                createTransformation(operation.toString, List(), Map[String, String]()),
                logger)
              logger.log(Level.INFO, "processor initSub called successfully")
              logger.log(Level.INFO, "New streaming node " + processor.toString() + " initialized with transformation string")
            }
            case _: Throwable => {
              logger.log(Level.INFO, "Unrecognized transformation type")
              throw new Exception("Unrecognized transformation type")
            }
          }

          logger.log(Level.INFO, "processor initPub being called")
          processor.initPub(
            context,
            processorConfig.get("sendPort") match { case Some(list: String) => list },
            processorConfig.get("sendTopic") match { case Some(list: String) => list },
            logger)
          logger.log(Level.INFO, "processor initPub called successfully")
          logger.log(Level.INFO, "New streaming node " + processor.toString() + " initialized with transformation object")

          logger.log(Level.INFO, "Steaming Node created: " + processor)
          breakable {
            while ((!Thread.currentThread().isInterrupted()) && (!stopStreamFlag)) {
              try {
                processor.receive()
              } catch {
                case e: ZMQException if ZMQ.Error.ETERM == e.getErrorCode() => {
                  //              case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode
                  logger.log(Level.WARNING, "Processors interrupted via ZMQException")
                  Thread.currentThread.interrupt()
                  break
                }
                case e: InterruptedException => {
                  logger.log(Level.INFO, "Processor service interrupted via InterruptedException")
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
              logger.log(Level.INFO, "waiting for interrupt or stopStreamFlag")
              logger.log(Level.INFO, "thread interrupt status: " + Thread.currentThread().isInterrupted())
            }
          }

          logger.log(Level.INFO, "processor clearSub being called")
          processor.clearSub()
          logger.log(Level.INFO, "processor clearSub called successfully")

          logger.log(Level.INFO, "processor clearPub being called")
          //          processor.clear()
          processor.clearPub()
          logger.log(Level.INFO, "processor clearPub called successfully")

          logger.log(Level.INFO, "Processor " + processor.toString() + " has been cleared.")
        }
      })
    }

    logger.log(Level.INFO, "EpiData stream started successfully.")
  }

  def stopStream(): Unit = {
    //_runStream = false
    logger.log(Level.INFO, "Streams are being stopped")

    try {
      stopStreamFlag = true
      logger.log(Level.INFO, "stopStreamFlag set to true")
      Thread.sleep(1000)

      logger.log(Level.INFO, "context is being terminated")
      context.term()
      logger.log(Level.INFO, "ZMQ context terminated successfully.")
    } catch {
      case e: InterruptedException =>
        logger.log(Level.WARNING, "InterruptedException during stream shutdown", e.getMessage)
        executorService.shutdownNow()
        Thread.currentThread().interrupt();
        context.term()
        logger.log(Level.INFO, "Streams stopped successfully.")
      case e: Throwable =>
        logger.log(Level.WARNING, "Exception during ZMQ context termination Line 430." + e.getMessage)
    }
  }

  def addShutdownHook(): Unit = {
    Runtime.getRuntime().addShutdownHook(new Thread { () => stopStream() })
  }

}

object OpenGateway {

  def main(args: Array[String]): Unit = {
    val ec = new EpidataLiteContext()
    val esc = new EpidataLiteStreamingContext();
    val server = new GatewayServer(esc);
    println("RUNNING SERVER")
    server.start()
  }
}
