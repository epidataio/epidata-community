/*
 * Copyright (c) 2015-2022 EpiData, Inc.
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

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable.ListBuffer
//import scala.collection.JavaConverters

class EpidataLiteStreamingContext(epidataConf: EpiDataConf = EpiDataConf("", "")) {
  var startPort: Integer = 5551
  var cleansedPort: Integer = 5552
  var summaryPort: Integer = 5553
  var dynamicPort: Integer = 5554
  var processors: ListBuffer[StreamingNode] = _
  val streamAuditor = new EpidataLiteStreamValidation()
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

  //default bufferSize based on configuration settings
  var bufferSize: Integer = conf.getInt("spark.epidata.streamDefaultBufferSize")
  var bufferOverlapSize: Integer = conf.getInt("spark.epidata.streamDefaultBufferOverlap")

  // Auxiliary constructor for Java and Python
  def this() = {
    this(EpiDataConf("", ""))
    this.logger.log(Level.INFO, "EpiDataLiteStreamingContext object created with settings: " + epidataConf.model + ", " + epidataConf.dbUrl)
  }

  def init(): Unit = {
    // logger.log(Level.INFO, "EpiDataLiteStreamingContext is being initialized.")
    context = ZMQ.context(1)
    processors = ListBuffer()
    topicMap = MutableMap[String, Integer]()
    topicMap.put("measurements_original", startPort)
    topicMap.put("measurements_cleansed", cleansedPort)
    topicMap.put("measurements_summary", summaryPort)
    topicMap.put("measurements_dynamic", dynamicPort)
    streamAuditor.init(logger)

    logger.log(Level.INFO, "EpiDataLiteStreamingContext initialized.")

    //    addShutdownHook()
  }

  def createTransformation(opName: String, meas_names: List[String], params: Map[String, Any]): Transformation = {
    // logger.log(Level.INFO, "Transformation " + opName.toString() + " is being created.")

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
      case _ => new Identity()
    }

    logger.log(Level.INFO, "Transformation " + op.toString() + " created.")

    op
  }

  /** Interface for Java and Python. */
  def createTransformation(
    opName: String,
    meas_names: java.util.List[String],
    params: java.util.Map[String, String]): Transformation = {
    logger.log(Level.FINE, "createTransformation API method invoked.")
    import scala.collection.JavaConversions._
    val sBuffer = asScalaBuffer(meas_names)
    createTransformation(opName, sBuffer.toList, params.toMap)
  }

  def createStream(sourceTopic: String, destinationTopic: String, operation: Transformation): Unit = {
    // logger.log(Level.FINE, "createStream method invoked. Source topic: " + sourceTopic.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    createStream(ListBuffer(sourceTopic), ListBuffer(bufferSize), ListBuffer(bufferOverlapSize), destinationTopic, operation)
  }

  def createStream(sourceTopics: ListBuffer[String], destinationTopic: String, operation: Transformation): Unit = {
    // logger.log(Level.FINE, "createStream method invoked. Source topics: " + sourceTopics.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    createStream(sourceTopics, ListBuffer(bufferSize), ListBuffer(bufferOverlapSize), destinationTopic, operation)
  }

  def createStream(sourceTopic: String, bufferSize: Integer, bufferOverlapSize: Integer, destinationTopic: String, operation: Transformation): Unit = {
    // logger.log(Level.FINE, "createStream method invoked. Source topic: " + sourceTopic.toString() + ", buffer size: " + bufferSize.toString() + ", buffer overlap size: " + bufferOverlapSize.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    createStream(ListBuffer(sourceTopic), ListBuffer(bufferSize), ListBuffer(bufferOverlapSize), destinationTopic, operation)
  }

  def createStream(sourceTopics: ListBuffer[String], bufferSize: Integer, bufferOverlapSize: Integer, destinationTopic: String, operation: Transformation): Unit = {
    // logger.log(Level.FINE, "createStream method invoked. Source topic: " + sourceTopics.toString() + ", buffer size: " + bufferSize.toString() + ", buffer overlap size: " + bufferOverlapSize.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    createStream(sourceTopics, ListBuffer(bufferSize), ListBuffer(bufferOverlapSize), destinationTopic, operation)
  }

  def createStream(sourceTopic: String, bufferSizes: ListBuffer[Integer], bufferOverlapSizes: ListBuffer[Integer], destinationTopic: String, operation: Transformation): Unit = {
    // logger.log(Level.FINE, "createStream method invoked. Source topic: " + sourceTopic.toString() + ", buffer sizes: " + bufferSizes.toString() + ", buffer overlap sizes: " + bufferOverlapSizes.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    createStream(ListBuffer(sourceTopic), bufferSizes, bufferOverlapSizes, destinationTopic, operation)
  }

  def createStream(sourceTopics: ListBuffer[String], bufferSizes: ListBuffer[Integer], bufferOverlapSizes: ListBuffer[Integer], destinationTopic: String, operation: Transformation): Unit = {
    logger.log(Level.FINE, "createStream method invoked. Source topic: " + sourceTopics.toString() + ", buffer sizes: " + bufferSizes.toString() + ", buffer overlap sizes:" + bufferOverlapSizes.toString() + ", destination topic: " + destinationTopic.toString() /* + ", transformation: " + operation */ )
    var streamSourcePort: ListBuffer[String] = ListBuffer()
    for (topic <- sourceTopics) {
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
      sourceTopics,
      bufferSizes,
      bufferOverlapSizes,
      streamDestinationPort,
      destinationTopic,
      operation)

    logger.log(Level.INFO, "Stream processing node configurations created successfully.")
  }

  def startStream(): Unit = {
    stopStreamFlag = false

    val processorConfigs: ListBuffer[MutableMap[String, Any]] = streamAuditor.validate(topicMap, intermediatePort)
    logger.log(Level.FINE, "Stream processors are being created. \nStream processor configs: " + processorConfigs.toString())

/***
    def createProcessor(processorConfig: MutableMap[String, Any]): StreamingNode = {
      logger.log(Level.FINE, "createProcessor method invoked.")
      processorConfig.get("transformation") match {
        case Some(operation: Transformation) => {
          logger.log(Level.FINE, "New stream processor being created with transformation object")
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
          logger.log(Level.INFO, "New stream processor created with transformation object. Stream processor object: " + processor.toString())
          processor
        }
        case Some(operation: String) => {
          logger.log(Level.FINE, "New stream processor being created with transformation name")
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
          logger.log(Level.INFO, "New stream processor created with transformation object. Stream processor object: " + processor.toString())
          processor
        }
        case _: Throwable => {
          logger.log(Level.WARNING, "Unrecognized transformation type")
          throw new Exception("Unrecognized transformation type")
        }
      }
    }
***/

    poolSize = processorConfigs.size
    executorService = Executors.newFixedThreadPool(poolSize)

    logger.log(Level.FINE, "Steam processors are being started.")
    for (processorConfig <- processorConfigs) {
      // logger.log(Level.FINE, "Stream processor config: " + processorConfig.toString())
      executorService.submit(new Runnable {
        override def run(): Unit = {
          //          val processor: StreamingNode = createProcessor(processorConfig)
          logger.log(Level.FINE, "New streaming node being created based on its configuration")
          logger.log(Level.FINE, "Stream processor so far: " + processors.toString())

          val processor: StreamingNode = new StreamingNode(logger)
          logger.log(Level.FINE, "Stream processor has been created: " + processor.toString())
          processors += processor

          logger.log(Level.FINE, "Processor's initSub being called")
          processorConfig.get("transformation") match {
            case Some(operation: Transformation) => {
              logger.log(Level.FINE, "Stream processor initSub being called")
              processor.initSub(
                context,
                processorConfig.get("receivePorts") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("receiveTopics") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("bufferSizes") match { case Some(list: ListBuffer[Integer]) => list },
                processorConfig.get("bufferOverlapSizes") match { case Some(list: ListBuffer[Integer]) => list },
                receiveTimeout,
                operation)
              logger.log(Level.FINE, "Stream processor initSub called successfully")
              logger.log(Level.INFO, "New stream processor initialized with transformation object. Stream processor object: " + processor.toString())
            }
            case Some(operation: String) => {
              // val processor: StreamingNode = new StreamingNode(logger)
              logger.log(Level.FINE, "Stream processor initSub being called")
              processor.initSub(
                context,
                processorConfig.get("receivePorts") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("receiveTopics") match { case Some(list: ListBuffer[String]) => list },
                processorConfig.get("bufferSizes") match { case Some(list: ListBuffer[Integer]) => list },
                processorConfig.get("bufferOverlapSizes") match { case Some(list: ListBuffer[Integer]) => list },
                receiveTimeout,
                createTransformation(operation.toString, List(), Map[String, String]()))
              logger.log(Level.FINE, "Stream processor initSub called successfully")
              logger.log(Level.INFO, "New stream processor initialized with transformation string. Stream processor object: " + processor.toString())
            }
            case _: Throwable => {
              logger.log(Level.WARNING, "Unrecognized transformation type")
              throw new Exception("Unrecognized transformation type")
            }
          }

          logger.log(Level.FINE, "Stream processor initPub being called")
          processor.initPub(
            context,
            processorConfig.get("sendPort") match { case Some(list: String) => list },
            processorConfig.get("sendTopic") match { case Some(list: String) => list })
          logger.log(Level.FINE, "Stream processor initPub called successfully")
          // logger.log(Level.INFO, "Stream processor - number of input buffers: " + processor.inputBuffers.length)

          breakable {
            while ((!Thread.currentThread().isInterrupted()) && (!stopStreamFlag)) {
              try {
                logger.log(Level.FINE, "Stream processor waiting to receive data")
                processor.receive()
              } catch {
                case e: ZMQException if ZMQ.Error.ETERM == e.getErrorCode() => {
                  //              case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode
                  logger.log(Level.WARNING, "Stream processors interrupted via ZMQException")
                  Thread.currentThread.interrupt()
                  break
                }
                case e: InterruptedException => {
                  logger.log(Level.WARNING, "Stream processor service interrupted via InterruptedException")
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
              logger.log(Level.FINE, "Waiting for interrupt or stopStreamFlag")
              // logger.log(Level.INFO, "Thread interrupt status: " + Thread.currentThread().isInterrupted())
            }
          }

          logger.log(Level.FINE, "Stream processor clearSub being called")
          processor.clearSub()
          logger.log(Level.FINE, "Stream processor clearSub called successfully")

          logger.log(Level.FINE, "Stream processor clearPub being called")
          //          processor.clear()
          processor.clearPub()
          logger.log(Level.FINE, "Stream processor clearPub called successfully")

          logger.log(Level.INFO, "Stream processor " + processor.toString() + " has been cleared.")
        }
      })
    }

    logger.log(Level.INFO, "EpiData stream started successfully.")
  }

  def stopStream(): Unit = {
    //_runStream = false
    logger.log(Level.FINE, "Streams are being stopped")

    try {
      stopStreamFlag = true
      logger.log(Level.FINE, "stopStreamFlag set to true")
      Thread.sleep(1000)

      logger.log(Level.FINE, "context is being terminated")
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
