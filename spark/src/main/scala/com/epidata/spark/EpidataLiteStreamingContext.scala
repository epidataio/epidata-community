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
import java.util.logging.ConsoleHandler
import java.util.logging.FileHandler
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogManager
import java.util.logging.Logger
//--------------------------------------------
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable.ListBuffer

class EpidataLiteStreamingContext {
  var startPort: Integer = 5551
  var cleansedPort: Integer = 5552
  var summaryPort: Integer = 5553
  var dynamicPort: Integer = 5554
  var processors: ListBuffer[StreamingNode] = _
  val streamAuditor = new EpidataStreamValidation()
  var _runStream: Boolean = _
  var context: ZMQ.Context = _
  val receiveTimeout: Integer = -1
  var topicMap: MutableMap[String, Integer] = _
  var intermediatePort: Integer = 5555
  val logger = Logger.getLogger("Epidata lite logger")
  logger.setLevel(Level.FINE)
  logger.addHandler(new ConsoleHandler)

  private var poolSize: Int = 1
  private var executorService: ExecutorService = _

  //adding custom handler
  private val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  private val basePath = new java.io.File(".").getAbsoluteFile().getParentFile().getParent()
  private val logFilePath = basePath + "/log/" + conf.getString("spark.epidata.SQLite.logFileName")
  println("log file path: " + logFilePath)

  val fileHandler = new FileHandler("logFilePath")
  logger.addHandler(fileHandler)

  //default bufferSize based on configuration settings
  var bufferSize: Integer = conf.getInt("spark.epidata.streamDefaultBufferSize")

  def config(): Unit = {

  }

  def init(): Unit = {
    //ec.start_streaming()
    context = ZMQ.context(1)
    _runStream = true
    processors = ListBuffer()
    topicMap = MutableMap[String, Integer]()
    topicMap.put("measurements_original", startPort)
    topicMap.put("measurements_cleansed", cleansedPort)
    topicMap.put("measurements_summary", summaryPort)
    topicMap.put("measurements_dynamic", dynamicPort)
    streamAuditor.init()

    //    addShutdownHook()
  }

  def createTransformations(opName: String, meas_names: List[String], params: Map[String, Any]): Transformation = {
    //println("Transformation being created")

    // create and return a transformation object
    opName match {
      case "Identity" => new Identity()

      case "FillMissingValue" => new FillMissingValue(meas_names, params.getOrElse("method", "rolling").asInstanceOf[String], params.getOrElse("s", 3).asInstanceOf[Int])
      //case "OutlierDetector" => new OutlierDetector("meas_value", params.get("method"))
      case "MeasStatistics" => new MeasStatistics(meas_names, params.getOrElse("method", "standard").asInstanceOf[String])
      case _ => new Identity()
    }
  }

  /** Interface for Java and Python. */
  def createTransformations(
    opName: String,
    meas_names: java.util.List[String],
    params: java.util.Map[String, String]): Transformation = {
    import scala.collection.JavaConversions._
    val sBuffer = asScalaBuffer(meas_names)
    createTransformations(opName, sBuffer.toList, params.toMap)
  }

  def createStream(sourceTopic: String, destinationTopic: String, operation: Transformation): Unit = {
    createStream(ListBuffer(sourceTopic), ListBuffer(bufferSize), destinationTopic, operation)
  }

  def createStream(sourceTopic: ListBuffer[String], destinationTopic: String, operation: Transformation): Unit = {
    createStream(sourceTopic, ListBuffer(bufferSize), destinationTopic, operation)
  }

  def createStream(sourceTopic: String, buffersize: Integer, destinationTopic: String, operation: Transformation): Unit = {
    createStream(ListBuffer(sourceTopic), ListBuffer(buffersize), destinationTopic, operation)
  }

  def createStream(sourceTopic: ListBuffer[String], buffersize: Integer, destinationTopic: String, operation: Transformation): Unit = {
    createStream(sourceTopic, ListBuffer(buffersize), destinationTopic, operation)
  }

  def createStream(sourceTopic: String, buffersizes: ListBuffer[Integer], destinationTopic: String, operation: Transformation): Unit = {
    createStream(ListBuffer(sourceTopic), buffersizes, destinationTopic, operation)
  }

  def createStream(sourceTopic: ListBuffer[String], buffersizes: ListBuffer[Integer], destinationTopic: String, operation: Transformation): Unit = {
    //logger(Level.INFO, "sourcetopic:  " + sourceTopic)
    //logger.log(Level.INFO, "destinationTopic:  " + destinationTopic)
    //logger.log(Level.INFO, "transformation:  " + operation)
    //-------------------------------------------------------
    var streamSourcePort: ListBuffer[String] = ListBuffer()
    for (topic <- sourceTopic) {
      if (topicMap.get(topic) != None) {
        streamSourcePort += topicMap.get(topic).toString.replace("Some(", "").dropRight(1)
      } else {
        throw new IllegalArgumentException("Source Topic is not recognized.")
      }
    }

    //logger.log(Level.INFO, "streamSourcePort: ", streamSourcePort)

    topicMap.get(destinationTopic) match {
      case None => {
        topicMap.put(destinationTopic, intermediatePort)
        intermediatePort += 1
        println("new destination topic - port added")
      }
      case _ => {
        println("destination topic - port exists")
      }
    }

    val streamDestinationPort = topicMap.get(destinationTopic) match {
      case Some(port) => port.toString
      case None => throw new IllegalArgumentException("Destination Topic is not recognized.")
    }

    streamAuditor.addProcessor(
      streamSourcePort,
      sourceTopic,
      buffersizes,
      streamDestinationPort,
      destinationTopic,
      operation)
  }

  def startStream(): Unit = {
    val processorConfigs: ListBuffer[MutableMap[String, Any]] = streamAuditor.validate(topicMap, intermediatePort)
    for (processor <- processorConfigs) {
      processor.get("transformation") match {
        case Some(operation: Transformation) => {
          //println("!!@Operation is of type Object")
          processors += new StreamingNode().init(
            context,
            processor.get("receivePorts") match { case Some(list: ListBuffer[String]) => list },
            processor.get("receiveTopics") match { case Some(list: ListBuffer[String]) => list },
            processor.get("bufferSizes") match { case Some(list: ListBuffer[Integer]) => list },
            processor.get("sendPort") match { case Some(list: String) => list },
            processor.get("sendTopic") match { case Some(list: String) => list },
            receiveTimeout,
            operation)
        }
        case operation: Some[String] => {
          //println("!!@Operation is of type String")
          processors += new StreamingNode().init(
            context,
            processor.get("receivePorts") match { case Some(list: ListBuffer[String]) => list },
            processor.get("receiveTopics") match { case Some(list: ListBuffer[String]) => list },
            processor.get("bufferSizes") match { case Some(list: ListBuffer[Integer]) => list },
            processor.get("sendPort") match { case Some(list: String) => list },
            processor.get("sendTopic") match { case Some(list: String) => list },
            receiveTimeout,
            createTransformations(operation.toString, List(), Map[String, String]()))
        }
      }
    }
    processors.reverse

    poolSize = processors.size
    println("poolSize " + poolSize)
    executorService = Executors.newFixedThreadPool(poolSize)

    //println("Enter 'Q' to start streaming")
    //while ((StdIn.readChar()).toLower.compare('q') != 0) {
    //  println("Enter 'Q' to start streaming.")
    //}

    for (processor <- processors) {
      executorService.submit(new Runnable {
        override def run(): Unit = {
          breakable {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                processor.receive()
                processor.publish()
              } catch {
                case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode => {
                  Thread.currentThread.interrupt()
                  break
                  println("Processor service interrupted via ZMQException")
                }
                case e: InterruptedException => {
                  Thread.currentThread.interrupt()
                  break
                  println("Processor service interrupted via InterruptedException")
                }
                case e: JsonMappingException => throw new Exception(e.getMessage)
                case _: Throwable => throw new Exception("Error while receiving transforming and publishing stream data")
              }
            }
          }
          processor.clear()
        }
      })
    }

  }

  def stopStream(): Unit = {
    //_runStream = false
    println("Streams being stopped")
    try {
      executorService.shutdownNow()
      Thread.currentThread().interrupt();
    } catch {
      case e: InterruptedException =>
        println("InterruptedException during stream shutdown", e.getMessage)
        executorService.shutdownNow()
        Thread.currentThread().interrupt();
      case e: Throwable =>
        println("Exception during stream shutdown", e.getMessage)
    }
  }

  //  def addShutdownHook(): Unit = {
  //    Runtime.getRuntime().addShutdownHook(new Thread { () => stopStream() })
  //  }

  def printSomething(bar: String): String = {
    val s = "py4j connection working fine "
    s
  }

  def testUnit(): Unit = {
    print("testing unit")
  }

}
