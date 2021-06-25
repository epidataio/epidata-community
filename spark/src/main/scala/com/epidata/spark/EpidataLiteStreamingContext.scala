package com.epidata.spark

import java.util
import java.util.concurrent.Executors

import org.zeromq.ZMQ
import com.epidata.spark.ops.{ FillMissingValue, Identity, MeasStatistics, OutlierDetector, Transformation }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import scala.collection.mutable.{ Map => MutableMap }
import scala.io.StdIn
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

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`

case class Message(topic: Object, key: Object, value: Object)

class EpidataLiteStreamingContext {
  var startPort: Integer = 5551
  var endPort: Integer = 5552
  var processors: Array[StreamingNode] = _
  var _runStream: Boolean = _
  var context: ZMQ.Context = _
  val receiveTimeout: Integer = -1
  var topicMap: MutableMap[String, Integer] = _
  var intermediatePort: Integer = 5553
  val logger = Logger.getLogger("Epidata lite logger")
  logger.setLevel(Level.FINE)
  logger.addHandler(new ConsoleHandler)
  //adding custom handler
  //  val fileHandler = new FileHandler("users/desktop/logger/logger.log")
  //  logger.addHandler(fileHandler)
  var bufferSize: Integer = 2 //config setting

  def init(): Unit = {
    //ec.start_streaming()
    context = ZMQ.context(1)
    _runStream = true
    processors = Array[StreamingNode]()
    topicMap = MutableMap[String, Integer]()
    topicMap.put("measurements_original", startPort)
    topicMap.put("measurements_cleansed", endPort)
    topicMap.put("measurements_summary", endPort)
  }

  def createTransformations(opName: String, meas_names: List[String], params: Map[String, String]): Transformation = {
    //println("Transformation being created")

    // create and return a transformation object
    opName match {
      case "Identity" => new Identity()

      //case "FillMissingValue" => new FillMissingValue(meas_names, params.get("method").getOrElse("rolling"), params.get("s"))
      //case "OutlierDetector" => new OutlierDetector("meas_value", params.get("method"))
      //case "MeasStatistics" => new MeasStatistics(meas_names, "standard")
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
    //    println("Create Stream. Source Topic: " + sourceTopic + ". Destination Topic: " + destinationTopic + ". Transformation: " + operation)
    //---------------------------logger--------------------------------------------
    //LogManager.getLogManager.readConfiguration(new FileInputStream("mylogging.properties"))
    //    val logger = Logger.getLogger("Epidata lite logger")

    logger.log(Level.INFO, "sourcetopic:  " + sourceTopic)
    logger.log(Level.INFO, "destinationTopic:  " + destinationTopic)
    logger.log(Level.INFO, "transformation:  " + operation)

    //-------------------------------------------------------

    val streamSourcePort = topicMap.get(sourceTopic) match {
      case Some(port) => port.toString
      case None => throw new IllegalArgumentException("Source Topic is not recognized.")
    }
    logger.log(Level.INFO, "streamSourcePort: ", streamSourcePort)

    topicMap.get(destinationTopic) match {
      case None => {
        topicMap.put(destinationTopic, intermediatePort)
        intermediatePort += 1
        //println("new destination topic - port added")
      }
      case _ => {
        println("destination topic - port exists")
      }
    }

    val streamDestinationPort = topicMap.get(destinationTopic) match {
      case Some(port) => port.toString
      case None => throw new IllegalArgumentException("Destination Topic is not recognized.")
    }
    logger.log(Level.INFO, "streamDestinationPort: ", streamDestinationPort)

    processors :+= (new StreamingNode()).init(context, streamSourcePort, streamDestinationPort, sourceTopic, destinationTopic, receiveTimeout, operation)
    logger.log(Level.INFO, "processors: ", processors)
    //println("Source port: " + streamSourcePort + ", destination port: " + streamDestinationPort)
    //println("Processors: " + processors)
  }

  def startStream(): Unit = {
    //println("Start Stream called")

    processors.reverse

    //iterate through processors arraylist backwards creating thread

    //println("number of processors: " + processors.size)
    for (processor <- processors) {
      Executors.newSingleThreadExecutor.execute(new Runnable {
        override def run(): Unit = {
          println("processor started in new thread. runstream value - " + _runStream)
          while (_runStream) {
            //println("calling processor receive method")
            //println("processor ready to receive: " + processor)
            processor.receive()
            //println("processor received by " + processor)
          }

          //println("while loop exited")
          //println("clearing processor: " + processor)
          processor.clear()

          //println("completing thead execution")
        }
      })
    }
    logger.log(Level.INFO, "startstream successfully: ", processors)
  }

  def stopStream(): Unit = {
    _runStream = false
    // process needs to be inturrupted externally
    println("streams being stopped")
    //    for (processor <- processors) {
    //      println("clearing processor: " + processor)
    //      processor.clear()
    //    }
    logger.log(Level.INFO, "stopstream successfully")
  }

  def printSomething(bar: String): String = {
    val s = "py4j connection working fine "
    s
  }

  def testUnit(): Unit = {
    print("testing unit")
  }

}
