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
import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.collection.mutable.ListBuffer

class EpidataLiteStreamingContext {
  var startPort: Integer = 5551
  var cleansedEndPort: Integer = 5554
  var summaryEndPort: Integer = 5554
  var dynamicEndPort: Integer = 5554
  var processors: ListBuffer[StreamingNode] = _
  var _runStream: Boolean = _
  var context: ZMQ.Context = _
  val receiveTimeout: Integer = -1
  var topicMap: MutableMap[String, Integer] = _
  var intermediatePort: Integer = 5554
  val logger = Logger.getLogger("Epidata lite logger")
  logger.setLevel(Level.FINE)
  logger.addHandler(new ConsoleHandler)

  //adding custom handler
  private val conf = ConfigFactory.parseResources("sqlite-defaults.conf")
  private val basePath = new java.io.File(".").getAbsoluteFile().getParentFile().getParent()
  private val logFilePath = basePath + "/log/" + conf.getString("spark.epidata.SQLite.logFileName")
  println("log file path: " + logFilePath)

  val fileHandler = new FileHandler("logFilePath")
  logger.addHandler(fileHandler)

  //default bufferSize based on configuration settings
  var bufferSize: Integer = conf.getInt("spark.epidata.streamDefaultBufferSize")

  def init(): Unit = {
    //ec.start_streaming()
    context = ZMQ.context(1)
    _runStream = true
    processors = ListBuffer()
    topicMap = MutableMap[String, Integer]()
    topicMap.put("measurements_original", startPort)
    topicMap.put("measurements_cleansed", cleansedEndPort)
    topicMap.put("measurements_summary", summaryEndPort)
    topicMap.put("measurements_dynamic", dynamicEndPort)
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
    //.log(Level.INFO, "streamDestinationPort: ", streamDestinationPort)

    println("\nEnter 'Q' to create processor for stream 6")
    while ((StdIn.readChar()).toLower.compare('q') != 0) {
      println("Continuing streaming. Enter 'Q' to stop streaming.")
    }

    processors += (new StreamingNode()).init(
      context,
      streamSourcePort,
      sourceTopic,
      buffersizes,
      streamDestinationPort,
      destinationTopic,
      receiveTimeout,
      operation)
    //logger.log(Level.INFO, "processors: ", processors)
  }

  def startStream(): Unit = {
    //println("---------Start Stream called-------" + processors.length)

    processors.reverse

    //iterate through processors arraylist backwards creating thread

    //println("number of processors: " + processors.size)
    for (processor <- processors) {
      Executors.newSingleThreadExecutor.execute(new Runnable {
        override def run(): Unit = {
          //println("processor started in new thread. runstream value - " + _runStream)
          while (_runStream) {

            processor.receive()
            processor.publish()

          }

          //println("while loop exited")
          //println("clearing processor: " + processor)
          processor.clear()

          //println("completing thread execution")
        }
      })
    }
    //logger.log(Level.INFO, "startstream successfully: ", processors)
  }

  def stopStream(): Unit = {
    _runStream = false
    // process needs to be inturrupted externally
    println("streams being stopped")
    //    for (processor <- processors) {
    //      println("clearing processor: " + processor)
    //      processor.clear()
    //    }
    //logger.log(Level.INFO, "stopstream successfully")
  }

  def printSomething(bar: String): String = {
    val s = "py4j connection working fine "
    s
  }

  def testUnit(): Unit = {
    print("testing unit")
  }

}
