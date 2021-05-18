package com.epidata.spark

import java.util
import java.util.concurrent.Executors

import org.zeromq.ZMQ
import com.epidata.spark.ops.{ FillMissingValue, Identity, MeasStatistics, OutlierDetector, Transformation }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import scala.collection.mutable.Map
import scala.io.StdIn

import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`

case class Message(topic: Object, key: Object, value: Object)

class EpidataLiteStreamingContext {
  var startPort: Integer = 5551
  var endPort: Integer = 5552
  var processors: Array[StreamingNode.type] = _
  var _runStream: Boolean = _
  var context: ZMQ.Context = _
  val receiveTimeout: Integer = -1
  var topicMap: Map[String, Integer] = _
  var intermediatePort: Integer = 5553

  def init(): Unit = {
    //ec.start_streaming()
    context = ZMQ.context(1)
    _runStream = true
    processors = Array[StreamingNode.type]()
    topicMap = Map[String, Integer]()
    topicMap.put("measurements_original", startPort)
    topicMap.put("measurements_cleansed", endPort)
    topicMap.put("measurements_summary", endPort)
  }

  def createTransformations(opName: String, meas_names: List[String], params: Map[String, String]): Transformation = {
    println("Transformation being created")

    // create and return a transformation object
    opName match {
      case "Identity" => new Identity()
      //case "FillMissingValue" => new FillMissingValue(meas_names, "rolling", 3)
      //case "OutlierDetector" => new OutlierDetector("meas_value", "quartile")
      //case "MeasStatistics" => new MeasStatistics(meas_names, "standard")
      case _ => new Identity()
    }
  }

  //  def createCustomTransformation(opName: String, transformation: Transformation): Transformation {
  //    transformation
  //  }

  //  def createStream(sourceTopic: String, destinationTopic: String, operations: Array[Transformation]): Unit {
  //    processors.add(new StreamingNode(context, port, port, ))
  //  }

  def createStream(sourceTopic: String, destinationTopic: String, operation: Transformation): Unit = {
    println("Create Stream called")
    println("source topic: " + sourceTopic)
    println("destinationTopic: " + destinationTopic)
    println("operation: " + operation)

    val streamSourcePort = topicMap.get(sourceTopic) match {
      case Some(port) => port.toString
      case None => throw new IllegalArgumentException("Source Topic is not recognized.")
    }
    println("streamSourcePort: " + streamSourcePort)

    topicMap.get(destinationTopic) match {
      case None => {
        topicMap.put(destinationTopic, intermediatePort)
        intermediatePort += 1
        println("new destination topic - port added")
      }
      case _ => {
        println("detination topic - port exists")
      }
    }

    val streamDestinationPort = topicMap.get(destinationTopic) match {
      case Some(port) => port.toString
      case None => throw new IllegalArgumentException("Destination Topic is not recognized.")
    }
    println("streamDestinationPort: " + streamDestinationPort)

    //    if (destinationTopic.equals("measurements_cleansed") || destinationTopic.equals("measurements_summary")) {
    println("Ready to create proessor cleansed. Enter 'Q' to continue.")
    while ((StdIn.readChar()).toLower.compare('q') != 0) {
      println("Ready to create proessor cleansed. Enter 'Q' to continue.")
    }

    processors :+= StreamingNode.init(context, streamSourcePort, streamDestinationPort, sourceTopic, destinationTopic, receiveTimeout, operation)

    println("created proessor cleansed. Enter 'Q' to continue.")
    while ((StdIn.readChar()).toLower.compare('q') != 0) {
      println("created proessor cleansed. Enter 'Q' to continue.")
    }
    //    }
    //    else if (processors.size == 0) {
    //      processors :+= StreamingNode.init(context, port.toString, (port + 2).toString, sourceTopic, destinationTopic, receiveTimeout, operation)
    //    port += 1
    //    }
    //     else if (destinationTopic.equals("measurements_substituted") || destinationTopic.equals("measurement_cleansed") || destinationTopic.equals("measurements_summary")) {
    //      processors :+= StreamingNode.init(context, port.toString, "5552", sourceTopic, destinationTopic, receiveTimeout, operation)
    //    }
    //    else {
    //      while ((StdIn.readChar()).toLower.compare('q') != 0) {
    // print data
    //        println("Ready to create intermediate proessor. Enter 'Q' to continue.")
    //      }
    //      processors :+= StreamingNode.init(context, intermediatePort.toString, endPort.toString, sourceTopic, destinationTopic, receiveTimeout, operation)
    //      while ((StdIn.readChar()).toLower.compare('q') != 0) {
    // print data
    //        println("Created intermediate proessor. Enter 'Q' to continue.")
    //      }
    //    }
    //    port += 1

    //    while ((StdIn.readChar()).toLower.compare('q') != 0) {
    // print data
    //      println("Created processor. Enter 'Q' to continue.")
    //    }

    println("source port: " + streamSourcePort + ", destination port: " + streamDestinationPort)
    println("processor: " + processors)
  }

  def startStream(): Unit = {
    println("Start Stream called")

    processors.reverse

    //iterate through processors arraylist backwards creating thread

    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        println("processor started in new thread. runstream value - " + _runStream)
        while (_runStream) {
          println("calling processor receive method")
          println("number of processors: " + processors.size)
          for (processor <- processors) {
            println("processor ready to receive: " + processor)
            processor.receive()
            println("processor reeived by " + processor)
          }
          //          Thread.sleep(loopTime)
        }

        println("while loop exited")
        for (processor <- processors) {
          println("clearing processor: " + processor)
          processor.clear()
        }

        println("completing thead execution")
      }
    })
  }

  def stopStream(): Unit = {
    _runStream = false //dont think you can stop thread internally in runtime, it needs to be prescripted
    // process needs to be inturrupted externally
    println("streams being stopped")
    //    for (processor <- processors) {
    //      println("clearing processor: " + processor)
    //      processor.clear()
    //    }
  }

}
