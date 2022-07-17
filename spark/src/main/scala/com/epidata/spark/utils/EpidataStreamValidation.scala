/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.spark

import com.epidata.spark.ops.Transformation
import org.json4s.DefaultFormats
import org.json4s.JsonDSL.int2jvalue

import scala.collection.mutable.{ HashMap, ListBuffer, Map => MutableMap }
import scala.io.StdIn

class EpidataStreamValidation {
  //Stores all StreamingNode configs that need validatoring before init.
  var processorConfigs: ListBuffer[MutableMap[String, Any]] = _
  //Stores all the topics and whether theyre loose or closed.
  //var streamStatus: HashMap[String, Boolean] = _
  var looseSource: HashMap[String, Boolean] = _
  var looseDestination: HashMap[String, Boolean] = _
  var numOfProcessors: Integer = _
  var topicMap: MutableMap[String, Integer] = _
  var intermediatePort: Integer = _

  def init(): Unit = {
    processorConfigs = ListBuffer[MutableMap[String, Any]]()
    //streamStatus = HashMap[String, Boolean]()
    looseSource = HashMap[String, Boolean]()
    looseDestination = HashMap[String, Boolean]()
    numOfProcessors = 0
  }

  def addProcessor(
    receivePorts: ListBuffer[String],
    receiveTopics: ListBuffer[String],
    bufferSizes: ListBuffer[Integer],
    sendPort: String,
    sendTopic: String,
    transformation: Transformation): Unit = {

    processorConfigs += HashMap(
      "receivePorts" -> receivePorts,
      "receiveTopics" -> receiveTopics,
      "bufferSizes" -> bufferSizes,
      "sendPort" -> sendPort,
      "sendTopic" -> sendTopic,
      "transformation" -> transformation)

    numOfProcessors += 1
  }

  def validate(topicMap: MutableMap[String, Integer], intermediatePort: Integer): ListBuffer[MutableMap[String, Any]] = {
    this.topicMap = topicMap
    this.intermediatePort = intermediatePort

    /*
    All validations take place here
    */
    looseStreamValidation()

    for (processor <- processorConfigs) {
      //println("Operation: " + processor.get("transformation").getClass + "\n")
    }

    processorConfigs
  }

  def looseStreamValidation() = {
    /*
    "measurements_original" is a loose topic until a streaming node is seen subscribing to it.
    */
    looseSource.put("measurements_original", true)

    for (processor <- processorConfigs) {
      val subTopics = processor.get("receiveTopics") match {
        case topic: Some[ListBuffer[String]] => topic.to[ListBuffer]
        case None => throw new IllegalArgumentException("Subscribe Topic is not recognized.")
      }
      //println("\n")
      for (topic <- subTopics(0)) {
        //println("65" + subTopics(0) + " === " + topic + " (" + topic.getClass + ")")
        if (looseSource.contains(topic)) {
          /*
           If the sub topic of current node matches a previously loose source pub topic:
           1. Update its value to false (the topic is no-longer loose)
           2. Add it to "looseDestination" as false.
           */
          looseSource.put(topic, false) //pub topic has destination node - no longer loose
          looseDestination.put(topic, false) //sub topic has a destination node
        } else {
          /*
            If the sub topic has no previously seen pub counterpart:
            1. Add it to "looseDestination" as true
            (this destination is not connected to the rest of the stream yet)
            */
          looseDestination.put(topic, true) //subscribe topic is not connected to a source
        }
      }
      val pubTopic = processor.get("sendTopic") match {
        case Some(topic: String) => topic
        case None => throw new IllegalArgumentException("Publish Topic is not recognized.")
      }
      if (looseDestination.contains(pubTopic)) {
        /*
         If this publish topic is publishing to a previously loose destination:
         1. Update the "looseDestination" value to false
         (the previously loose destination now has a port to listen to)
         2. Add to looseSource as false (this source has a destination stream)
         */
        looseDestination.put(pubTopic, false) //sub topic is connected to a topic
        looseSource.put(pubTopic, false) //pub topic has a sub topic listening
      } else {
        /*
         Adding a new pub topic with no sources listening yet:
         1. Add to looseSource as true
         (this topic has not been seen yet and must be tied to a destination)
         */
        looseSource.put(pubTopic, true)
      }
    }

    //println("----looseDestinations----: \n" + looseDestination)
    //("----looseSources----: \n" + looseSource)
    rectifyLooseStreams()
  }

  def rectifyLooseStreams() {
    var numOfLooseSources = 0
    //println("Line 115: " + looseSource.values)
    for (loose <- looseSource.values) {
      //println("Line 117: " + loose)
      if (loose) {
        numOfLooseSources += 1
      }
    }
    //var cleansed:Boolean = false
    //var summary:Boolean = false
    var datasickTopics: Integer = 0;

    //println("Line 115: " + looseSource)
    if (looseSource.contains("measurements_cleansed")) {
      //println("Line 128")
      datasickTopics += 1
      //cleansed = true
    }

    if (looseSource.contains("measurements_summary")) {
      //println("Line 134")
      datasickTopics += 1
      //summary = true
    }

    if (numOfLooseSources > datasickTopics) {
      val looseSources: ListBuffer[String] = ListBuffer()
      //println("LINE 137 rectifying the stream.")

      var cleansedCounter = 0
      var summaryCounter = 0

      for (i <- 0 until processorConfigs.size) {
        //println("Line 153 Topic: " + processorConfigs(i).get("sendTopic"))
        val pubTopic = processorConfigs(i).get("sendTopic") match {
          case Some(topic: String) => topic
          case None => {
            throw new IllegalArgumentException("Publish Topic is not recognized.")
          }
        }
        //println("Line 157 Topic: " + looseSource.get(pubTopic))
        val loose = looseSource.get(pubTopic) match { case Some(value: Boolean) => value }
        if (pubTopic.equals("measurements_cleansed")) {
          //println("First if")
          val uniqueBuffer = "measurements_cleansed" + cleansedCounter
          processorConfigs(i).put("sendTopic", uniqueBuffer)
          topicMap.put(uniqueBuffer, intermediatePort)
          processorConfigs(i).put("sendPort", intermediatePort.toString)
          intermediatePort += 1
          cleansedCounter += 1
        } else if (pubTopic.equals("measurements_summary")) {
          //println("Second if")
          val uniqueBuffer = "measurements_summary" + summaryCounter
          processorConfigs(i).put("sendTopic", uniqueBuffer)
          topicMap.put(uniqueBuffer, intermediatePort)
          processorConfigs(i).put("sendPort", intermediatePort.toString)
          intermediatePort += 1
          summaryCounter += 1
        } else if (loose) {
          //println("Third if")
          looseSources.append(pubTopic)
          //          topicMap.put(pubTopic, intermediatePort)
          //          intermediatePort += 1
        }
      }

      //println("getNewNodeConfigs Params: " + cleansedCounter + "\n" + summaryCounter + "\n" + looseSources)

      val newNodes = getNewNodeConfigs(cleansedCounter, summaryCounter, looseSources)

      var topic: Integer = 0
      for (newNodeTopics <- newNodes) {
        topic += 1
        // println(newNodeTopics + "\n")
        val newNodePorts: ListBuffer[String] = ListBuffer()
        for (topic <- newNodeTopics) {
          if (topicMap.get(topic) != None) {
            newNodePorts.append(topicMap.get(topic).toString.replace("Some(", "").dropRight(1))
          } else {
            throw new IllegalArgumentException("Source Topic is not recognized.")
          }
        }

        if (topic == 1) {
          processorConfigs += HashMap(
            "receivePorts" -> newNodePorts,
            "receiveTopics" -> newNodeTopics,
            "bufferSizes" -> ListBuffer(0),
            "sendPort" -> "5552",
            "sendTopic" -> "measurements_cleansed",
            "transformation" -> "Identity")
        } else if (topic == 2) {
          processorConfigs += HashMap(
            "receivePorts" -> newNodePorts,
            "receiveTopics" -> newNodeTopics,
            "bufferSizes" -> ListBuffer(0),
            "sendPort" -> "5553",
            "sendTopic" -> "measurements_summary",
            "transformation" -> "Identity")
        } else {
          processorConfigs += HashMap(
            "receivePorts" -> newNodePorts,
            "receiveTopics" -> newNodeTopics,
            "bufferSizes" -> ListBuffer(0),
            "sendPort" -> "5554",
            "sendTopic" -> "measurements_dynamic",
            "transformation" -> "Identity")
        }
      }
    }

    looseDestination.foreach {
      case (key, value) => if (value == true) throw new RuntimeException("'" + key + "' listening to nonexistant topic.")
    }
  }

  def getNewNodeConfigs(cleansedCounter: Integer, summaryCounter: Integer, looseSources: ListBuffer[String]): ListBuffer[ListBuffer[String]] = {
    val newNodes: ListBuffer[ListBuffer[String]] = ListBuffer[ListBuffer[String]]()

    val newCleansedNodeTopics: ListBuffer[String] = ListBuffer()
    val newSummaryNodeTopics: ListBuffer[String] = ListBuffer()
    val newDynamicNodeTopics: ListBuffer[String] = ListBuffer()

    for (i <- 0 until cleansedCounter) {
      newCleansedNodeTopics.append("measurements_cleansed" + i)
    }

    for (i <- 0 until summaryCounter) {
      newSummaryNodeTopics.append("measurements_summary" + i)
    }

    for (looseSource <- looseSources) {
      /*
      Get the processorConfigs for current topic and inspect Transformation object here
       */
      newDynamicNodeTopics.append(looseSource)
    }

    newNodes.append(newCleansedNodeTopics)
    newNodes.append(newSummaryNodeTopics)
    newNodes.append(newDynamicNodeTopics)

    newNodes
  }
}
