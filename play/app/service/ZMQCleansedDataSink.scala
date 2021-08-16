/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import org.json.simple.{ JSONArray, JSONObject }
import org.json.simple.parser.{ ParseException, JSONParser }
import com.epidata.lib.models.util.JsonHelpers._
import com.epidata.lib.models.util.Message
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import com.epidata.lib.models.{ AutomatedTest => BaseAutomatedTest, AutomatedTestCleansed => BaseAutomatedTestCleansed, AutomatedTestSummary => BaseAutomatedTestSummary }
import com.epidata.lib.models.{ SensorMeasurement => BaseSensorMeasurement, SensorMeasurementCleansed => BaseSensorMeasurementCleansed, SensorMeasurementSummary => BaseSensorMeasurementSummary }
import controllers.Assets.JSON
import play.api.libs.json.Json
import org.zeromq.ZMQ
import org.zeromq.ZMQException
import play.api.Logger

class ZMQCleansedDataSink {
  var context: ZMQ.Context = _
  var subSocket: ZMQ.Socket = _
  val subTopicCleansed: String = "measurements_cleansed"

  val logger: Logger = Logger(this.getClass())

  def init(context: ZMQ.Context, subPort: String): Unit = {
    println("ZMQCleansedDataSink initialized")

    //initializing ZMQ context which will be used for SUB
    this.context = context

    //using context to create SUB model and binding it to socket
    subSocket = context.socket(ZMQ.SUB)
    subSocket.connect("tcp://127.0.0.1:" + subPort)

    subSocket.subscribe(subTopicCleansed.getBytes(ZMQ.CHARSET))
  }

  def sub(): (String, Message) = {
    // println("ZMQSummaryDataSink sub called.")
    try {
      val topic = subSocket.recvStr()
      // println("Sub topic: " + topic + "\n")
      val receivedString = subSocket.recvStr()
      // println("Sub data: " + receivedString + "\n")
      val message = jsonToMessage(receivedString)
      (topic, message)
    } catch {
      case e: Throwable => throw e
    }
  }

  def clear(subPort: String): Unit = {
    try {
      subSocket.unsubscribe(subTopicCleansed.getBytes(ZMQ.CHARSET))
      subSocket.setLinger(1)
      subSocket.disconnect("tcp://127.0.0.1:" + subPort)
      subSocket.close()
      println("Cleansed DataSink service closed successfully")
    } catch {
      case e: Throwable => println("Exception while closing DataSink sub service", e.getMessage)
    }
  }

}
