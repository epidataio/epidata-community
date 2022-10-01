/*
 * Copyright (c) 2020-2022 EpiData, Inc.
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

class ZMQDynamicDataSink {
  var subSocket: ZMQ.Socket = _
  val subTopicDynamic: String = "measurements_dynamic"

  val logger: Logger = Logger(this.getClass())

  def init(context: ZMQ.Context, subPort: String): Unit = {
    //using context to create SUB model and binding it to socket
    subSocket = context.socket(ZMQ.SUB)
    subSocket.connect("tcp://127.0.0.1:" + subPort)

    subSocket.subscribe(subTopicDynamic.getBytes(ZMQ.CHARSET))
  }

  def sub(): (String, Message) = {
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
      subSocket.unsubscribe(subTopicDynamic.getBytes(ZMQ.CHARSET))
      subSocket.setLinger(1)
      // subSocket.disconnect("tcp://127.0.0.1:" + subPort)
      subSocket.disconnect(subSocket.getLastEndpoint())
      subSocket.close()
      println("Dynamic DataSink service closed successfully")
    } catch {
      case e: Throwable => println("Exception while closing DataSink sub service", e.getMessage)
    }
  }

}
