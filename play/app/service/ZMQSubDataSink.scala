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

class ZMQSubDataSink {
  var context: ZMQ.Context = _
  var subSocket: ZMQ.Socket = _
  val subTopicOriginal: String = "measurements_original"
  val subTopicCleansed: String = "measurements_cleansed"
  val subTopicSummary: String = "measurements_summary"

  val logger: Logger = Logger(this.getClass())

  def init(context: ZMQ.Context, subPort: String): Unit = {
    println("ZMQSubDataSink initialized")

    //initializing ZMQ context which will be used for SUB
    this.context = context

    //using context to create SUB model and binding it to socket
    subSocket = context.socket(ZMQ.SUB)
    subSocket.connect("tcp://127.0.0.1:" + subPort)

    subSocket.subscribe(subTopicOriginal.getBytes(ZMQ.CHARSET))
    subSocket.subscribe(subTopicCleansed.getBytes(ZMQ.CHARSET))
    subSocket.subscribe(subTopicSummary.getBytes(ZMQ.CHARSET))
  }

  def sub(): Message = {
    //println("ZMQSubDataSink sub called.")
    try {
      val topic = subSocket.recvStr()
      //println("Sub topic: " + topic + "\n")
      val receivedString = subSocket.recvStr()
      println("Sub data: " + receivedString + "\n")
      val message = jsonToMessage(receivedString)
      message
    } catch {
      case e: Throwable => throw e
    }
  }

  def clear(subPort: String): Unit = {
    try {
      subSocket.unsubscribe(subTopicOriginal.getBytes(ZMQ.CHARSET))
      subSocket.unsubscribe(subTopicCleansed.getBytes(ZMQ.CHARSET))
      subSocket.unsubscribe(subTopicSummary.getBytes(ZMQ.CHARSET))
      subSocket.setLinger(1)
      subSocket.disconnect("tcp://127.0.0.1:" + subPort)
      subSocket.close()
      println("DataSink sub service closed successfully")
    } catch {
      case e: Throwable => println("Exception while closing DataSink sub service", e.getMessage)
    }
  }

}
