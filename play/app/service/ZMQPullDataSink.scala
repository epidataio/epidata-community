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

class ZMQPullDataSink {
  var pullSocket: ZMQ.Socket = _
  var forwardMessage: ZMQ.Socket = _

  val logger: Logger = Logger(this.getClass())

  def init(context: ZMQ.Context, pullPort: String): Unit = {
    //using context to create PUSH and PUB models and binding them to sockets
    pullSocket = context.socket(ZMQ.PULL)
    pullSocket.bind("tcp://127.0.0.1:" + pullPort)
  }

  def pull(): Message = {
    try {
      val receivedString = pullSocket.recvStr()
      // println("Pull data: " + receivedString + "\n")
      val message: Message = jsonToMessage(receivedString)
      message
    } catch {
      case e: Throwable => throw e
    }
  }

  def clear(pullPort: String): Unit = {
    try {
      pullSocket.setLinger(1)
      // pullSocket.unbind("tcp://127.0.0.1:" + pullPort)
      pullSocket.unbind(pullSocket.getLastEndpoint())
      pullSocket.close()
    } catch {
      case e: Throwable => println("Exception while closing DataSink pull service", e.getMessage)
    }
  }

}
