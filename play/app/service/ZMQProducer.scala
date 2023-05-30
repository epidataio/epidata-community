/*
 * Copyright (c) 2020-2022 EpiData, Inc.
*/

package service

import org.json.simple.{ JSONArray, JSONObject }
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import controllers.Assets.JSON
import com.epidata.lib.models.util.JsonHelpers._
import com.epidata.lib.models.util.Message
import com.epidata.lib.models.{ AutomatedTest => BaseAutomatedTest, AutomatedTestCleansed => BaseAutomatedTestCleansed, AutomatedTestSummary => BaseAutomatedTestSummary }
import com.epidata.lib.models.{ SensorMeasurement => BaseSensorMeasurement, SensorMeasurementCleansed => BaseSensorMeasurementCleansed, SensorMeasurementSummary => BaseSensorMeasurementSummary }
import play.api.libs.json.Json
import org.zeromq.ZMQ
import play.api.Logger

object ZMQProducer {
  var pushSocket: ZMQ.Socket = _
  var pubSocket: ZMQ.Socket = _

  var pushPort: String = _
  var pubPort: String = _

  val logger: Logger = Logger(this.getClass())

  def init(context: ZMQ.Context, pushPort: String, pubPort: String): ZMQProducer.type = {
    this.pushPort = pushPort
    this.pubPort = pubPort

    //using context to create PUSH and PUB models and binding them to sockets
    pushSocket = context.socket(ZMQ.PUSH)
    pushSocket.connect("tcp://127.0.0.1:" + this.pushPort)

    pubSocket = context.socket(ZMQ.PUB)
    pubSocket.bind("tcp://127.0.0.1:" + this.pubPort)
    this
  }

  /**
   * Encapsulating key and value as Message object and pushing data to DataSink
   */
  def push(key: String, value: String): Unit = {
    //create the Json message with (key, value) pair
    val message: String = messageToJson(Message(key, value))

    //push the Json message
    pushSocket.send(message.getBytes(ZMQ.CHARSET), 0)
    logger.info("Message pushed: " + message)
  }

  /**
   * Encapsulating key and value as Message object and publishing data to Stream
   */
  def pub(topic: String, key: String, value: String): Unit = {
    //setting the topic as measurements_original
    pubSocket.sendMore(topic)

    //create the message
    val message: String = messageToJson(Message(key, value))

    //publish the message
    pubSocket.send(message.getBytes(ZMQ.CHARSET), 0)
    logger.info("Message published: " + message)
  }

  def clear(): Unit = {
    try {
      //pushSocket.send("$TERM")
      pushSocket.setLinger(0)
      // pushSocket.unbind("tcp://127.0.0.1:" + this.pushPort)
      pushSocket.unbind(pushSocket.getLastEndpoint())
      pushSocket.close()
    } catch {
      case e: Throwable => logger.error("Exception while closing ZMQ push socket" + e.getMessage)
    }

    try {
      //pubSocket.send("$TERM")
      pubSocket.setLinger(0)
      // pubSocket.unbind("tcp://127.0.0.1:" + this.pubPort)
      pubSocket.unbind(pubSocket.getLastEndpoint())
      pubSocket.close()
    } catch {
      case e: Throwable => logger.error("Exception while closing ZMQ Pub socket" + e.getMessage)
    }
  }

}
