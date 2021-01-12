/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service

import controllers.Assets.JSON
import play.api.libs.json.Json
import org.zeromq.ZMQ

object ZMQDataSink {
  var pullSocket: ZMQ.Socket = _
  var subSocket: ZMQ.Socket = _
  var forwardMessage: ZMQ.Socket = _

  def init(pushPort: String, pullPort: String): ZMQDataSink.type = {
    //creating ZMQ context which will be used for PUB and PUSH
    val context = ZMQ.context(1)

    //using context to create PUSH and PUB models and binding them to sockets
    pullSocket = context.socket(ZMQ.PULL)
    pullSocket.bind("tcp://127.0.0.1:" + pushPort)

    subSocket = context.socket(ZMQ.SUB)
    subSocket.connect("tcp://127.0.0.1:" + pullPort)
    subSocket.subscribe("measurements_substituted".getBytes(ZMQ.CHARSET))
    subSocket.subscribe("measurement_cleansed".getBytes(ZMQ.CHARSET))
    subSocket.subscribe("measurements_summary".getBytes(ZMQ.CHARSET))
    this
  }

  def pull(): Map[String, String] = {
    //val messageObject = new JSONObject(pullSocket.recvStr())
    //JSON.parse(pullSocket.recvStr()).collect{case map: Map[String, Any] => (map("topic"), map("key"), map("value"))}.get
    //JSON.toMap.asInstanceOf[Map[String, Int]]
    JSON.asInstanceOf
    //new Message(messageObject.get("topic"), messageObject.get("key"), messageObject.get("value"))
    (Json.parse(pullSocket.recvStr()) \ "key_value").as[Map[String, String]]
  }

  def sub() = {
    val topic = subSocket.recvStr()
    //val messageObject = new JSONObject(subSocket.recvStr())
    //JSON.parseFull(pullSocket.recvStr()).collect{case map: Map[String, Any] => (map("topic"), map("key"), map("value"))}.get
    //new Message(messageObject.get("topic"), messageObject.get("key"), messageObject.get("value"))
    (Json.parse(subSocket.recvStr()) \ "key_value").as[Map[String, String]]
  }

  def end(): Unit = {
    subSocket.close()
    subSocket.close()
  }
}
