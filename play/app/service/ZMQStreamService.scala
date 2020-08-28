package service

import controllers.Assets.JSON
import org.zeromq.ZMQ

case class Message(topic: String,
                   message: String)

object ZMQStreamService {
  var subSocket: ZMQ.Socket = _
  var forwardSocket: ZMQ.Socket = _

  def init (pubPort: String, forwardPort: String): ZMQStreamService.type = {
    val context = ZMQ.context(1)

    subSocket = context.socket(ZMQ.SUB)
    subSocket.connect("tcp://127.0.0.1:" + pubPort)
    subSocket.subscribe("measurements".getBytes(ZMQ.CHARSET))
    subSocket.subscribe("passBack".getBytes(ZMQ.CHARSET))

    forwardSocket = context.socket(ZMQ.PUB)
    forwardSocket.bind("tcp://127.0.0.1:" + forwardPort)
    this
  }

  def receive(sendTopic: String): Unit = {
    val topic = subSocket.recvStr() //measurements or passBack
    val message = subSocket.recvStr() //key value pair
    println("Subscribed Message: " + message + " Topic: " + topic)
    /**
     * A message is received either from ZMQProducer or ZMQForwardService within
     * StreamService. If it is from ZMQProducer the topic will be "measurements" and if
     * it is cycled data from within the StreamService it will have any topic the user
     * desires. However if the topic of the message is "save" (which will act as a reserved
     * topic) the data will be published to the dataSink topic and recieved by the ZMQDataSink
     */
    val msg: Message = process(topic, message)
    publish(msg.topic, msg.message)
//    if (sendTopic != "save") {
//      forwardSocket.sendMore("PassBack")
//      val msg: String = JSON.format(Message(sendTopic,message))
//      forwardSocket.send(msg.getBytes(), 0)
//    } else {
//      forwardSocket.sendMore("Processed")
//      val msg: String = JSON.format(Message(sendTopic,message))
//      forwardSocket.send(msg.getBytes(), 0)
//    }
//    /**
//     * publish the message again on forwardPort
//     * Mimics the Kafka broker service in a very bare bones way
//     */
//    val pubMessage = message //this line mimics any data processing that might take place before the data is passed on
//    //    val passonMessge = sparkSteaming(message)
//    //setting the topic as Publisher
//    forwardSocket.sendMore("PassBack")
//    //sending the message
//    val msg = String.format("Update %d" + pubMessage)
//    forwardSocket.send(msg.getBytes(), 0)
//    println(msg)
  }

  def publish(title: String, message: String): Unit = {
    if (title != "save") {
      forwardSocket.sendMore("PassBack")
      val msg: String = JSON.format(message)
      forwardSocket.send(msg.getBytes(), 0)
      println(msg)
    } else {
      forwardSocket.sendMore("Processed")
      val msg: String = JSON.format(message)
      forwardSocket.send(msg.getBytes(), 0)
      println(msg)
    }
//    /**
//     * publish the message again on forwardPort
//     */
//    val pubMessage = message //this line mimics any data processing that might take place before the data is passed on
//    //    val passonMessge = sparkSteaming(message)
//    //setting the topic as Publisher
//    forwardSocket.sendMore("PassBack")
//    //sending the message
//    val msg = String.format("Update %d" + pubMessage)
//    forwardSocket.send(msg.getBytes(), 0)
//    println(msg)
  }

  def process(topic: String, message: String) = {
    //iPython read write functions
    /**
     * Assumption is that the values of topic and msg were read to iPython and some data manipulation
     * has occured. User is also expected to write their desired topic for the message to "topic" variable
     */
    Message(topic,message)
  }
}
