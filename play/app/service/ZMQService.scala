/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service
import java.util.concurrent.Executors

import com.fasterxml.jackson.databind.JsonMappingException
import play.api.Configuration

object ZMQService {

  def startThreads(config: Configuration): Unit = {
    //    /**
    //     * ZMQ Stream as a thread
    //     */
    //    val stream = ZMQStream.init((config.getOptional[Int]("queue.servers").get + 1).toString, (config.getOptional[Int]("queue.servers").get + 1).toString)
    //    Executors.newSingleThreadExecutor.execute(new Runnable {
    //      override def run(): Unit = {
    //
    //        while (true) {
    //          stream.receive()
    //          stream.publish(ZMQInit.streamQueue.dequeue())
    //        }
    //      }
    //    })
    /**
     * ZMQ DataSink as a thread
     */
    val sink = ZMQDataSink.init(config.getOptional[Int]("queue.servers").get.toString, (config.getOptional[Int]("queue.servers").get + 2).toString)
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {

        while (true) {
          val rawData: Map[String, String] = sink.pull()
          val processedData: Map[String, String] = sink.sub()
          try {
            Configs.measurementClass match {
              case com.epidata.lib.models.AutomatedTest.NAME => {
                models.AutomatedTest.insertRecordFromZMQ(rawData("value"))
                models.AutomatedTest.insertRecordFromZMQ(processedData("value"))
              }
              case com.epidata.lib.models.SensorMeasurement.NAME => {
                models.SensorMeasurement.insertRecordFromZMQ(rawData("value"))
                models.SensorMeasurement.insertRecordFromZMQ(processedData("value"))
              }
              case _ =>
            }
          } catch {
            case e: JsonMappingException => throw new Exception(e.getMessage)
            case _: Throwable => throw new Exception("Error while insert data to cassandra from data sink service")
          }
        }
      }
    })
  }

}
