/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service
import java.util.concurrent.Executors
import org.json.simple.{ JSONArray, JSONObject }
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import com.fasterxml.jackson.databind.JsonMappingException
import play.api.Configuration

object ZMQService {
  var pullPort: String = _
  var subPort: String = _
  var _run: Boolean = true
  var sink = ZMQDataSink

  /**
   * ZMQ DataSink as a thread
   */
  def startThreads(config: Configuration): Unit = {
    //println("ZMQService startThreads called")

    pullPort = config.getOptional[Int]("queue.servers").get.toString
    subPort = (config.getOptional[Int]("queue.servers").get + 2).toString

    sink = ZMQDataSink.init(pullPort, subPort)

    try {
      // Pull Thread - Original Data
      Executors.newSingleThreadExecutor.execute(new Runnable {
        override def run(): Unit = {
          println("ZMQ datasink pull running in a new thread.")

          while (_run) {
            val rawData: JMap[String, String] = sink.pull()
            //            val rawData: Map[String, Object] = sink.pull()
            //println("ZMQ datasink Pulled rawData: " + rawData + "\n")

            try {
              Configs.measurementClass match {
                case com.epidata.lib.models.AutomatedTest.NAME => {
                  models.AutomatedTest.insertRecordFromZMQ(rawData.get("value"))
                  println("inserted AutomatedTest rawData")
                }
                case com.epidata.lib.models.SensorMeasurement.NAME => {
                  models.SensorMeasurement.insertRecordFromZMQ(rawData.get("value"))
                  println("inserted SensorMeasurement rawData")
                }
                case _ =>
              }
            } catch {
              case e: JsonMappingException => throw new Exception(e.getMessage)
              case _: Throwable => throw new Exception("Error while insert data to database from data sink service")
            }
          }
          println("Stopping ZMQDataSink for Pull")
          sink.clearPull(pullPort, subPort)
        }
      })

      // Subscribe Thread - Cleansed Data
      Executors.newSingleThreadExecutor.execute(new Runnable {
        override def run(): Unit = {
          //sink = ZMQDataSink.init(pullPort, subPort)
          println("ZMQ datasink subscribe started in a new thread.")

          while (_run) {
            val cleansedData: JMap[String, String] = sink.sub()
            //          val cleansedData: Map[String, Object] = sink.sub()
            //println("ZMQ datasink Subscribed cleansedData: " + cleansedData)

            try {
              Configs.measurementClass match {
                case com.epidata.lib.models.AutomatedTest.NAME => {
                  models.AutomatedTest.insertCleansedRecordFromZMQ(cleansedData.get("value"))
                  //println("inserted AutomatedTest cleansedData")
                }
                case com.epidata.lib.models.SensorMeasurement.NAME => {
                  models.SensorMeasurement.insertCleansedRecordFromZMQ(cleansedData.get("value"))
                  //println("inserted SensorMeasurement cleansedData")
                }
                case _ =>
              }
            } catch {
              case e: JsonMappingException => throw new Exception(e.getMessage)
              case _: Throwable => throw new Exception("Error while insert data to database from data sink service")
            }
          }
          println("Stopping ZMQDataSink for subscriber")
          sink.clearSub(pullPort, subPort)
        }
      })

    } catch {
      case e: Throwable => throw new Exception(e.getMessage)
    }
  }

  def stop(): Unit = {
    println("Stopping ZMQService")
    _run = false
    if (sink != null) {
      println("called ZMQ data sink clear")
      //      sink.clear(pullPort, subPort)
    }
  }

}
