/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service
import java.util.concurrent.{ Executors, ExecutorService, TimeUnit, Future }
import org.json.simple.{ JSONArray, JSONObject }
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
import com.epidata.lib.models.{ AutomatedTest => BaseAutomatedTest, AutomatedTestCleansed => BaseAutomatedTestCleansed, AutomatedTestSummary => BaseAutomatedTestSummary }
import com.epidata.lib.models.{ SensorMeasurement => BaseSensorMeasurement, SensorMeasurementCleansed => BaseSensorMeasurementCleansed, SensorMeasurementSummary => BaseSensorMeasurementSummary }
import com.fasterxml.jackson.databind.JsonMappingException
import play.api.Configuration
import com.epidata.lib.models.util.JsonHelpers._
import com.epidata.lib.models.util.Message
import org.zeromq.ZMQ
import org.zeromq.ZMQException
import scala.util.control.Breaks._
import play.api.Logger

object ZMQService {
  private var pullPort: String = _
  private var cleansedSubPort: String = _
  private var summarySubPort: String = _
  private var dynamicSubPort: String = _
  private var context: ZMQ.Context = _
  private var executorService: ExecutorService = _

  val logger: Logger = Logger(this.getClass())

  private val poolSize: Int = 4

  def init(context: ZMQ.Context, pullPort: String, cleansedSubPort: String, summarySubPort: String, dynamicSubPort: String): ZMQService.type = {
    this.executorService = Executors.newFixedThreadPool(this.poolSize)
    this.context = context
    this.pullPort = pullPort
    this.cleansedSubPort = cleansedSubPort
    this.summarySubPort = summarySubPort
    this.dynamicSubPort = dynamicSubPort
    this
  }

  /**
   * ZMQ DataSink as a thread
   */
  def start(): Unit = {
    try {
      // Pull Thread - Original Data
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val originalDataSink = new ZMQPullDataSink()
          originalDataSink.init(context, pullPort)

          breakable {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                val rawData: String = originalDataSink.pull().value

                Configs.measurementClass match {
                  case com.epidata.lib.models.AutomatedTest.NAME => {
                    models.AutomatedTest.insertRecordFromZMQ(rawData)
                    // println("inserted AutomatedTest rawData: " + rawData + "\n")
                  }
                  case com.epidata.lib.models.SensorMeasurement.NAME => {
                    models.SensorMeasurement.insertRecordFromZMQ(rawData)
                    // println("inserted SensorMeasurement rawData: " + rawData + "\n")
                  }
                  case _ =>
                }
              } catch {
                case e: JsonMappingException => throw new Exception(e.getMessage)
                case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode => {
                  break
                  // Thread.currentThread.interrupt()
                }
                case e: ZMQException => println("DataSink pull service thread interrupted")
                case _: Throwable => throw new Exception("Error while insert data to database from data sink service")
              }
            }
          }
          originalDataSink.clear(pullPort)
        }
      })

      // Subscribe Thread - Cleansed Data
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val cleansedDataSink = new ZMQCleansedDataSink()
          cleansedDataSink.init(context, cleansedSubPort)

          breakable {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                val data: (String, Message) = cleansedDataSink.sub()
                val topic = data._1
                val processedData: String = data._2.value
                // println("Sub topic: " + topic + ", Sub data: " + processedData + "\n")

                Configs.measurementClass match {
                  case com.epidata.lib.models.AutomatedTest.NAME => {
                    if (topic == "measurements_cleansed") {
                      models.AutomatedTest.insertCleansedRecordFromZMQ(processedData)
                      // println("inserted AutomatedTest cleansed data: " + processedData + "\n")
                    } else {
                      logger.error("unrecognized topic")
                    }
                  }
                  case com.epidata.lib.models.SensorMeasurement.NAME => {
                    if (topic == "measurements_cleansed") {
                      models.SensorMeasurement.insertCleansedRecordFromZMQ(processedData)
                      // println("inserted SensorMeasurement cleansed data: " + processedData + "\n")
                    } else {
                      logger.error("unrecognized topic")
                    }
                  }
                  case _ =>
                }
              } catch {
                case e: JsonMappingException => throw new Exception(e.getMessage)
                case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode => {
                  break
                  // Thread.currentThread.interrupt()
                }
                case e: ZMQException => println("DataSink sub service thread interrupted. " + e.getMessage)
                case _: Throwable => throw new Exception("Error while insert data to database from data sink service")
              }
            }
          }
          cleansedDataSink.clear(cleansedSubPort)
        }
      })

      // Subscribe Thread - Summary Data
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val summaryDataSink = new ZMQSummaryDataSink()
          summaryDataSink.init(context, summarySubPort)

          breakable {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                val data: (String, Message) = summaryDataSink.sub()
                val topic = data._1
                val processedData: String = data._2.value
                // println("Sub topic: " + topic + ", Sub data: " + processedData + "\n")

                Configs.measurementClass match {
                  case com.epidata.lib.models.AutomatedTest.NAME => {
                    if (topic == "measurements_summary") {
                      models.AutomatedTest.insertSummaryRecordFromZMQ(processedData)
                      // println("inserted AutomatedTest summary data: " + processedData + "\n")
                    } else {
                      logger.error("unrecognized topic")
                    }
                  }
                  case com.epidata.lib.models.SensorMeasurement.NAME => {
                    if (topic == "measurements_summary") {
                      models.SensorMeasurement.insertSummaryRecordFromZMQ(processedData)
                      // println("inserted SensorMeasurement summary data: " + processedData + "\n")
                    } else {
                      logger.error("unrecognized topic")
                    }
                  }
                  case _ =>
                }
              } catch {
                case e: JsonMappingException => throw new Exception(e.getMessage)
                case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode => {
                  break
                  // Thread.currentThread.interrupt()
                }
                case e: ZMQException => println("DataSink sub service thread interrupted. " + e.getMessage)
                case _: Throwable => throw new Exception("Error while insert data to database from data sink service")
              }
            }
          }
          summaryDataSink.clear(summarySubPort)
        }
      })

      // Subscribe Thread - Dynamic (Cleansed and/or Summary) Data
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val dynamicDataSink = new ZMQDynamicDataSink()
          dynamicDataSink.init(context, dynamicSubPort)

          breakable {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                val data: (String, Message) = dynamicDataSink.sub()
                val topic = data._1
                val processedData: String = data._2.value
                // println("Sub topic: " + topic + ", Sub data: " + processedData + "\n")

                Configs.measurementClass match {
                  case com.epidata.lib.models.AutomatedTest.NAME => {
                    if (topic == "measurements_dynamic") {
                      models.AutomatedTest.insertDynamicRecordFromZMQ(processedData)
                      // println("inserted AutomatedTest dynamic data: " + processedData + "\n")
                    } else {
                      logger.error("unrecognized topic")
                    }
                  }
                  case com.epidata.lib.models.SensorMeasurement.NAME => {
                    if (topic == "measurements_dynamic") {
                      models.SensorMeasurement.insertDynamicRecordFromZMQ(processedData)
                      // println("inserted SensorMeasurement dynamic data: " + processedData + "\n")
                    } else {
                      logger.error("unrecognized topic")
                    }
                  }
                  case _ =>
                }
              } catch {
                case e: JsonMappingException => throw new Exception(e.getMessage)
                case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode => {
                  break
                  // Thread.currentThread.interrupt()
                }
                case e: ZMQException => println("DataSink sub service thread interrupted. " + e.getMessage)
                case _: Throwable => throw new Exception("Error while insert data to database from data sink service")
              }
            }
          }
          dynamicDataSink.clear(dynamicSubPort)
        }
      })

    } catch {
      case e: Throwable =>
        throw new Exception(e.getMessage)
    }
  }

  def stop(): Unit = {
    try {
      executorService.shutdown()
    } catch {
      case e: InterruptedException =>
        logger.error("InterruptedException during DataSink shutdown. " + e.getMessage)
        executorService.shutdownNow()
        Thread.currentThread().interrupt();
      case e: Throwable =>
        logger.error("Exception during DataSink shutdown. " + e.getMessage)
    }
  }

}
