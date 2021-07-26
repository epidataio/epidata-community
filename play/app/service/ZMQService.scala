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
  private var subPort: String = _
  private var context: ZMQ.Context = _

  val logger: Logger = Logger(this.getClass())

  private val poolSize: Int = 2
  private val executorService: ExecutorService = Executors.newFixedThreadPool(poolSize)

  def init(context: ZMQ.Context, pullPort: String, subPort: String): ZMQService.type = {
    this.context = context
    this.pullPort = pullPort
    this.subPort = subPort
    this
  }

  /**
   * ZMQ DataSink as a thread
   */
  def start(): Unit = {
    println("ZMQService started")

    try {
      // Pull Thread - Original Data
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val sink = new ZMQPullDataSink()
          sink.init(context, pullPort)

          //println("ZMQ DataSink pull running in a new thread.")

          breakable {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                val rawData: String = sink.pull().value

                Configs.measurementClass match {
                  case com.epidata.lib.models.AutomatedTest.NAME => {
                    models.AutomatedTest.insertRecordFromZMQ(rawData)
                    //println("inserted AutomatedTest rawData: " + rawData + "\n")
                  }
                  case com.epidata.lib.models.SensorMeasurement.NAME => {
                    models.SensorMeasurement.insertRecordFromZMQ(rawData)
                    //println("inserted SensorMeasurement rawData: " + rawData + "\n")
                  }
                  case _ =>
                }
              } catch {
                case e: JsonMappingException => throw new Exception(e.getMessage)
                case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode => {
                  break
                  // Thread.currentThread.interrupt()
                  //println("DataSink pull service interrupted")
                }
                case e: ZMQException => println("DataSink pull service thread interrupted")
                case _: Throwable => throw new Exception("Error while insert data to database from data sink service")
              }
            }
          }
          //println("DataSink pull loop exited")
          sink.clear(pullPort)
        }
      })

      // Subscribe Thread - Cleansed Data
      executorService.submit(new Runnable {
        //Executors.newSingleThreadExecutor.execute(new Runnable {
        override def run(): Unit = {
          val sink = new ZMQSubDataSink()
          sink.init(context, subPort)

          breakable {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                val cleansedData: String = sink.sub().value

                Configs.measurementClass match {
                  case com.epidata.lib.models.AutomatedTest.NAME => {
                    models.AutomatedTest.insertCleansedRecordFromZMQ(cleansedData)
                    //println("inserted AutomatedTest cleansedData: " + cleansedData + "\n")
                  }
                  case com.epidata.lib.models.SensorMeasurement.NAME => {
                    models.SensorMeasurement.insertCleansedRecordFromZMQ(cleansedData)
                    //println("inserted SensorMeasurement cleansedData: " + cleansedData + "\n")
                  }
                  case com.epidata.lib.models.AutomatedTest.NAME => {
                    models.AutomatedTest.insertSummaryRecordFromZMQ(cleansedData)
                    //println("inserted AutomatedTest summaryData: " + cleansedData + "\n")
                  }
                  case com.epidata.lib.models.SensorMeasurement.NAME => {
                    models.SensorMeasurement.insertSummaryRecordFromZMQ(cleansedData)
                    //println("inserted SensorMeasurement summaryData: " + cleansedData + "\n")
                  }
                  case _ =>
                }
              } catch {
                case e: JsonMappingException => throw new Exception(e.getMessage)
                case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode => {
                  break
                  // Thread.currentThread.interrupt()
                  println("DataSink sub service interrupted")
                }
                case e: ZMQException => println("ZMQ sub service thread interrupted")
                case _: Throwable => throw new Exception("Error while insert data to database from data sink service")
              }
            }
          }
          //println("DataSink Sub loop exited")
          sink.clear(subPort)
        }
      })

    } catch {
      case e: Throwable =>
        throw new Exception(e.getMessage)
    }
  }

  def stop(): Unit = {
    println("Stopping DataSink services ...")
    try {
      executorService.shutdown()
    } catch {
      case e: InterruptedException =>
        println(e.getMessage)
        logger.error("InterruptedException during DataSink shutdown", e)
        executorService.shutdownNow()
        Thread.currentThread().interrupt();
      case e: Throwable =>
        println(e.getMessage)
        logger.error("Exception during DataSink shutdown", e)
    }
  }

}
