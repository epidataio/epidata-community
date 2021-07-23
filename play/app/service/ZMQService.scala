/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service
import java.util.concurrent.{ Executors, ExecutorService, TimeUnit, Future }
import org.json.simple.{ JSONArray, JSONObject }
import java.util.{ Map => JMap, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList }
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

          println("ZMQ DataSink pull running in a new thread.")

          breakable {
            while (!Thread.currentThread().isInterrupted()) {
              try {
                val rawData: Message = sink.pull()

                Configs.measurementClass match {
                  case com.epidata.lib.models.AutomatedTest.NAME => {
                    models.AutomatedTest.insertRecordFromZMQ(rawData.value)
                    println("inserted AutomatedTest rawData")
                  }
                  case com.epidata.lib.models.SensorMeasurement.NAME => {
                    models.SensorMeasurement.insertRecordFromZMQ(rawData.value)
                    println("inserted SensorMeasurement rawData")
                  }
                  case _ =>
                }
              } catch {
                case e: JsonMappingException => throw new Exception(e.getMessage)
                case e: ZMQException if ZMQ.Error.ETERM.getCode == e.getErrorCode => {
                  break
                  // Thread.currentThread.interrupt()
                  println("DataSink pull service interrupted")
                }
                case e: ZMQException => println("DataSink pull service thread interrupted")
                case _: Throwable => throw new Exception("Error while insert data to database from data sink service")
              }
            }
          }
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
                val cleansedData: Message = sink.sub()

                Configs.measurementClass match {
                  case com.epidata.lib.models.AutomatedTest.NAME => {
                    models.AutomatedTest.insertCleansedRecordFromZMQ(cleansedData.value)
                    //println("inserted AutomatedTest cleansedData")
                  }
                  case com.epidata.lib.models.SensorMeasurement.NAME => {
                    models.SensorMeasurement.insertCleansedRecordFromZMQ(cleansedData.value)
                    //println("inserted SensorMeasurement cleansedData")
                  }
                  case com.epidata.lib.models.AutomatedTest.NAME => {
                    models.AutomatedTest.insertSummaryRecordFromZMQ(cleansedData.value)
                    //println("inserted AutomatedTest summaryData")
                  }
                  case com.epidata.lib.models.SensorMeasurement.NAME => {
                    models.SensorMeasurement.insertSummaryRecordFromZMQ(cleansedData.value)
                    //println("inserted SensorMeasurement summaryData")
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
