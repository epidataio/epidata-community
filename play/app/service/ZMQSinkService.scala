/*
* Copyright (c) 2020 EpiData, Inc.
*/

package service
import java.util.concurrent.Executors

import com.fasterxml.jackson.databind.JsonMappingException

class ZMQSinkService {

  def run(): Unit = {
    Executors.newSingleThreadExecutor.execute(new Runnable {
      override def run(): Unit = {
        while (true) {
          val rawData: Message = ZMQInit.ZMQSinkService.pull()
          val processedData: Message = ZMQInit.ZMQSinkService.sub()
          try {
            Configs.measurementClass match {
              case com.epidata.lib.models.AutomatedTest.NAME => {
                models.AutomatedTest.insertRecordFromZMQ(rawData.message)
                models.AutomatedTest.insertRecordFromZMQ(processedData.message)
              }
              case com.epidata.lib.models.SensorMeasurement.NAME => {
                models.SensorMeasurement.insertRecordFromZMQ(rawData.message)
                models.SensorMeasurement.insertRecordFromZMQ(processedData.message)
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
