/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark

import com.epidata.spark.models.MeasurementDB
import com.epidata.spark.ops.Transformation
import com.epidata.spark.utils.ConvertUtils
import kafka.serializer.StringDecoder
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SaveMode, SQLContext }
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ Time, StreamingContext, Duration }

class EpidataStreamingContext(
    val epidataContext: EpidataContext,
    val batchDuration: Duration,
    val topics: String
) {

  val topicsSet = topics.split(",").toSet

  private val sparkContext = epidataContext.getSparkContext
  private val kafkaBrokers = epidataContext.getKafkaBrokers
  private val cassandraKeyspaceName = epidataContext.getCassandraKeyspaceName
  private val sparkStreamingContext = new StreamingContext(sparkContext, batchDuration)
  private val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    sparkStreamingContext,
    Map[String, String]("metadata.broker.list" -> kafkaBrokers),
    topicsSet
  )

  val sqlContext = epidataContext.getSQLContext
  import sqlContext.implicits._

  private def start: Unit = {
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
    sparkStreamingContext.stop(true, true)
  }

  def saveToCassandra(op: Transformation): Unit = {
    kafkaStream.foreachRDD {
      (message: RDD[(String, String)], batchTime: Time) =>
        {
          val inputDataFrame = message.map(_._2).map(m => {
            ConvertUtils.convertJsonStringToMeasurementDB(m)
          }).toDF(
            MeasurementDB.FieldNames: _*
          )

          if (inputDataFrame.count() > 0) {
            val outputDataFrame = op(inputDataFrame, sqlContext)

            if (outputDataFrame.count() > 0) {
              outputDataFrame.write.format("org.apache.spark.sql.cassandra")
                .mode(SaveMode.Append)
                .options(Map("keyspace" -> cassandraKeyspaceName, "table" -> op.destination))
                .save()
            }
          }
        }
    }

    start
  }

  // For testing
  def startStreamingAndPrint(op: Transformation): Unit = {

    kafkaStream.foreachRDD {
      (message: RDD[(String, String)], batchTime: Time) =>
        {
          val dataFrame = message.map(_._2).map(m => {
            ConvertUtils.convertJsonStringToMeasurementDB(m)
          }).toDF(
            MeasurementDB.FieldNames: _*
          )

          val df = op(dataFrame, sqlContext)
          df.show()

          println(s"${df.count()} rows processed.")
        }
    }

    start
  }

  def stop(): Unit = {
    sparkStreamingContext.stop()
  }

}

object EpidataStreamingContext {
  val BatchDurationInSecond: Int = 6
}
