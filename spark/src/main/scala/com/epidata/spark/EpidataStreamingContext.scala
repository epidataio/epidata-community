/*
 * Copyright (c) 2015-2020 EpiData, Inc.
*/

package com.epidata.spark

import com.epidata.spark.models.MeasurementDB
import com.epidata.spark.ops.Transformation
import com.epidata.spark.utils.ConvertUtils

//import _root_.kafka.serializer.StringDecoder
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.{ SaveMode, SQLContext }
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{ Time, StreamingContext, Duration }

class EpidataStreamingContext(
    val epidataContext: EpidataContext,
    val batchDuration: Duration,
    val topics: String) {

  val topicsSet = topics.split(",").toSet

  private val sparkContext = epidataContext.getSparkContext
  private val kafkaBrokers = epidataContext.getKafkaBrokers
  private val cassandraKeyspaceName = epidataContext.getCassandraKeyspaceName
  private val sparkStreamingContext = new StreamingContext(sparkContext, batchDuration)
  private val kafkaStream = KafkaUtils.createDirectStream[String, String](
    sparkStreamingContext,
    PreferConsistent,
    Subscribe[String, String](topicsSet, Map[String, String]("metadata.broker.list" -> kafkaBrokers)))

  val sqlContext = epidataContext.getSQLContext
  import sqlContext.implicits._

  private def start: Unit = {
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
    sparkStreamingContext.stop(true, true)
  }

  def saveToCassandra(op: Transformation): Unit = {
    kafkaStream.foreachRDD {
      (message: RDD[ConsumerRecord[String, String]], batchTime: Time) =>
        {
          val inputDataFrame = message.map(_.value()).map(m => {
            ConvertUtils.convertJsonStringToMeasurementDB(m)
          }).toDF(
            MeasurementDB.FieldNames: _*)

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

  def stop(): Unit = {
    sparkStreamingContext.stop()
  }

}

object EpidataStreamingContext {
  val BatchDurationInSecond: Int = 6
}