/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.{ SparkContext, SparkConf }

object MeasStatisticsStream {

  def main(args: Array[String]) {

    val conf = ConfigFactory.load()

    val sparkConf = new SparkConf()
      .setMaster(conf.getString("spark.master"))
      .setAppName("epidata-meas-stats")
      .set("spark.cassandra.connection.host", conf.getString("cassandra.host"))
      .set("spark.epidata.cassandraKeyspaceName", conf.getString("cassandra.keyspace"))
      .set("spark.epidata.kafkaBrokers", conf.getString("kafka.brokers"))
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.epidata.streaming.batchDuration", conf.getInt("kafka.batch-duration").toString)

    val sparkContext = new SparkContext(sparkConf)
    val epidataContext = new EpidataContext(sparkContext)

    val (op, meas_names) = ("MeasStatistics", List("Wind_Speed", "Relative_Humidity"))

    epidataContext.createStream(op, meas_names, new java.util.HashMap())

  }

}

