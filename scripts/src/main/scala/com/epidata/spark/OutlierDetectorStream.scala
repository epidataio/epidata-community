/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.{ SparkContext, SparkConf }

object OutlierDetectorStream {

  def main(args: Array[String]) {

    val conf = ConfigFactory.load()

    val sparkConf = new SparkConf()
      .setMaster(conf.getString("spark.master"))
      .setAppName("epidata-outlier-detector")
      .set("spark.cassandra.connection.host", conf.getString("cassandra.host"))
      .set("spark.epidata.cassandraKeyspaceName", conf.getString("cassandra.keyspace"))
      .set("spark.epidata.kafkaBrokers", conf.getString("kafka.brokers"))
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.epidata.streaming.batchDuration", conf.getInt("kafka.batch-duration").toString)

    val sparkContext = new SparkContext(sparkConf)
    val epidataContext = new EpidataContext(sparkContext)

    val op = "OutlierDetector"

    epidataContext.createStream(op, List.empty, new java.util.HashMap())

  }

}

