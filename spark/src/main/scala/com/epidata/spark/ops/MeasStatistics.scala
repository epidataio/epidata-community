/*
* Copyright (c) 2015-2022 EpiData, Inc.
*/

package com.epidata.spark.ops

import com.epidata.lib.models.{ StatsSummary, StatsSummaryAsJson }
import org.apache.spark.sql.{ SQLContext, Column, DataFrame }
import org.apache.spark.sql.functions._
import scala.collection.mutable.{ Map => MutableMap, ListBuffer }
import java.util.{ Date, LinkedHashMap => JLinkedHashMap, LinkedList => JLinkedList, List => JList }
import scala.io.StdIn

class MeasStatistics(
    val meas_names: List[String],
    val method: String) extends Transformation {

  override def apply(measurements: ListBuffer[JLinkedHashMap[String, Object]]): ListBuffer[JLinkedHashMap[String, Object]] = {
    println("\n input measurements - meas statistics: " + measurements + "\n")

    var measStatistics = ListBuffer[JLinkedHashMap[String, Object]]()

    method match {
      case "standard" =>
        val groupBy = Seq("customer", "customer_site", "collection", "dataset", "start_time", "stop_time", "key1", "key2", "key3")
        val operations = Seq("start_time", "stop_time", "min", "max", "mean", "count", "std")

        val filteredMeas = measurements
          .filter(m => meas_names.contains(m.get("meas_name").asInstanceOf[String]))
          .groupBy(record => (record.get("customer"), record.get("customer_site"), record.get("collection"), record.get("dataset"), record.get("key1"), record.get("meas_name")))
        //          .groupBy(record => (record.get("company"), record.get("site"), record.get("station"), record.get("sensor"), record.get("event"), record.get("meas_name")))

        for (k <- filteredMeas.keySet.toSeq) {
          var values = filteredMeas.get(k).get

          var map = new JLinkedHashMap[String, Object]()
          map.put("customer", k._1)
          map.put("customer_site", k._2)
          map.put("collection", k._3)
          map.put("dataset", k._4)
          map.put("key1", k._5)
          map.put("meas_name", k._6)

          map.put("start_time", values(1).get("ts"))
          map.put("stop_time", values(1).get("ts"))

          var min: Double = values(1).get("meas_value").asInstanceOf[Double]
          var max: Double = values(1).get("meas_value").asInstanceOf[Double]
          var sum: Double = 0.00
          var count: Int = 0
          var mean: Double = min
          var std: Double = 0

          var measValues = ListBuffer[Double]()

          for (v <- values) {
            if (v.get("ts").asInstanceOf[Long].compareTo(map.get("start_time").asInstanceOf[Long]) < 0)
              map.put("start_time", v.get("ts"))
            if (v.get("ts").asInstanceOf[Long].compareTo(map.get("stop_time").asInstanceOf[Long]) > 0)
              map.put("stop_time", v.get("ts"))

            if (v.get("meas_value").asInstanceOf[Double].compareTo(min) < 0)
              min = v.get("meas_value").asInstanceOf[Double]
            if (v.get("meas_value").asInstanceOf[Double].compareTo(max) > 0)
              max = v.get("meas_value").asInstanceOf[Double]

            sum += v.get("meas_value").asInstanceOf[Double]
            count += 1
            measValues += v.get("meas_value").asInstanceOf[Double]
          }

          if (count > 0) {
            mean = (sum / count).asInstanceOf[Double]
            std = Math.sqrt((measValues.map(_ - mean)
              .map(x => x * x).sum) / measValues.length).asInstanceOf[Double]
            map.put("meas_summary_description", "descriptive statistics")
            map.put("meas_summary_name", "statistics")
            map.put("meas_summary_value", StatsSummaryAsJson(min, max, mean, count, std).toJson)
          } else {
            println("insufficient data for computing measurements statistics")
          }

          measStatistics.append(map)
        }

        println("\n output - measurement statistics: " + measStatistics + "\n")

        measStatistics

      case _ => throw new Exception("Unsupported statistics method: " + method)
    }
  }

  override def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {
    println("\n input measurements - meas statistics: " + dataFrame + "\n")

    method match {
      case "standard" =>

        dataFrame.registerTempTable("df_tmp")
        val row = sqlContext.sql(s"SELECT min(ts) as min, max(ts) as max FROM df_tmp").first()
        val startTime = row.getTimestamp(0)
        val stopTime = row.getTimestamp(1)

        val mapping: Map[String, Column => Column] = Map(
          "min" -> min, "max" -> max, "mean" -> mean, "count" -> count, "std" -> stddev)

        val groupBy = Seq("customer", "customer_site", "collection", "dataset", "start_time", "stop_time", "key1", "key2", "key3")
        val aggregate = Seq("meas_value")
        val operations = Seq("min", "max", "mean", "count", "std")
        val exprs = aggregate.flatMap(c => operations.map(f => mapping(f)(col(c)).alias(f)))

        val describeUDF = udf((min: Float, max: Float, mean: Float, count: Long, std: Float) =>
          StatsSummary(min, max, mean, count, std).toJson)

        val df = dataFrame
          .withColumn("start_time", lit(startTime))
          .withColumn("stop_time", lit(stopTime))
          .filter(dataFrame("key2").isin(meas_names: _*) || dataFrame("key3").isin(meas_names: _*) || dataFrame("key1").isin(meas_names: _*))
          .groupBy(groupBy.map(col): _*)
          .agg(exprs.head, exprs.tail: _*)
          .withColumn("meas_summary_description", lit("descriptive statistics"))
          .withColumn("meas_summary_name", lit("statistics"))
          .withColumn("meas_summary_value", describeUDF(col("min"), col("max"), col("mean"), col("count"), col("std")))
          .drop(operations: _*)

        println("\n output - measurement statistics: " + df + "\n")

        df

      case _ => throw new Exception("Unsupported statistics method: " + method)
    }
  }

  override val name: String = "MeasStatistics"
  override def destination: String = "measurements_summary"
}
