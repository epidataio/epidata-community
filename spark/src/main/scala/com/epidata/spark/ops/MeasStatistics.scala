/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark.ops

import com.epidata.lib.models.StatsSummary
import org.apache.spark.sql.{ SQLContext, Column, DataFrame }
import org.apache.spark.sql.functions._

class MeasStatistics(
    val meas_names: List[String],
    val method: String) extends Transformation {
  override def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {

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

        df

      case _ => throw new Exception("Unsupported statistics method: " + method)
    }
  }

  override val name: String = "MeasStatistics"
  override def destination: String = "measurements_summary"
}
