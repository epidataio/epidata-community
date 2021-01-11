/*
* Copyright (c) 2015-2017 EpiData, Inc.
*/

package com.epidata.spark.ops

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ SQLContext, DataFrame }

class OutlierDetector(
  val column: String,
  val method: String) extends Transformation {
  override def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {
    method match {
      case "quartile" =>

        dataFrame.registerTempTable("df_tmp")
        val row = sqlContext.sql(s"SELECT avg($column) as mean, PERCENTILE($column, 0.25) as q1, PERCENTILE($column, 0.75) as q3 FROM df_tmp").first()
        val mean = row.getDouble(0)
        val q1 = row.getDouble(1)
        val q3 = row.getDouble(2)
        val iqr = q3 - q1
        val lif = q1 - 1.5 * iqr
        val lof = q1 - 3.0 * iqr
        val uif = q3 + 1.5 * iqr
        val uof = q3 + 3.0 * iqr

        dataFrame.filter(dataFrame(column) < lif || dataFrame(column) > uif)
          .withColumn("meas_flag", when(col(column) < lof || col(column) > uif, lit("extreme")).otherwise(lit("mild")))
          .withColumn("meas_method", lit(method))

      case _ => throw new Exception("Unexpected outlier method: " + method)
    }
  }

  override def destination: String = "measurements_cleansed"
}
