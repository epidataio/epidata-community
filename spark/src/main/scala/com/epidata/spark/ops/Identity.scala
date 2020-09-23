/*
 * Copyright (c) 2015-2020 EpiData, Inc.
*/

package com.epidata.spark.ops

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Identity extends Transformation {

  override def apply(dataFrame: DataFrame, sqlContext: SQLContext): DataFrame =
    dataFrame
      .withColumn("meas_flag", lit(null: String))
      .withColumn("meas_method", lit(null: String))

  override def destination: String = "measurements_cleansed"
}
